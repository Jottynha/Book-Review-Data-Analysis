"""
03_cruzar_api.py

Etapa de coleta e consolidação das fontes externas:

Goodreads
   ├── Open Library
   └── Google Books

Principais características:
- mantém Open Library e Google Books em estruturas separadas;
- reutiliza o cache antigo da branch openlibrary;
- permite consultar as duas fontes para o mesmo livro;
- usa ISBN-13/ISBN-10 antes de buscas por título;
- salva caches independentes;
- não considera "encontrado por uma API" como "finalizado";
- trata HTTP 429 com Retry-After e backoff;
- salva o cache periodicamente;
- permite retomada após interrupção;
- normaliza tipos antes de salvar Parquet;
- gera uma tabela de livros enriquecida e uma tabela de reviews.

Uso:
    python3 03_cruzar_api.py

Opções:
    --skip-openlibrary
    --skip-google
    --only-migrate-cache

Os dados desta etapa ainda NÃO são considerados matches validados.
A validação rigorosa será feita no 04_validar_matches.py.
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import random
import re
import sys
import time
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from tqdm import tqdm


# ============================================================
# CONFIGURAÇÃO
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

PASTA_PROJETO = Path(
    os.getenv(
        "PASTA_PROJETO",
        str(BASE_DIR),
    )
)

PASTA_PROCESSED = (
    PASTA_PROJETO / "processed"
)

ARQUIVO_BOOKS = (
    PASTA_PROCESSED
    / "goodreads_books_100k.parquet"
)

ARQUIVO_REVIEWS = (
    PASTA_PROCESSED
    / "goodreads_reviews_100k.parquet"
)


# ============================================================
# CACHE
# ============================================================

# Cache original criado pela branch anterior.
ARQUIVO_CACHE_ANTIGO = (
    PASTA_PROCESSED
    / "google_books_cache.json"
)

# Cache independente Open Library.
ARQUIVO_CACHE_OPENLIBRARY = (
    PASTA_PROCESSED
    / "openlibrary_cache.json"
)

# Cache independente Google Books.
ARQUIVO_CACHE_GOOGLE = (
    PASTA_PROCESSED
    / "google_books_cache_v2.json"
)


# ============================================================
# SAÍDAS
# ============================================================

ARQUIVO_BOOKS_ENRIQUECIDO = (
    PASTA_PROCESSED
    / "goodreads_books_enriquecido_100k.parquet"
)

ARQUIVO_REVIEWS_ENRIQUECIDO = (
    PASTA_PROCESSED
    / "goodreads_reviews_enriquecidas_100k.parquet"
)


# ============================================================
# OPEN LIBRARY
# ============================================================

OPENLIBRARY_BATCH_URL = (
    "https://openlibrary.org/api/books"
)

OPENLIBRARY_SEARCH_URL = (
    "https://openlibrary.org/search.json"
)

OPENLIBRARY_BATCH_SIZE = 50

OPENLIBRARY_DELAY = 0.40

OPENLIBRARY_SEARCH_WORKERS = 3

OPENLIBRARY_SEARCH_LIMIT = 5


# ============================================================
# GOOGLE BOOKS
# ============================================================

GOOGLE_BOOKS_URL = (
    "https://www.googleapis.com/books/v1/volumes"
)

GOOGLE_API_KEY = os.getenv(
    "GOOGLE_BOOKS_API_KEY"
)

GOOGLE_DELAY = 0.50

GOOGLE_TIMEOUT = 20

GOOGLE_MAX_RESULTS = 5


# ============================================================
# RATE LIMIT / RETRY
# ============================================================

MAX_RETRIES_429 = 5

BACKOFF_BASE = 2.0

BACKOFF_MAX = 120.0

# Pequena aleatoriedade para evitar que várias execuções
# sincronizadas façam requisições exatamente juntas.
BACKOFF_JITTER = 0.25


# ============================================================
# SEGURANÇA
# ============================================================

# Limita novas consultas por execução.
# Use 0 para sem limite.

MAX_NEW_OPENLIBRARY_BOOKS = 1000

MAX_NEW_GOOGLE_BOOKS = 1000


# ============================================================
# CACHE / CHECKPOINT
# ============================================================

# Quantidade de matches após a qual o cache é salvo.
CACHE_SAVE_EVERY = 25


# ============================================================
# FLAGS
# ============================================================

SKIP_OPENLIBRARY = (
    "--skip-openlibrary" in sys.argv
)

SKIP_GOOGLE = (
    "--skip-google" in sys.argv
)

ONLY_MIGRATE = (
    "--only-migrate-cache" in sys.argv
)


# ============================================================
# HEADERS
# ============================================================

OPENLIBRARY_HEADERS = {
    "User-Agent": (
        "Book-Review-Data-Analysis/2.0 "
        "(academic project; CEFET-MG)"
    )
}


GOOGLE_HEADERS = {
    "User-Agent": (
        "Book-Review-Data-Analysis/2.0 "
        "(academic project; CEFET-MG)"
    )
}


# ============================================================
# LEITURA DE .ENV
# ============================================================

def carregar_env_local() -> None:
    env_file = BASE_DIR / ".env"

    if not env_file.exists():
        return

    try:
        linhas = env_file.read_text(
            encoding="utf-8-sig"
        ).splitlines()

        for linha in linhas:
            linha = linha.strip()

            if not linha:
                continue

            if linha.startswith("#"):
                continue

            if "=" not in linha:
                continue

            chave, valor = linha.split(
                "=",
                1,
            )

            chave = (
                chave
                .strip()
                .lstrip("\ufeff")
            )

            valor = (
                valor
                .strip()
                .strip("\"'")
            )

            if (
                chave
                and chave not in os.environ
            ):
                os.environ[chave] = valor

    except OSError:
        pass


carregar_env_local()

GOOGLE_API_KEY = os.getenv(
    "GOOGLE_BOOKS_API_KEY"
)


# ============================================================
# UTILITÁRIOS
# ============================================================

def limpar(valor: Any) -> str:
    if valor is None:
        return ""

    try:
        if pd.isna(valor):
            return ""
    except (
        TypeError,
        ValueError,
    ):
        pass

    texto = str(valor).strip()

    if texto.lower() in {
        "",
        "nan",
        "none",
        "null",
    }:
        return ""

    return texto


def isbn_limpo(valor: Any) -> str:
    return re.sub(
        r"[^0-9X]",
        "",
        limpar(valor).upper(),
    )


def normalizar_texto(valor: Any) -> str:
    """
    Normalização Unicode:
    preserva letras de diferentes alfabetos e remove apenas
    pontuação/espaçamento irrelevante.
    """

    texto = limpar(valor).lower()

    if not texto:
        return ""

    texto = unicodedata.normalize(
        "NFKD",
        texto,
    )

    texto = "".join(
        caractere
        for caractere in texto
        if not unicodedata.combining(
            caractere
        )
    )

    texto = re.sub(
        r"[^\w\s]",
        " ",
        texto,
        flags=re.UNICODE,
    )

    return re.sub(
        r"\s+",
        " ",
        texto,
    ).strip()


def titulo_busca(valor: Any) -> str:
    titulo = limpar(valor)

    titulo = re.sub(
        r"\s*\([^)]*\)",
        "",
        titulo,
    )

    return titulo.split(":")[0].strip()


def similaridade(
    a: Any,
    b: Any,
) -> float:

    a_norm = normalizar_texto(a)

    b_norm = normalizar_texto(b)

    if not a_norm or not b_norm:
        return 0.0

    if a_norm == b_norm:
        return 1.0

    return SequenceMatcher(
        None,
        a_norm,
        b_norm,
    ).ratio()


def primeiro_autor(
    valor: Any,
) -> str:

    if isinstance(valor, list):
        return (
            limpar(valor[0])
            if valor
            else ""
        )

    texto = limpar(valor)

    if "|" in texto:
        return (
            texto
            .split("|")[0]
            .strip()
        )

    return texto


def juntar_lista(
    valor: Any,
) -> str:

    if isinstance(valor, list):
        return "|".join(
            limpar(item)
            for item in valor
            if limpar(item)
        )

    return limpar(valor)


# ============================================================
# JSON
# ============================================================

def carregar_json(
    caminho: Path,
) -> dict[str, Any]:

    if not caminho.exists():
        return {}

    try:
        with caminho.open(
            "r",
            encoding="utf-8",
        ) as arquivo:

            dados = json.load(
                arquivo
            )

        if isinstance(
            dados,
            dict,
        ):
            return dados

        return {}

    except (
        OSError,
        json.JSONDecodeError,
    ):
        return {}


def salvar_json(
    caminho: Path,
    dados: dict[str, Any],
) -> None:

    caminho.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporario = caminho.with_suffix(
        caminho.suffix + ".tmp"
    )

    with temporario.open(
        "w",
        encoding="utf-8",
    ) as arquivo:

        json.dump(
            dados,
            arquivo,
            ensure_ascii=False,
            indent=2,
        )

    temporario.replace(
        caminho
    )


# ============================================================
# RATE LIMIT
# ============================================================

def obter_retry_after(
    resposta: requests.Response,
) -> float | None:

    valor = resposta.headers.get(
        "Retry-After"
    )

    if not valor:
        return None

    valor = valor.strip()

    try:
        segundos = float(valor)

        if segundos >= 0:
            return segundos

    except ValueError:
        pass

    return None


def calcular_backoff(
    tentativa: int,
) -> float:

    atraso = min(
        BACKOFF_MAX,
        BACKOFF_BASE
        ** max(tentativa, 1),
    )

    jitter = random.uniform(
        0,
        BACKOFF_JITTER,
    )

    return atraso + jitter


def esperar_rate_limit(
    resposta: requests.Response,
    tentativa: int,
    fonte: str,
) -> bool:

    if resposta.status_code != 429:
        return False

    retry_after = obter_retry_after(
        resposta
    )

    if retry_after is not None:
        espera = min(
            retry_after,
            BACKOFF_MAX,
        )

        origem = (
            "Retry-After"
        )

    else:
        espera = calcular_backoff(
            tentativa
        )

        origem = (
            "backoff exponencial"
        )

    print(
        f"[{fonte}] HTTP 429. "
        f"Aguardando {espera:.1f}s "
        f"({origem}; tentativa "
        f"{tentativa}/{MAX_RETRIES_429})."
    )

    time.sleep(espera)

    return True


# ============================================================
# REQUEST COM RETRY
# ============================================================

def get_com_retry(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 20,
    fonte: str = "API",
) -> requests.Response | None:

    for tentativa in range(
        1,
        MAX_RETRIES_429 + 1,
    ):

        try:
            resposta = session.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout,
            )

        except requests.RequestException as erro:

            if tentativa >= MAX_RETRIES_429:
                print(
                    f"[{fonte}] Erro de rede "
                    f"após {tentativa} tentativas: "
                    f"{erro}"
                )
                return None

            espera = min(
                BACKOFF_MAX,
                BACKOFF_BASE
                ** tentativa,
            )

            espera += random.uniform(
                0,
                BACKOFF_JITTER,
            )

            print(
                f"[{fonte}] Erro de rede. "
                f"Nova tentativa em "
                f"{espera:.1f}s."
            )

            time.sleep(
                espera
            )

            continue

        if resposta.status_code != 429:
            return resposta

        if tentativa >= MAX_RETRIES_429:
            print(
                f"[{fonte}] HTTP 429 "
                f"persistente após "
                f"{tentativa} tentativas."
            )

            return resposta

        esperar_rate_limit(
            resposta,
            tentativa,
            fonte,
        )

    return None


# ============================================================
# MIGRAÇÃO DO CACHE ANTIGO
# ============================================================

def parece_openlibrary(
    registro: Any,
) -> bool:

    if not isinstance(
        registro,
        dict,
    ):
        return False

    origem = limpar(
        registro.get(
            "api_source"
        )
    ).lower()

    tipo = limpar(
        registro.get(
            "google_kind"
        )
    ).lower()

    metodo = limpar(
        registro.get(
            "search_method"
        )
    ).lower()

    return (
        origem.startswith(
            "openlibrary"
        )
        or "openlibrary" in tipo
        or "openlibrary" in metodo
    )


def converter_cache_antigo_openlibrary(
    registro: dict[str, Any],
) -> dict[str, Any]:

    mapa = {
        "google_volume_id":
            "openlibrary_key",

        "google_title":
            "openlibrary_title",

        "google_subtitle":
            "openlibrary_subtitle",

        "google_authors":
            "openlibrary_authors",

        "google_publisher":
            "openlibrary_publishers",

        "google_published_date":
            "openlibrary_published_date",

        "google_description":
            "openlibrary_description",

        "google_page_count":
            "openlibrary_pages",

        "google_categories":
            "openlibrary_subjects",

        "google_isbn10":
            "openlibrary_isbn10",

        "google_isbn13":
            "openlibrary_isbn13",

        "google_thumbnail":
            "openlibrary_thumbnail",

        "google_small_thumbnail":
            "openlibrary_small_thumbnail",

        "google_preview_link":
            "openlibrary_url",

        "google_info_link":
            "openlibrary_url",
    }

    novo = {}

    for origem, destino in mapa.items():

        if origem in registro:
            novo[destino] = (
                registro.get(origem)
            )

    novo["status"] = "matched"

    novo["source"] = "openlibrary"

    novo["search_method"] = registro.get(
        "search_method",
        "",
    )

    return novo


def migrar_caches() -> tuple[
    dict[str, Any],
    dict[str, Any],
]:

    cache_antigo = carregar_json(
        ARQUIVO_CACHE_ANTIGO
    )

    cache_openlibrary = carregar_json(
        ARQUIVO_CACHE_OPENLIBRARY
    )

    cache_google = carregar_json(
        ARQUIVO_CACHE_GOOGLE
    )

    migrados_ol = 0

    migrados_google = 0

    for book_id, registro in (
        cache_antigo.items()
    ):

        if not registro:
            continue

        book_id = str(
            book_id
        )

        if parece_openlibrary(
            registro
        ):

            if (
                book_id
                not in cache_openlibrary
            ):

                cache_openlibrary[
                    book_id
                ] = (
                    converter_cache_antigo_openlibrary(
                        registro
                    )
                )

                migrados_ol += 1

        elif isinstance(
            registro,
            dict,
        ):

            if (
                book_id
                not in cache_google
            ):

                cache_google[
                    book_id
                ] = registro

                migrados_google += 1

    salvar_json(
        ARQUIVO_CACHE_OPENLIBRARY,
        cache_openlibrary,
    )

    salvar_json(
        ARQUIVO_CACHE_GOOGLE,
        cache_google,
    )

    print(
        f"Cache antigo migrado: "
        f"{migrados_ol:,} Open Library + "
        f"{migrados_google:,} Google Books."
    )

    return (
        cache_openlibrary,
        cache_google,
    )


# ============================================================
# PARSERS OPEN LIBRARY
# ============================================================

def extrair_openlibrary_batch(
    dados: dict[str, Any],
    book_id: str,
    metodo: str,
) -> dict[str, Any]:

    autores = []

    for autor in dados.get(
        "authors",
        [],
    ):

        if isinstance(
            autor,
            dict,
        ):

            nome = limpar(
                autor.get("name")
            )

            if nome:
                autores.append(nome)

    categorias = []

    for subject in dados.get(
        "subjects",
        [],
    ):

        if isinstance(
            subject,
            dict,
        ):
            nome = limpar(
                subject.get("name")
            )
        else:
            nome = limpar(subject)

        if nome:
            categorias.append(nome)

    editoras = []

    for publisher in dados.get(
        "publishers",
        [],
    ):

        if isinstance(
            publisher,
            dict,
        ):
            nome = limpar(
                publisher.get("name")
            )
        else:
            nome = limpar(
                publisher
            )

        if nome:
            editoras.append(nome)

    identificadores = dados.get(
        "identifiers",
        {},
    )

    isbn10 = ""

    isbn13 = ""

    if isinstance(
        identificadores,
        dict,
    ):

        isbn10_list = (
            identificadores.get(
                "isbn_10",
                [],
            )
        )

        isbn13_list = (
            identificadores.get(
                "isbn_13",
                [],
            )
        )

        if isbn10_list:
            isbn10 = limpar(
                isbn10_list[0]
            )

        if isbn13_list:
            isbn13 = limpar(
                isbn13_list[0]
            )

    cover = dados.get(
        "cover",
        {},
    )

    thumbnail = ""

    if isinstance(
        cover,
        dict,
    ):

        thumbnail = (
            limpar(
                cover.get("medium")
            )
            or limpar(
                cover.get("large")
            )
            or limpar(
                cover.get("small")
            )
        )

    paginas = (
        dados.get(
            "number_of_pages"
        )
        or dados.get(
            "pagination"
        )
    )

    return {
        "status":
            "matched",

        "source":
            "openlibrary",

        "openlibrary_key":
            limpar(
                dados.get("key")
            ),

        "openlibrary_title":
            limpar(
                dados.get("title")
            ),

        "openlibrary_subtitle":
            limpar(
                dados.get("subtitle")
            ),

        "openlibrary_authors":
            "|".join(autores),

        "openlibrary_publishers":
            "|".join(editoras),

        "openlibrary_published_date":
            limpar(
                dados.get(
                    "publish_date"
                )
            ),

        "openlibrary_description":
            limpar(
                dados.get("notes")
            ),

        "openlibrary_pages":
            paginas,

        "openlibrary_subjects":
            "|".join(
                categorias[:20]
            ),

        "openlibrary_isbn10":
            isbn10,

        "openlibrary_isbn13":
            isbn13,

        "openlibrary_thumbnail":
            thumbnail,

        "openlibrary_url": (
            f"https://openlibrary.org"
            f"{limpar(dados.get('key'))}"
            if limpar(
                dados.get("key")
            )
            else ""
        ),

        "openlibrary_search_method":
            metodo,

        "goodreads_book_id":
            book_id,
    }


def extrair_openlibrary_search(
    doc: dict[str, Any],
    book_id: str,
    metodo: str,
) -> dict[str, Any]:

    subjects = doc.get(
        "subject",
        [],
    )

    if not isinstance(
        subjects,
        list,
    ):
        subjects = []

    return {
        "status":
            "matched",

        "source":
            "openlibrary",

        "openlibrary_key":
            limpar(
                doc.get("key")
            ),

        "openlibrary_title":
            limpar(
                doc.get("title")
            ),

        "openlibrary_subtitle":
            limpar(
                doc.get("subtitle")
            ),

        "openlibrary_authors":
            juntar_lista(
                doc.get(
                    "author_name",
                    [],
                )
            ),

        "openlibrary_publishers":
            juntar_lista(
                doc.get(
                    "publisher",
                    [],
                )
            ),

        "openlibrary_published_date":
            limpar(
                doc.get(
                    "first_publish_year"
                )
            ),

        "openlibrary_description":
            "",

        "openlibrary_pages":
            doc.get(
                "number_of_pages_median"
            ),

        "openlibrary_subjects":
            juntar_lista(
                subjects[:20]
            ),

        "openlibrary_isbn10":
            "",

        "openlibrary_isbn13":
            "",

        "openlibrary_thumbnail": (
            f"https://covers.openlibrary.org/b/id/"
            f"{doc.get('cover_i')}-M.jpg"
            if doc.get("cover_i")
            else ""
        ),

        "openlibrary_url": (
            f"https://openlibrary.org"
            f"{limpar(doc.get('key'))}"
            if limpar(
                doc.get("key")
            )
            else ""
        ),

        "openlibrary_search_method":
            metodo,

        "goodreads_book_id":
            book_id,
    }


# ============================================================
# SCORE OPEN LIBRARY
# ============================================================

def score_openlibrary_doc(
    row: pd.Series,
    doc: dict[str, Any],
) -> tuple[
    int,
    dict[str, float],
]:

    titulo_gr = (
        limpar(
            row.get(
                "title_without_series"
            )
        )
        or limpar(
            row.get("title")
        )
    )

    autor_gr = primeiro_autor(
        row.get("authors")
    )

    ano_gr = limpar(
        row.get(
            "publication_year"
        )
    )

    editora_gr = limpar(
        row.get("publisher")
    )

    titulo_ol = limpar(
        doc.get("title")
    )

    score = 0

    sim_titulo = similaridade(
        titulo_gr,
        titulo_ol,
    )

    if sim_titulo >= 0.97:
        score += 60

    elif sim_titulo >= 0.92:
        score += 48

    elif sim_titulo >= 0.85:
        score += 35

    elif sim_titulo >= 0.75:
        score += 20

    else:
        return (
            0,
            {
                "title_similarity":
                    sim_titulo,
                "author_similarity":
                    0.0,
            },
        )

    sim_autor = 0.0

    autores = doc.get(
        "author_name",
        [],
    )

    if isinstance(
        autores,
        list,
    ):

        for autor in autores[:10]:

            sim_autor = max(
                sim_autor,
                similaridade(
                    autor_gr,
                    autor,
                ),
            )

    # Autor forte ajuda na confirmação.
    if sim_autor >= 0.95:
        score += 20

    elif sim_autor >= 0.85:
        score += 15

    elif sim_autor >= 0.70:
        score += 8

    ano_ol = limpar(
        doc.get(
            "first_publish_year"
        )
    )

    if ano_gr and ano_ol:

        try:

            diferenca = abs(
                int(ano_gr)
                - int(ano_ol)
            )

            if diferenca <= 1:
                score += 25

            elif diferenca <= 3:
                score += 15

            elif diferenca > 10:
                score -= 15

        except ValueError:
            pass

    editoras = doc.get(
        "publisher",
        [],
    )

    if (
        editora_gr
        and isinstance(
            editoras,
            list,
        )
    ):

        gr_pub = normalizar_texto(
            editora_gr
        )

        for pub in editoras[:10]:

            pub_norm = normalizar_texto(
                pub
            )

            if (
                pub_norm
                and (
                    pub_norm in gr_pub
                    or gr_pub in pub_norm
                )
            ):

                score += 15
                break

    return (
        score,
        {
            "title_similarity":
                sim_titulo,

            "author_similarity":
                sim_autor,
        },
    )


# ============================================================
# OPEN LIBRARY POR TÍTULO
# ============================================================

def buscar_openlibrary_por_titulo(
    session: requests.Session,
    row: pd.Series,
) -> dict[str, Any] | None:

    titulo = (
        limpar(
            row.get(
                "title_without_series"
            )
        )
        or limpar(
            row.get("title")
        )
    )

    titulo = titulo_busca(
        titulo
    )

    if not titulo:
        return None

    params = {
        "title":
            titulo,

        "limit":
            OPENLIBRARY_SEARCH_LIMIT,

        "fields": (
            "key,title,subtitle,author_name,"
            "first_publish_year,publisher,"
            "isbn,subject,number_of_pages_median,"
            "cover_i"
        ),
    }

    resposta = get_com_retry(
        session,
        OPENLIBRARY_SEARCH_URL,
        params=params,
        headers=OPENLIBRARY_HEADERS,
        timeout=20,
        fonte="Open Library",
    )

    if resposta is None:
        return None

    if resposta.status_code == 429:
        return {
            "status":
                "rate_limited",

            "source":
                "openlibrary",
        }

    if resposta.status_code != 200:
        return None

    try:
        dados = resposta.json()

    except ValueError:
        return None

    documentos = dados.get(
        "docs",
        [],
    )

    melhor = None

    melhor_score = 0

    melhor_sinais = {}

    for doc in documentos:

        if not isinstance(
            doc,
            dict,
        ):
            continue

        score, sinais = (
            score_openlibrary_doc(
                row,
                doc,
            )
        )

        if score > melhor_score:

            melhor_score = score

            melhor = doc

            melhor_sinais = sinais

    if (
        melhor is None
        or melhor_score < 55
    ):
        return None

    resultado = (
        extrair_openlibrary_search(
            melhor,
            str(
                row["book_id"]
            ),
            f"openlibrary_title_score_{melhor_score}",
        )
    )

    resultado.update(
        {
            "title_similarity":
                round(
                    melhor_sinais[
                        "title_similarity"
                    ],
                    4,
                ),

            "author_similarity":
                round(
                    melhor_sinais[
                        "author_similarity"
                    ],
                    4,
                ),

            "search_score":
                melhor_score,
        }
    )

    isbns = melhor.get(
        "isbn",
        [],
    )

    if isinstance(
        isbns,
        list,
    ):

        for item in isbns:

            code = isbn_limpo(
                item
            )

            if (
                len(code) == 13
                and not resultado[
                    "openlibrary_isbn13"
                ]
            ):

                resultado[
                    "openlibrary_isbn13"
                ] = code

            elif (
                len(code) == 10
                and not resultado[
                    "openlibrary_isbn10"
                ]
            ):

                resultado[
                    "openlibrary_isbn10"
                ] = code

    return resultado


# ============================================================
# OPEN LIBRARY POR ISBN
# ============================================================

def consultar_openlibrary_batch(
    df_books: pd.DataFrame,
    cache: dict[str, Any],
) -> None:

    if SKIP_OPENLIBRARY:

        print(
            "[Open Library] "
            "etapa desativada."
        )

        return

    pendentes = {}

    for _, row in df_books.iterrows():

        book_id = str(
            row["book_id"]
        ).strip()

        if book_id in cache:
            continue

        isbn13 = isbn_limpo(
            row.get("isbn13")
        )

        isbn10 = isbn_limpo(
            row.get("isbn")
        )

        codigo = (
            isbn13
            or isbn10
        )

        if codigo:

            pendentes.setdefault(
                codigo,
                [],
            ).append(
                book_id
            )

    if MAX_NEW_OPENLIBRARY_BOOKS:

        ids_permitidos = set(
            list(
                dict.fromkeys(
                    book_id
                    for ids in pendentes.values()
                    for book_id in ids
                )
            )[
                :MAX_NEW_OPENLIBRARY_BOOKS
            ]
        )

        pendentes = {
            codigo: [
                book_id
                for book_id in ids
                if book_id
                in ids_permitidos
            ]
            for codigo, ids
            in pendentes.items()
        }

        pendentes = {
            codigo: ids
            for codigo, ids
            in pendentes.items()
            if ids
        }

    codigos = list(
        pendentes
    )

    print(
        f"[Open Library] ISBNs pendentes: "
        f"{len(codigos):,}"
    )

    if not codigos:
        return

    total_encontrados = 0

    session = requests.Session()

    try:

        for inicio in tqdm(
            range(
                0,
                len(codigos),
                OPENLIBRARY_BATCH_SIZE,
            ),
            desc="Open Library - ISBN",
        ):

            lote = codigos[
                inicio:
                inicio
                + OPENLIBRARY_BATCH_SIZE
            ]

            bibkeys = ",".join(
                f"ISBN:{codigo}"
                for codigo in lote
            )

            params = {
                "bibkeys":
                    bibkeys,

                "format":
                    "json",

                "jscmd":
                    "data",
            }

            resposta = get_com_retry(
                session,
                OPENLIBRARY_BATCH_URL,
                params=params,
                headers=OPENLIBRARY_HEADERS,
                timeout=30,
                fonte="Open Library",
            )

            if resposta is None:
                continue

            if resposta.status_code == 429:

                print(
                    "[Open Library] "
                    "Limite persistente. "
                    "Encerrando etapa por ISBN."
                )

                break

            if resposta.status_code != 200:

                time.sleep(
                    OPENLIBRARY_DELAY
                )

                continue

            try:

                dados = resposta.json()

            except ValueError:

                continue

            for codigo in lote:

                chave = (
                    f"ISBN:{codigo}"
                )

                if chave not in dados:
                    continue

                livro = dados[
                    chave
                ]

                for book_id in (
                    pendentes[codigo]
                ):

                    cache[
                        book_id
                    ] = (
                        extrair_openlibrary_batch(
                            livro,
                            book_id,
                            "openlibrary_isbn",
                        )
                    )

                    total_encontrados += 1

            # Checkpoint após cada lote.
            salvar_json(
                ARQUIVO_CACHE_OPENLIBRARY,
                cache,
            )

            time.sleep(
                OPENLIBRARY_DELAY
            )

    except KeyboardInterrupt:

        print(
            "\n[Open Library] "
            "Interrupção detectada. "
            "Salvando cache..."
        )

        salvar_json(
            ARQUIVO_CACHE_OPENLIBRARY,
            cache,
        )

        raise

    except Exception as erro:

        print(
            f"[Open Library] "
            f"Erro inesperado: {erro}"
        )

        salvar_json(
            ARQUIVO_CACHE_OPENLIBRARY,
            cache,
        )

    print(
        f"[Open Library] novos matches "
        f"por ISBN: "
        f"{total_encontrados:,}"
    )


# ============================================================
# OPEN LIBRARY POR TÍTULO
# ============================================================

def consultar_openlibrary_titulos(
    df_books: pd.DataFrame,
    cache: dict[str, Any],
) -> None:

    if SKIP_OPENLIBRARY:
        return

    pendentes = []

    for _, row in df_books.iterrows():

        book_id = str(
            row["book_id"]
        ).strip()

        if book_id in cache:
            continue

        pendentes.append(
            row
        )

    if MAX_NEW_OPENLIBRARY_BOOKS:

        pendentes = pendentes[
            :MAX_NEW_OPENLIBRARY_BOOKS
        ]

    print(
        f"[Open Library] títulos pendentes: "
        f"{len(pendentes):,}"
    )

    if not pendentes:
        return

    total_encontrados = 0

    session = requests.Session()

    def tarefa(
        row: pd.Series,
    ):

        resultado = (
            buscar_openlibrary_por_titulo(
                session,
                row,
            )
        )

        return (
            str(
                row["book_id"]
            ).strip(),
            resultado,
        )

    futuros = []

    try:

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=OPENLIBRARY_SEARCH_WORKERS
        ) as executor:

            futuros = [
                executor.submit(
                    tarefa,
                    row,
                )
                for _, row
                in pd.DataFrame(
                    pendentes
                ).iterrows()
            ]

            for futuro in tqdm(
                concurrent.futures.as_completed(
                    futuros
                ),
                total=len(futuros),
                desc="Open Library - título",
            ):

                try:

                    (
                        book_id,
                        resultado,
                    ) = futuro.result()

                    if (
                        resultado
                        and resultado.get(
                            "status"
                        )
                        == "matched"
                    ):

                        cache[
                            book_id
                        ] = resultado

                        total_encontrados += 1

                    elif (
                        resultado
                        and resultado.get(
                            "status"
                        )
                        == "rate_limited"
                    ):

                        print(
                            "[Open Library] "
                            "Rate limit detectado."
                        )

                        # Não adicionamos esse livro ao cache.
                        # Assim ele poderá ser retomado.
                        break

                except Exception as erro:

                    print(
                        "[Open Library] "
                        f"Erro em tarefa: {erro}"
                    )

                if (
                    total_encontrados
                    and total_encontrados
                    % CACHE_SAVE_EVERY
                    == 0
                ):

                    salvar_json(
                        ARQUIVO_CACHE_OPENLIBRARY,
                        cache,
                    )

    except KeyboardInterrupt:

        print(
            "\n[Open Library] "
            "Interrupção detectada. "
            "Salvando cache..."
        )

        salvar_json(
            ARQUIVO_CACHE_OPENLIBRARY,
            cache,
        )

        raise

    except Exception as erro:

        print(
            f"[Open Library] "
            f"Erro inesperado: {erro}"
        )

        salvar_json(
            ARQUIVO_CACHE_OPENLIBRARY,
            cache,
        )

    finally:

        salvar_json(
            ARQUIVO_CACHE_OPENLIBRARY,
            cache,
        )

    print(
        f"[Open Library] novos matches "
        f"por título: "
        f"{total_encontrados:,}"
    )


# ============================================================
# GOOGLE BOOKS
# ============================================================

def buscar_google(
    session: requests.Session,
    row: pd.Series,
) -> dict[str, Any] | None:

    book_id = str(
        row["book_id"]
    ).strip()

    isbn13 = isbn_limpo(
        row.get("isbn13")
    )

    isbn10 = isbn_limpo(
        row.get("isbn")
    )

    titulo = (
        limpar(
            row.get(
                "title_without_series"
            )
        )
        or limpar(
            row.get("title")
        )
    )

    queries = []

    if isbn13:

        queries.append(
            (
                f"isbn:{isbn13}",
                "google_isbn13",
            )
        )

    if isbn10:

        queries.append(
            (
                f"isbn:{isbn10}",
                "google_isbn10",
            )
        )

    titulo_limpo = titulo_busca(
        titulo
    )

    if titulo_limpo:

        queries.append(
            (
                f'intitle:"{titulo_limpo}"',
                "google_title",
            )
        )

    for query, metodo in queries:

        params = {
            "q":
                query,

            "maxResults":
                GOOGLE_MAX_RESULTS,

            "printType":
                "books",
        }

        if GOOGLE_API_KEY:

            params["key"] = (
                GOOGLE_API_KEY
            )

        resposta = get_com_retry(
            session,
            GOOGLE_BOOKS_URL,
            params=params,
            headers=GOOGLE_HEADERS,
            timeout=GOOGLE_TIMEOUT,
            fonte="Google Books",
        )

        if resposta is None:
            continue

        if resposta.status_code == 429:

            return {
                "status":
                    "rate_limited",

                "source":
                    "google_books",
            }

        if resposta.status_code != 200:
            continue

        try:

            dados = resposta.json()

        except ValueError:

            continue

        itens = dados.get(
            "items",
            [],
        )

        if not itens:
            continue

        melhor = None

        melhor_score = -1

        for item in itens:

            if not isinstance(
                item,
                dict,
            ):
                continue

            info = item.get(
                "volumeInfo",
                {},
            )

            sim_titulo = similaridade(
                titulo,
                info.get(
                    "title",
                    "",
                ),
            )

            score = sim_titulo

            if metodo.startswith(
                "google_isbn"
            ):

                score += 2

            if score > melhor_score:

                melhor_score = score

                melhor = item

        if melhor is None:
            continue

        info = melhor.get(
            "volumeInfo",
            {},
        )

        sale = melhor.get(
            "saleInfo",
            {},
        )

        access = melhor.get(
            "accessInfo",
            {},
        )

        autores = juntar_lista(
            info.get(
                "authors",
                [],
            )
        )

        categorias = juntar_lista(
            info.get(
                "categories",
                [],
            )
        )

        isbn_10 = ""

        isbn_13 = ""

        identificadores = info.get(
            "industryIdentifiers",
            [],
        )

        if isinstance(
            identificadores,
            list,
        ):

            for identifier in (
                identificadores
            ):

                if not isinstance(
                    identifier,
                    dict,
                ):
                    continue

                tipo = limpar(
                    identifier.get(
                        "type",
                        "",
                    )
                )

                valor = isbn_limpo(
                    identifier.get(
                        "identifier",
                        "",
                    )
                )

                if tipo == "ISBN_10":
                    isbn_10 = valor

                elif tipo == "ISBN_13":
                    isbn_13 = valor

        imagens = info.get(
            "imageLinks",
            {},
        )

        if not isinstance(
            imagens,
            dict,
        ):
            imagens = {}

        return {
            "status":
                "matched",

            "source":
                "google_books",

            "google_volume_id":
                limpar(
                    melhor.get("id")
                ),

            "google_title":
                limpar(
                    info.get("title")
                ),

            "google_subtitle":
                limpar(
                    info.get("subtitle")
                ),

            "google_authors":
                autores,

            "google_publisher":
                limpar(
                    info.get("publisher")
                ),

            "google_published_date":
                limpar(
                    info.get(
                        "publishedDate"
                    )
                ),

            "google_description":
                limpar(
                    info.get(
                        "description"
                    )
                ),

            "google_page_count":
                info.get(
                    "pageCount"
                ),

            "google_categories":
                categorias,

            "google_average_rating":
                info.get(
                    "averageRating"
                ),

            "google_ratings_count":
                info.get(
                    "ratingsCount"
                ),

            "google_language":
                limpar(
                    info.get(
                        "language"
                    )
                ),

            "google_isbn10":
                isbn_10,

            "google_isbn13":
                isbn_13,

            "google_maturity_rating":
                limpar(
                    info.get(
                        "maturityRating"
                    )
                ),

            "google_print_type":
                limpar(
                    info.get(
                        "printType"
                    )
                ),

            "google_thumbnail":
                limpar(
                    imagens.get(
                        "thumbnail"
                    )
                ),

            "google_small_thumbnail":
                limpar(
                    imagens.get(
                        "smallThumbnail"
                    )
                ),

            "google_preview_link":
                limpar(
                    info.get(
                        "previewLink"
                    )
                ),

            "google_info_link":
                limpar(
                    info.get(
                        "infoLink"
                    )
                ),

            "google_web_reader_link":
                limpar(
                    access.get(
                        "webReaderLink"
                    )
                ),

            "google_viewability":
                limpar(
                    access.get(
                        "viewability"
                    )
                ),

            "google_public_domain":
                bool(
                    access.get(
                        "publicDomain",
                        False,
                    )
                ),

            "google_ebook_available":
                bool(
                    "epub" in access
                    or "pdf" in access
                ),

            "google_saleability":
                limpar(
                    sale.get(
                        "saleability"
                    )
                ),

            "google_search_method":
                metodo,

            "goodreads_book_id":
                book_id,
        }

    return None


def consultar_google(
    df_books: pd.DataFrame,
    cache: dict[str, Any],
) -> None:

    if SKIP_GOOGLE:

        print(
            "[Google Books] "
            "etapa desativada."
        )

        return

    pendentes = []

    for _, row in df_books.iterrows():

        book_id = str(
            row["book_id"]
        ).strip()

        if book_id not in cache:

            pendentes.append(
                row
            )

    if MAX_NEW_GOOGLE_BOOKS:

        pendentes = pendentes[
            :MAX_NEW_GOOGLE_BOOKS
        ]

    print(
        f"[Google Books] livros pendentes: "
        f"{len(pendentes):,}"
    )

    if not pendentes:
        return

    session = requests.Session()

    encontrados = 0

    try:

        for row in tqdm(
            pendentes,
            desc="Google Books",
        ):

            book_id = str(
                row["book_id"]
            ).strip()

            resultado = buscar_google(
                session,
                row,
            )

            if (
                resultado
                and resultado.get(
                    "status"
                )
                == "matched"
            ):

                cache[
                    book_id
                ] = resultado

                encontrados += 1

            elif (
                resultado
                and resultado.get(
                    "status"
                )
                == "rate_limited"
            ):

                print(
                    "[Google Books] "
                    "HTTP 429 persistente. "
                    "Interrompendo a etapa."
                )

                # O livro atual NÃO é colocado no cache.
                # Assim ele será novamente tentado na próxima execução.
                break

            # Delay entre livros.
            time.sleep(
                GOOGLE_DELAY
            )

            if (
                encontrados
                and encontrados
                % CACHE_SAVE_EVERY
                == 0
            ):

                salvar_json(
                    ARQUIVO_CACHE_GOOGLE,
                    cache,
                )

    except KeyboardInterrupt:

        print(
            "\n[Google Books] "
            "Interrupção detectada. "
            "Salvando cache..."
        )

        salvar_json(
            ARQUIVO_CACHE_GOOGLE,
            cache,
        )

        raise

    except Exception as erro:

        print(
            f"[Google Books] "
            f"Erro inesperado: {erro}"
        )

        salvar_json(
            ARQUIVO_CACHE_GOOGLE,
            cache,
        )

    finally:

        salvar_json(
            ARQUIVO_CACHE_GOOGLE,
            cache,
        )

    print(
        f"[Google Books] novos matches: "
        f"{encontrados:,}"
    )


# ============================================================
# CONSOLIDAÇÃO
# ============================================================

def records_from_cache(
    df_books: pd.DataFrame,
    cache_openlibrary: dict[str, Any],
    cache_google: dict[str, Any],
) -> pd.DataFrame:

    registros = []

    for _, row in df_books.iterrows():

        book_id = str(
            row["book_id"]
        ).strip()

        registro = {
            "book_id":
                book_id,

            "openlibrary_match":
                False,

            "openlibrary_status":
                "not_found",

            "google_match":
                False,

            "google_status":
                "not_found",
        }

        ol = cache_openlibrary.get(
            book_id
        )

        if isinstance(
            ol,
            dict,
        ):

            registro.update(
                {
                    chave: valor
                    for chave, valor
                    in ol.items()
                    if (
                        chave.startswith(
                            "openlibrary_"
                        )
                        or chave == "status"
                    )
                }
            )

            registro[
                "openlibrary_match"
            ] = (
                ol.get("status")
                == "matched"
            )

            registro[
                "openlibrary_status"
            ] = ol.get(
                "status",
                "unknown",
            )

        gb = cache_google.get(
            book_id
        )

        if isinstance(
            gb,
            dict,
        ):

            registro.update(
                {
                    chave: valor
                    for chave, valor
                    in gb.items()
                    if chave.startswith(
                        "google_"
                    )
                }
            )

            registro[
                "google_match"
            ] = (
                gb.get("status")
                == "matched"
            )

            registro[
                "google_status"
            ] = gb.get(
                "status",
                "unknown",
            )

        registros.append(
            registro
        )

    return pd.DataFrame(
        registros
    )


# ============================================================
# FALLBACK
# ============================================================

def preencher_fallback(
    df: pd.DataFrame,
    destino: str,
    *fontes: str,
) -> None:

    resultado = pd.Series(
        pd.NA,
        index=df.index,
        dtype="object",
    )

    for fonte in fontes:

        if fonte not in df.columns:
            continue

        serie = df[fonte]

        vazios = (
            resultado.isna()
            | resultado.astype(
                "string"
            )
            .str.strip()
            .eq("")
        )

        resultado.loc[
            vazios
        ] = serie.loc[vazios]

    df[destino] = resultado


# ============================================================
# NORMALIZAÇÃO PARA PARQUET
# ============================================================

def normalizar_tipos_para_parquet(
    livros: pd.DataFrame,
    reviews: pd.DataFrame,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
]:

    # --------------------------------------------------------
    # Inteiros
    # --------------------------------------------------------

    colunas_inteiras = [
        "openlibrary_pages",
        "google_page_count",
        "google_ratings_count",
        "num_pages",
        "pages_consolidated",
    ]

    for coluna in colunas_inteiras:

        if coluna in livros.columns:

            livros[coluna] = (
                pd.to_numeric(
                    livros[coluna],
                    errors="coerce",
                )
                .astype("Int64")
            )

        if coluna in reviews.columns:

            reviews[coluna] = (
                pd.to_numeric(
                    reviews[coluna],
                    errors="coerce",
                )
                .astype("Int64")
            )

    # --------------------------------------------------------
    # Floats
    # --------------------------------------------------------

    colunas_float = [
        "google_average_rating",
    ]

    for coluna in colunas_float:

        if coluna in livros.columns:

            livros[coluna] = (
                pd.to_numeric(
                    livros[coluna],
                    errors="coerce",
                )
            )

        if coluna in reviews.columns:

            reviews[coluna] = (
                pd.to_numeric(
                    reviews[coluna],
                    errors="coerce",
                )
            )

    # --------------------------------------------------------
    # Booleanos
    # --------------------------------------------------------

    colunas_bool = [
        "google_public_domain",
        "google_ebook_available",
        "openlibrary_match",
        "google_match",
    ]

    for coluna in colunas_bool:

        if coluna in livros.columns:

            livros[coluna] = (
                livros[coluna]
                .fillna(False)
                .astype(bool)
            )

        if coluna in reviews.columns:

            reviews[coluna] = (
                reviews[coluna]
                .fillna(False)
                .astype(bool)
            )

    return (
        livros,
        reviews,
    )


# ============================================================
# DIAGNÓSTICO DE TIPOS MISTOS
# ============================================================

def verificar_colunas_object(
    df: pd.DataFrame,
    nome: str,
) -> None:

    for coluna in df.select_dtypes(
        include=["object"]
    ).columns:

        valores = df[coluna].dropna()

        if valores.empty:
            continue

        tipos = {
            type(valor).__name__
            for valor in valores
        }

        if len(tipos) > 1:

            print(
                f"[Aviso] {nome}.{coluna}: "
                f"tipos mistos encontrados: "
                f"{sorted(tipos)}"
            )


# ============================================================
# SALVAR SAÍDAS
# ============================================================

def salvar_saidas(
    df_books: pd.DataFrame,
    df_reviews: pd.DataFrame,
    cache_openlibrary: dict[str, Any],
    cache_google: dict[str, Any],
) -> None:

    api_df = records_from_cache(
        df_books,
        cache_openlibrary,
        cache_google,
    )

    livros = df_books.copy()

    livros["book_id"] = (
        livros["book_id"]
        .astype(str)
        .str.strip()
    )

    livros = livros.merge(
        api_df,
        on="book_id",
        how="left",
    )

    # --------------------------------------------------------
    # Fallback de páginas
    # --------------------------------------------------------

    preencher_fallback(
        livros,
        "pages_consolidated",
        "num_pages",
        "google_page_count",
        "openlibrary_pages",
    )

    # --------------------------------------------------------
    # Fallback de idioma
    # --------------------------------------------------------

    preencher_fallback(
        livros,
        "language_consolidated",
        "language_code",
        "google_language",
        "openlibrary_languages",
    )

    # --------------------------------------------------------
    # Fallback de editora
    # --------------------------------------------------------

    preencher_fallback(
        livros,
        "publisher_consolidated",
        "publisher",
        "google_publisher",
        "openlibrary_publishers",
    )

    # --------------------------------------------------------
    # Reviews
    # --------------------------------------------------------

    reviews = df_reviews.copy()

    reviews["book_id"] = (
        reviews["book_id"]
        .astype(str)
        .str.strip()
    )

    colunas_api = [
        coluna
        for coluna in api_df.columns
        if coluna != "book_id"
    ]

    reviews = reviews.merge(
        api_df[
            ["book_id"]
            + colunas_api
        ],
        on="book_id",
        how="left",
    )

    # --------------------------------------------------------
    # Normalização antes do Parquet
    # --------------------------------------------------------

    livros, reviews = (
        normalizar_tipos_para_parquet(
            livros,
            reviews,
        )
    )

    # --------------------------------------------------------
    # Diagnóstico
    # --------------------------------------------------------

    verificar_colunas_object(
        livros,
        "livros",
    )

    verificar_colunas_object(
        reviews,
        "reviews",
    )

    # --------------------------------------------------------
    # Salvamento
    # --------------------------------------------------------

    livros.to_parquet(
        ARQUIVO_BOOKS_ENRIQUECIDO,
        index=False,
    )

    reviews.to_parquet(
        ARQUIVO_REVIEWS_ENRIQUECIDO,
        index=False,
    )

    print(
        f"\nLivros salvos em:\n"
        f"{ARQUIVO_BOOKS_ENRIQUECIDO}"
    )

    print(
        f"Reviews salvas em:\n"
        f"{ARQUIVO_REVIEWS_ENRIQUECIDO}"
    )


# ============================================================
# ESTATÍSTICAS
# ============================================================

def contar_matches(
    cache: dict[str, Any],
) -> int:

    return sum(
        1
        for item in cache.values()
        if (
            isinstance(
                item,
                dict,
            )
            and item.get(
                "status"
            )
            == "matched"
        )
    )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    PASTA_PROCESSED.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("=" * 70)

    print(
        "03 - GOODREADS + OPEN LIBRARY + GOOGLE BOOKS"
    )

    print("=" * 70)

    # --------------------------------------------------------
    # Verificação dos arquivos
    # --------------------------------------------------------

    if not ARQUIVO_BOOKS.exists():

        raise FileNotFoundError(
            f"Arquivo não encontrado:\n"
            f"{ARQUIVO_BOOKS}"
        )

    if not ARQUIVO_REVIEWS.exists():

        raise FileNotFoundError(
            f"Arquivo não encontrado:\n"
            f"{ARQUIVO_REVIEWS}"
        )

    # --------------------------------------------------------
    # Leitura
    # --------------------------------------------------------

    df_books = pd.read_parquet(
        ARQUIVO_BOOKS
    )

    df_reviews = pd.read_parquet(
        ARQUIVO_REVIEWS
    )

    if "book_id" not in df_books.columns:

        raise ValueError(
            "A base de livros não possui "
            "book_id."
        )

    print(
        f"Livros: "
        f"{len(df_books):,}"
    )

    print(
        f"Reviews: "
        f"{len(df_reviews):,}"
    )

    # --------------------------------------------------------
    # Migração / carregamento dos caches
    # --------------------------------------------------------

    (
        cache_openlibrary,
        cache_google,
    ) = migrar_caches()

    print(
        f"Cache Open Library: "
        f"{len(cache_openlibrary):,}"
    )

    print(
        f"Cache Google Books: "
        f"{len(cache_google):,}"
    )

    # --------------------------------------------------------
    # Execução
    # --------------------------------------------------------

    if not ONLY_MIGRATE:

        # ================================================
        # Open Library por ISBN
        # ================================================

        consultar_openlibrary_batch(
            df_books,
            cache_openlibrary,
        )

        # ================================================
        # Open Library por título
        # ================================================

        consultar_openlibrary_titulos(
            df_books,
            cache_openlibrary,
        )

        # ================================================
        # Google Books
        # ================================================

        consultar_google(
            df_books,
            cache_google,
        )

    # --------------------------------------------------------
    # Salvar resultados consolidados
    # --------------------------------------------------------

    salvar_saidas(
        df_books,
        df_reviews,
        cache_openlibrary,
        cache_google,
    )

    # --------------------------------------------------------
    # Estatísticas
    # --------------------------------------------------------

    livros_ol = contar_matches(
        cache_openlibrary
    )

    livros_google = contar_matches(
        cache_google
    )

    print(
        "\n"
        + "=" * 70
    )

    print("RESUMO")

    print(
        "=" * 70
    )

    if len(df_books):

        print(
            f"Open Library: "
            f"{livros_ol:,} "
            f"/ {len(df_books):,} "
            f"("
            f"{livros_ol / len(df_books) * 100:.2f}%"
            f")"
        )

        print(
            f"Google Books: "
            f"{livros_google:,} "
            f"/ {len(df_books):,} "
            f"("
            f"{livros_google / len(df_books) * 100:.2f}%"
            f")"
        )

    else:

        print(
            "Open Library: 0"
        )

        print(
            "Google Books: 0"
        )

    print(
        "\nIMPORTANTE:"
    )

    print(
        "Os resultados ainda serão auditados "
        "pelo 04_validar_matches.py."
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        print(
            "\nExecução interrompida pelo usuário."
        )

        sys.exit(130)

    except Exception as erro:

        print(
            "\nERRO FATAL:"
        )

        print(
            repr(erro)
        )

        sys.exit(1)


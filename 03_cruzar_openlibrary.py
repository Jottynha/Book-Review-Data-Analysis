#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
03_cruzar_openlibrary.py

Primeira etapa do cruzamento externo:
    Goodreads -> Open Library

Estratégia:
1. Usa ISBN-13/ISBN-10 em lote pelo endpoint /api/books.
2. Livros sem resultado por ISBN são buscados pelo Search API usando
   título + autor e, por último, título.
3. Mantém cache independente em processed/openlibrary_cache.json.
4. Permite interromper e continuar sem refazer livros já processados.
5. Salva uma tabela independente da Open Library; NÃO chama Google Books.

Saídas:
    processed/openlibrary_cache.json
    processed/goodreads_books_openlibrary_100k.parquet
    processed/goodreads_reviews_openlibrary_100k.parquet
"""

from __future__ import annotations

import concurrent.futures
import json
import re
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
PASTA_PROCESSED = BASE_DIR / "processed"

ARQUIVO_BOOKS = PASTA_PROCESSED / "goodreads_books_100k.parquet"
ARQUIVO_REVIEWS = PASTA_PROCESSED / "goodreads_reviews_100k.parquet"

ARQUIVO_CACHE = PASTA_PROCESSED / "openlibrary_cache.json"
ARQUIVO_CACHE_ANTIGO = PASTA_PROCESSED / "google_books_cache.json"

ARQUIVO_BOOKS_SAIDA = (
    PASTA_PROCESSED / "goodreads_books_openlibrary_100k.parquet"
)
ARQUIVO_REVIEWS_SAIDA = (
    PASTA_PROCESSED / "goodreads_reviews_openlibrary_100k.parquet"
)

OPENLIBRARY_BATCH_URL = "https://openlibrary.org/api/books"
OPENLIBRARY_SEARCH_URL = "https://openlibrary.org/search.json"

# A API /api/books permite consultar vários ISBNs em uma requisição.
OPENLIBRARY_BATCH_SIZE = 50

# Concorrência baixa para o Search API.
OPENLIBRARY_SEARCH_WORKERS = 3
OPENLIBRARY_SEARCH_LIMIT = 5
OPENLIBRARY_DELAY = 0.40

MAX_RETRIES_429 = 5
BACKOFF_BASE = 2.0
BACKOFF_MAX = 120.0

CACHE_SAVE_EVERY = 25

HEADERS = {
    "User-Agent": (
        "Book-Review-Data-Analysis/2.0 "
        "(academic-data-science-project)"
    ),
    "Accept": "application/json",
}


# ============================================================
# UTILITÁRIOS
# ============================================================

def limpar(valor: Any) -> str:
    if valor is None:
        return ""
    try:
        if pd.isna(valor):
            return ""
    except (TypeError, ValueError):
        pass
    texto = str(valor).strip()
    if texto.lower() in {"", "nan", "none", "null"}:
        return ""
    return texto


def isbn_limpo(valor: Any) -> str:
    return re.sub(r"[^0-9X]", "", limpar(valor).upper())


def normalizar_texto(valor: Any) -> str:
    texto = limpar(valor).lower()
    if not texto:
        return ""
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(
        c for c in texto if not unicodedata.combining(c)
    )
    texto = re.sub(r"[^\w\s]", " ", texto, flags=re.UNICODE)
    return re.sub(r"\s+", " ", texto).strip()


def similaridade(a: Any, b: Any) -> float:
    a = normalizar_texto(a)
    b = normalizar_texto(b)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def primeiro_autor(valor: Any) -> str:
    texto = limpar(valor)
    if "|" in texto:
        return texto.split("|")[0].strip()
    if isinstance(valor, list):
        return limpar(valor[0]) if valor else ""
    return texto


def juntar_lista(valor: Any) -> str:
    if isinstance(valor, list):
        return "|".join(
            limpar(item) for item in valor if limpar(item)
        )
    return limpar(valor)


def titulo_busca(valor: Any) -> str:
    titulo = limpar(valor)
    titulo = re.sub(r"\s*\([^)]*\)", "", titulo)
    return titulo.strip()


def salvar_json(caminho: Path, dados: dict[str, Any]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    temp = caminho.with_suffix(caminho.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    temp.replace(caminho)


def carregar_json(caminho: Path) -> dict[str, Any]:
    if not caminho.exists():
        return {}
    try:
        with caminho.open("r", encoding="utf-8") as f:
            dados = json.load(f)
        return dados if isinstance(dados, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def get_retry(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, Any],
    timeout: int = 30,
):
    for tentativa in range(1, MAX_RETRIES_429 + 1):
        try:
            resposta = session.get(
                url,
                params=params,
                headers=HEADERS,
                timeout=timeout,
            )
        except requests.RequestException as erro:
            if tentativa == MAX_RETRIES_429:
                print(f"[Open Library] erro de rede: {erro}")
                return None
            espera = min(BACKOFF_MAX, BACKOFF_BASE ** tentativa)
            time.sleep(espera)
            continue

        if resposta.status_code != 429:
            return resposta

        retry_after = resposta.headers.get("Retry-After")
        if retry_after:
            try:
                espera = min(float(retry_after), BACKOFF_MAX)
            except ValueError:
                espera = min(BACKOFF_MAX, BACKOFF_BASE ** tentativa)
        else:
            espera = min(BACKOFF_MAX, BACKOFF_BASE ** tentativa)

        print(
            f"[Open Library] HTTP 429; aguardando {espera:.1f}s "
            f"({tentativa}/{MAX_RETRIES_429})..."
        )
        time.sleep(espera)

    return None


# ============================================================
# MIGRAÇÃO DO CACHE ANTIGO
# ============================================================

def parece_openlibrary(registro: Any) -> bool:
    if not isinstance(registro, dict):
        return False
    origem = limpar(registro.get("api_source")).lower()
    tipo = limpar(registro.get("google_kind")).lower()
    metodo = limpar(registro.get("search_method")).lower()
    return (
        origem.startswith("openlibrary")
        or "openlibrary" in tipo
        or "openlibrary" in metodo
    )


def migrar_cache_antigo(cache: dict[str, Any]) -> int:
    antigo = carregar_json(ARQUIVO_CACHE_ANTIGO)
    migrados = 0

    for book_id, registro in antigo.items():
        if not parece_openlibrary(registro):
            continue
        book_id = str(book_id)
        if book_id in cache:
            continue

        cache[book_id] = {
            "status": "matched",
            "source": "openlibrary",
            "openlibrary_key": registro.get("google_volume_id", ""),
            "openlibrary_title": registro.get("google_title", ""),
            "openlibrary_authors": registro.get("google_authors", ""),
            "openlibrary_publishers": registro.get("google_publisher", ""),
            "openlibrary_published_date": registro.get("google_published_date", ""),
            "openlibrary_description": registro.get("google_description", ""),
            "openlibrary_pages": registro.get("google_page_count", ""),
            "openlibrary_subjects": registro.get("google_categories", ""),
            "openlibrary_isbn10": registro.get("google_isbn10", ""),
            "openlibrary_isbn13": registro.get("google_isbn13", ""),
            "openlibrary_thumbnail": registro.get("google_thumbnail", ""),
            "openlibrary_url": (
                registro.get("google_info_link", "")
                or registro.get("google_preview_link", "")
            ),
            "openlibrary_search_method": registro.get(
                "search_method", "cache_migrated"
            ),
            "goodreads_book_id": book_id,
        }
        migrados += 1

    if migrados:
        salvar_json(ARQUIVO_CACHE, cache)

    print(f"Cache Open Library migrado do cache antigo: {migrados:,}")
    return migrados


# ============================================================
# EXTRAÇÃO
# ============================================================

def extrair_batch(
    dados: dict[str, Any],
    book_id: str,
    metodo: str,
) -> dict[str, Any]:
    autores = []
    for autor in dados.get("authors", []):
        if isinstance(autor, dict):
            nome = limpar(autor.get("name"))
            if nome:
                autores.append(nome)

    subjects = []
    for subject in dados.get("subjects", []):
        nome = (
            limpar(subject.get("name"))
            if isinstance(subject, dict)
            else limpar(subject)
        )
        if nome:
            subjects.append(nome)

    publishers = []
    for publisher in dados.get("publishers", []):
        nome = (
            limpar(publisher.get("name"))
            if isinstance(publisher, dict)
            else limpar(publisher)
        )
        if nome:
            publishers.append(nome)

    identifiers = dados.get("identifiers", {})
    isbn10 = ""
    isbn13 = ""
    if isinstance(identifiers, dict):
        vals10 = identifiers.get("isbn_10", [])
        vals13 = identifiers.get("isbn_13", [])
        if vals10:
            isbn10 = isbn_limpo(vals10[0])
        if vals13:
            isbn13 = isbn_limpo(vals13[0])

    cover = dados.get("cover", {})
    thumbnail = ""
    if isinstance(cover, dict):
        thumbnail = (
            limpar(cover.get("medium"))
            or limpar(cover.get("large"))
            or limpar(cover.get("small"))
        )

    return {
        "status": "matched",
        "source": "openlibrary",
        "openlibrary_key": limpar(dados.get("key")),
        "openlibrary_title": limpar(dados.get("title")),
        "openlibrary_subtitle": limpar(dados.get("subtitle")),
        "openlibrary_authors": "|".join(autores),
        "openlibrary_publishers": "|".join(publishers),
        "openlibrary_published_date": limpar(dados.get("publish_date")),
        "openlibrary_description": limpar(dados.get("notes")),
        "openlibrary_pages": limpar(
            dados.get("number_of_pages") or dados.get("pagination")
        ),
        "openlibrary_subjects": "|".join(subjects[:30]),
        "openlibrary_isbn10": isbn10,
        "openlibrary_isbn13": isbn13,
        "openlibrary_thumbnail": thumbnail,
        "openlibrary_url": (
            "https://openlibrary.org" + limpar(dados.get("key"))
            if limpar(dados.get("key"))
            else ""
        ),
        "openlibrary_search_method": metodo,
        "goodreads_book_id": book_id,
    }


def escolher_candidato(
    row: pd.Series,
    docs: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, float, float, float]:
    titulo_gr = limpar(row.get("title_without_series")) or limpar(row.get("title"))
    autor_gr = primeiro_autor(row.get("authors"))
    ano_gr = limpar(row.get("publication_year"))

    melhor = None
    melhor_score = -1.0
    melhor_titulo = 0.0
    melhor_autor = 0.0

    for doc in docs:
        titulo = limpar(doc.get("title"))
        autores = doc.get("author_name", [])
        if not isinstance(autores, list):
            autores = []

        sim_titulo = similaridade(titulo_gr, titulo)
        sim_autor = 0.0
        for autor in autores[:10]:
            sim_autor = max(sim_autor, similaridade(autor_gr, autor))

        score = 70.0 * sim_titulo + 25.0 * sim_autor

        ano_doc = doc.get("first_publish_year")
        if ano_gr and ano_doc:
            try:
                diff = abs(int(ano_gr) - int(ano_doc))
                if diff == 0:
                    score += 5.0
                elif diff <= 3:
                    score += 2.0
                elif diff > 10:
                    score -= 10.0
            except (ValueError, TypeError):
                pass

        if score > melhor_score:
            melhor = doc
            melhor_score = score
            melhor_titulo = sim_titulo
            melhor_autor = sim_autor

    return melhor, melhor_score, melhor_titulo, melhor_autor


def extrair_search(
    doc: dict[str, Any],
    book_id: str,
    metodo: str,
    score: float,
    sim_titulo: float,
    sim_autor: float,
) -> dict[str, Any]:
    isbns = doc.get("isbn", [])
    isbn10 = ""
    isbn13 = ""
    if isinstance(isbns, list):
        for item in isbns:
            code = isbn_limpo(item)
            if len(code) == 13 and not isbn13:
                isbn13 = code
            elif len(code) == 10 and not isbn10:
                isbn10 = code

    subjects = doc.get("subject", [])
    if not isinstance(subjects, list):
        subjects = []

    return {
        "status": "matched",
        "source": "openlibrary",
        "openlibrary_key": limpar(doc.get("key")),
        "openlibrary_title": limpar(doc.get("title")),
        "openlibrary_subtitle": limpar(doc.get("subtitle")),
        "openlibrary_authors": juntar_lista(doc.get("author_name", [])),
        "openlibrary_publishers": juntar_lista(doc.get("publisher", [])),
        "openlibrary_published_date": limpar(doc.get("first_publish_year")),
        "openlibrary_description": "",
        "openlibrary_pages": limpar(doc.get("number_of_pages_median")),
        "openlibrary_subjects": juntar_lista(subjects[:30]),
        "openlibrary_isbn10": isbn10,
        "openlibrary_isbn13": isbn13,
        "openlibrary_thumbnail": (
            f"https://covers.openlibrary.org/b/id/{doc.get('cover_i')}-M.jpg"
            if doc.get("cover_i")
            else ""
        ),
        "openlibrary_url": (
            "https://openlibrary.org" + limpar(doc.get("key"))
            if limpar(doc.get("key"))
            else ""
        ),
        "openlibrary_search_method": metodo,
        "openlibrary_search_score": round(score, 2),
        "openlibrary_title_similarity": round(sim_titulo, 4),
        "openlibrary_author_similarity": round(sim_autor, 4),
        "goodreads_book_id": book_id,
    }


# ============================================================
# ETAPA 1: ISBN
# ============================================================

def consultar_por_isbn(
    books: pd.DataFrame,
    cache: dict[str, Any],
) -> None:
    pendentes: dict[str, list[str]] = {}

    for _, row in books.iterrows():
        book_id = str(row["book_id"]).strip()
        if book_id in cache:
            continue

        isbn13 = isbn_limpo(row.get("isbn13"))
        isbn10 = isbn_limpo(row.get("isbn"))
        codigo = isbn13 or isbn10

        if codigo:
            pendentes.setdefault(codigo, []).append(book_id)

    codigos = list(pendentes)

    print(f"[Open Library] ISBNs ainda não processados: {len(codigos):,}")

    if not codigos:
        return

    session = requests.Session()
    encontrados = 0

    try:
        for inicio in tqdm(
            range(0, len(codigos), OPENLIBRARY_BATCH_SIZE),
            desc="Open Library - ISBN",
        ):
            lote = codigos[inicio : inicio + OPENLIBRARY_BATCH_SIZE]

            resposta = get_retry(
                session,
                OPENLIBRARY_BATCH_URL,
                params={
                    "bibkeys": ",".join(f"ISBN:{c}" for c in lote),
                    "format": "json",
                    "jscmd": "data",
                },
            )

            if resposta is None:
                continue

            if resposta.status_code == 429:
                print("[Open Library] 429 persistente; encerrando ISBN.")
                break

            if resposta.status_code != 200:
                time.sleep(OPENLIBRARY_DELAY)
                continue

            try:
                dados = resposta.json()
            except ValueError:
                continue

            for codigo in lote:
                chave = f"ISBN:{codigo}"
                if chave not in dados:
                    continue
                for book_id in pendentes[codigo]:
                    cache[book_id] = extrair_batch(
                        dados[chave], book_id, "openlibrary_isbn"
                    )
                    encontrados += 1

            salvar_json(ARQUIVO_CACHE, cache)
            time.sleep(OPENLIBRARY_DELAY)

    except KeyboardInterrupt:
        salvar_json(ARQUIVO_CACHE, cache)
        print("\nInterrupção: cache salvo.")
        raise

    print(f"[Open Library] novos matches por ISBN: {encontrados:,}")


# ============================================================
# ETAPA 2: TÍTULO + AUTOR / TÍTULO
# ============================================================

def buscar_por_titulo(
    row: pd.Series,
) -> tuple[str, dict[str, Any] | None]:
    session = requests.Session()

    book_id = str(row["book_id"]).strip()
    titulo = (
        limpar(row.get("title_without_series"))
        or limpar(row.get("title"))
    )
    titulo = titulo_busca(titulo)
    autor = primeiro_autor(row.get("authors"))

    if not titulo:
        return book_id, {
            "status": "insufficient_data",
            "source": "openlibrary",
        }

    params = {
        "limit": OPENLIBRARY_SEARCH_LIMIT,
        "fields": (
            "key,title,subtitle,author_name,first_publish_year,"
            "publisher,isbn,subject,number_of_pages_median,cover_i"
        ),
    }

    if autor:
        params["title"] = titulo
        params["author"] = autor
        metodo = "openlibrary_title_author"
    else:
        params["title"] = titulo
        metodo = "openlibrary_title"

    resposta = get_retry(
        session,
        OPENLIBRARY_SEARCH_URL,
        params=params,
        timeout=30,
    )

    if resposta is None:
        return book_id, None

    if resposta.status_code == 429:
        return book_id, {"status": "rate_limited", "source": "openlibrary"}

    if resposta.status_code != 200:
        return book_id, None

    try:
        docs = resposta.json().get("docs", [])
    except ValueError:
        return book_id, None

    if not docs:
        return book_id, {
            "status": "no_match",
            "source": "openlibrary",
        }

    melhor, score, sim_titulo, sim_autor = escolher_candidato(row, docs)

    # Não aceitar título aparentemente aleatório.
    if melhor is None or score < 70 or sim_titulo < 0.85:
        return book_id, {
            "status": "no_match",
            "source": "openlibrary",
            "openlibrary_search_method": metodo,
            "openlibrary_search_score": round(score, 2),
            "openlibrary_title_similarity": round(sim_titulo, 4),
            "openlibrary_author_similarity": round(sim_autor, 4),
        }

    return book_id, extrair_search(
        melhor,
        book_id,
        metodo,
        score,
        sim_titulo,
        sim_autor,
    )


def consultar_por_titulo(
    books: pd.DataFrame,
    cache: dict[str, Any],
) -> None:
    pendentes = [
        row
        for _, row in books.iterrows()
        if str(row["book_id"]).strip() not in cache
    ]

    print(f"[Open Library] títulos pendentes: {len(pendentes):,}")

    if not pendentes:
        return

    encontrados = 0

    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=OPENLIBRARY_SEARCH_WORKERS
        ) as executor:
            futuros = [
                executor.submit(buscar_por_titulo, row)
                for row in pendentes
            ]

            for i, futuro in enumerate(
                tqdm(
                    concurrent.futures.as_completed(futuros),
                    total=len(futuros),
                    desc="Open Library - título",
                ),
                start=1,
            ):
                try:
                    book_id, resultado = futuro.result()
                except Exception as erro:
                    print(f"[Open Library] erro em tarefa: {erro}")
                    continue

                if resultado and resultado.get("status") == "matched":
                    cache[book_id] = resultado
                    encontrados += 1
                elif resultado and resultado.get("status") == "no_match":
                    # Só gravamos no cache uma ausência real de resultado.
                    cache[book_id] = resultado
                elif resultado and resultado.get("status") == "insufficient_data":
                    cache[book_id] = resultado
                elif resultado and resultado.get("status") == "rate_limited":
                    print(
                        "[Open Library] Rate limit detectado. "
                        "Parando buscas por título; retome depois."
                    )
                    break

                if i % CACHE_SAVE_EVERY == 0:
                    salvar_json(ARQUIVO_CACHE, cache)

                time.sleep(OPENLIBRARY_DELAY / max(1, OPENLIBRARY_SEARCH_WORKERS))

    except KeyboardInterrupt:
        salvar_json(ARQUIVO_CACHE, cache)
        print("\nInterrupção: cache salvo.")
        raise
    finally:
        salvar_json(ARQUIVO_CACHE, cache)

    print(f"[Open Library] novos matches por título: {encontrados:,}")


# ============================================================
# SAÍDA
# ============================================================

def construir_dataframe_api(
    books: pd.DataFrame,
    cache: dict[str, Any],
) -> pd.DataFrame:
    registros = []

    for _, row in books.iterrows():
        book_id = str(row["book_id"]).strip()
        dados = cache.get(book_id, {})

        registro = {
            "book_id": book_id,
            "openlibrary_status": (
                dados.get("status", "not_consulted")
                if isinstance(dados, dict)
                else "not_consulted"
            ),
            "openlibrary_match": (
                isinstance(dados, dict)
                and dados.get("status") == "matched"
            ),
        }

        if isinstance(dados, dict):
            registro.update(
                {
                    k: v
                    for k, v in dados.items()
                    if k.startswith("openlibrary_")
                }
            )

        registros.append(registro)

    return pd.DataFrame(registros)


def normalizar_parquet(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for coluna in df.columns:
        if coluna.startswith("openlibrary_"):
            if not pd.api.types.is_bool_dtype(df[coluna]):
                df[coluna] = df[coluna].map(
                    lambda x: None if pd.isna(x) else str(x)
                )

    for coluna in ["openlibrary_match"]:
        if coluna in df.columns:
            df[coluna] = df[coluna].fillna(False).astype(bool)

    return df


def salvar_saidas(
    books: pd.DataFrame,
    reviews: pd.DataFrame,
    cache: dict[str, Any],
) -> None:
    api_df = construir_dataframe_api(books, cache)

    books_out = books.copy()
    books_out["book_id"] = books_out["book_id"].astype(str).str.strip()
    books_out = books_out.merge(api_df, on="book_id", how="left")

    books_out = normalizar_parquet(books_out)
    books_out.to_parquet(ARQUIVO_BOOKS_SAIDA, index=False)

    reviews_out = reviews.copy()
    reviews_out["book_id"] = reviews_out["book_id"].astype(str).str.strip()
    reviews_out = reviews_out.merge(api_df, on="book_id", how="left")
    reviews_out = normalizar_parquet(reviews_out)
    reviews_out.to_parquet(ARQUIVO_REVIEWS_SAIDA, index=False)


def main() -> None:
    PASTA_PROCESSED.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("03 - GOODREADS + OPEN LIBRARY")
    print("=" * 70)

    if not ARQUIVO_BOOKS.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {ARQUIVO_BOOKS}")
    if not ARQUIVO_REVIEWS.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {ARQUIVO_REVIEWS}")

    books = pd.read_parquet(ARQUIVO_BOOKS)
    reviews = pd.read_parquet(ARQUIVO_REVIEWS)
    books["book_id"] = books["book_id"].astype(str).str.strip()

    cache = carregar_json(ARQUIVO_CACHE)
    migrar_cache_antigo(cache)

    # Primeiro ISBN, depois título.
    consultar_por_isbn(books, cache)
    consultar_por_titulo(books, cache)

    salvar_json(ARQUIVO_CACHE, cache)
    salvar_saidas(books, reviews, cache)

    matched = sum(
        1 for x in cache.values()
        if isinstance(x, dict) and x.get("status") == "matched"
    )
    no_match = sum(
        1 for x in cache.values()
        if isinstance(x, dict) and x.get("status") == "no_match"
    )

    print("\n" + "=" * 70)
    print("OPEN LIBRARY CONCLUÍDO")
    print("=" * 70)
    print(f"Livros Goodreads: {len(books):,}")
    print(f"Cache processado: {len(cache):,}")
    print(f"Matches: {matched:,}")
    print(f"Sem match: {no_match:,}")
    print(f"Cache: {ARQUIVO_CACHE}")
    print(f"Livros: {ARQUIVO_BOOKS_SAIDA}")
    print(f"Reviews: {ARQUIVO_REVIEWS_SAIDA}")


if __name__ == "__main__":
    main()

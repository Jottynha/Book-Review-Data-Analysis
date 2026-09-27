#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
04_cruzar_google_books.py

Segunda etapa do cruzamento externo otimizada:
    Goodreads -> Google Books API

Melhorias de Performance e Produção:
- Short-circuit: Se encontrar o livro por ISBN na 1ª requisição, encerra o livro sem gastar cota adicional.
- Multithreading: Processa múltiplos livros em paralelo usando ThreadPoolExecutor.
- Salvamento em Lote: Salva o cache em disco a cada N livros, eliminando a lentidão de escrita repetida.
- Suporta retomada por cache e tratamento robusto de erros/rate limit.
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import re
import time
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from threading import Lock
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

ARQUIVO_CACHE = PASTA_PROCESSED / "google_books_cache_batches.json"

ARQUIVO_BOOKS_SAIDA = (
    PASTA_PROCESSED / "goodreads_books_google_books_100k.parquet"
)
ARQUIVO_REVIEWS_SAIDA = (
    PASTA_PROCESSED / "goodreads_reviews_google_books_100k.parquet"
)

GOOGLE_URL = "https://www.googleapis.com/books/v1/volumes"

# Otimizações de alta produção:
BATCH_SIZE = 4000            # Quantidade total de livros pendentes para processar nesta rodada
MAX_WORKERS = 4              # Threads em paralelo (3 a 5 é o ideal para não estourar rate limit por segundo)
SAVE_INTERVAL = 50           # Salva no disco a cada N livros processados
GOOGLE_DELAY = 0.10          # Delay por thread para suavizar as requisições
GOOGLE_TIMEOUT = 15

MAX_RETRIES_429 = 5
BACKOFF_BASE = 2.0
BACKOFF_MAX = 120.0

HEADERS = {
    "User-Agent": (
        "Book-Review-Data-Analysis/2.0 "
        "(academic-data-science-project)"
    ),
    "Accept": "application/json",
}

# Lock para garantir atualização thread-safe do cache em memória
cache_lock = Lock()


# ============================================================
# .ENV
# ============================================================

def carregar_env_local() -> None:
    env = BASE_DIR / ".env"
    if not env.exists():
        return
    try:
        for linha in env.read_text(encoding="utf-8-sig").splitlines():
            linha = linha.strip()
            if not linha or linha.startswith("#") or "=" not in linha:
                continue
            chave, valor = linha.split("=", 1)
            chave = chave.strip()
            valor = valor.strip().strip("\"'")
            if chave and chave not in os.environ:
                os.environ[chave] = valor
    except OSError:
        pass


carregar_env_local()
GOOGLE_API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY", "").strip()


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
    return "" if texto.lower() in {"", "nan", "none", "null"} else texto


def isbn_limpo(valor: Any) -> str:
    return re.sub(r"[^0-9X]", "", limpar(valor).upper())


def normalizar_texto(valor: Any) -> str:
    texto = limpar(valor).lower()
    if not texto:
        return ""
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
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
    return texto


def juntar_lista(valor: Any) -> str:
    if isinstance(valor, list):
        return "|".join(limpar(x) for x in valor if limpar(x))
    return limpar(valor)


def titulo_busca(valor: Any) -> str:
    texto = limpar(valor)
    texto = re.sub(r"\s*\([^)]*\)", "", texto)
    return texto.strip()


def carregar_json(caminho: Path) -> dict[str, Any]:
    if not caminho.exists():
        return {}
    try:
        with caminho.open("r", encoding="utf-8") as f:
            dados = json.load(f)
        return dados if isinstance(dados, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def salvar_json(caminho: Path, dados: dict[str, Any]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    tmp = caminho.with_suffix(caminho.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    tmp.replace(caminho)


def get_retry(
    session: requests.Session,
    params: dict[str, Any],
):
    for tentativa in range(1, MAX_RETRIES_429 + 1):
        try:
            resposta = session.get(
                GOOGLE_URL,
                params=params,
                headers=HEADERS,
                timeout=GOOGLE_TIMEOUT,
            )
        except requests.RequestException:
            if tentativa == MAX_RETRIES_429:
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

        time.sleep(espera)

    return None


# ============================================================
# CACHE
# ============================================================

def normalizar_registro_google(registro: dict[str, Any]) -> dict[str, Any]:
    if registro.get("status"):
        return registro
    if registro.get("google_volume_id"):
        registro = dict(registro)
        registro.setdefault("status", "matched")
        registro.setdefault("source", "google_books")
        return registro
    return registro


def registro_e_google(registro: Any) -> bool:
    if not isinstance(registro, dict):
        return False
    source = limpar(registro.get("source")).lower()
    api_source = limpar(registro.get("api_source")).lower()
    search_method = limpar(registro.get("search_method")).lower()

    if "openlibrary" in source or "openlibrary" in api_source or "openlibrary" in search_method:
        return False

    return bool(
        registro.get("google_volume_id")
        or registro.get("google_title")
        or source == "google_books"
    )


def carregar_cache() -> dict[str, Any]:
    cache = carregar_json(ARQUIVO_CACHE)

    removidos = [
        str(book_id)
        for book_id, registro in cache.items()
        if not registro_e_google(registro)
    ]
    for book_id in removidos:
        del cache[book_id]

    if removidos:
        salvar_json(ARQUIVO_CACHE, cache)

    return cache


# ============================================================
# MATCH GOOGLE (LÓGICA OTIMIZADA)
# ============================================================

def extrair_isbns(info: dict[str, Any]) -> tuple[str, str]:
    isbn10 = ""
    isbn13 = ""
    ids = info.get("industryIdentifiers", [])

    if isinstance(ids, list):
        for identificador in ids:
            if not isinstance(identificador, dict):
                continue
            tipo = limpar(identificador.get("type"))
            valor = isbn_limpo(identificador.get("identifier"))
            if tipo == "ISBN_10" and valor:
                isbn10 = valor
            elif tipo == "ISBN_13" and valor:
                isbn13 = valor

    return isbn10, isbn13


def score_candidato(
    row: pd.Series,
    item: dict[str, Any],
    metodo: str,
    rank: int,
) -> tuple[float, dict[str, Any]]:
    info = item.get("volumeInfo", {})
    if not isinstance(info, dict):
        return -1.0, {}

    gr_titulo = limpar(row.get("title_without_series")) or limpar(row.get("title"))
    gr_autor = primeiro_autor(row.get("authors"))
    gr_isbn13 = isbn_limpo(row.get("isbn13"))
    gr_isbn10 = isbn_limpo(row.get("isbn"))
    gr_ano = limpar(row.get("publication_year"))

    gb_titulo = limpar(info.get("title"))
    gb_autores = info.get("authors", [])
    if not isinstance(gb_autores, list):
        gb_autores = []
    gb_isbn10, gb_isbn13 = extrair_isbns(info)
    gb_ano = limpar(info.get("publishedDate"))

    sim_titulo = similaridade(gr_titulo, gb_titulo)
    sim_autor = max(
        [similaridade(gr_autor, autor) for autor in gb_autores[:10]] or [0.0]
    )

    ano_diff = None
    if gr_ano and gb_ano:
        m1 = re.search(r"\b(1[5-9]\d{2}|20\d{2}|21\d{2})\b", gr_ano)
        m2 = re.search(r"\b(1[5-9]\d{2}|20\d{2}|21\d{2})\b", gb_ano)
        if m1 and m2:
            ano_diff = abs(int(m1.group(1)) - int(m2.group(1)))

    isbn_exato = (
        (bool(gr_isbn13) and gr_isbn13 == gb_isbn13)
        or (bool(gr_isbn10) and gr_isbn10 == gb_isbn10)
    )

    isbn_conflito = (
        (bool(gr_isbn13) and bool(gb_isbn13) and gr_isbn13 != gb_isbn13)
        or (bool(gr_isbn10) and bool(gb_isbn10) and gr_isbn10 != gb_isbn10)
    )

    score = 62 * sim_titulo + 28 * sim_autor

    if ano_diff is not None:
        if ano_diff == 0:
            score += 7
        elif ano_diff <= 3:
            score += 3
        elif ano_diff > 10:
            score -= 10

    if isbn_exato:
        score += 100
    elif isbn_conflito:
        score -= 40

    score += max(0, 1 - rank * 0.05)

    return score, {
        "title_similarity": round(sim_titulo, 4),
        "author_similarity": round(sim_autor, 4),
        "year_difference": ano_diff,
        "isbn_exact": isbn_exato,
        "isbn_conflict": isbn_conflito,
        "query_method": metodo,
    }


def extrair_google(
    row: pd.Series,
    item: dict[str, Any],
    sinais: dict[str, Any],
    queries: list[str],
    candidate_count: int,
    score: float,
) -> dict[str, Any]:
    info = item.get("volumeInfo", {})
    sale = item.get("saleInfo", {})
    access = item.get("accessInfo", {})
    if not isinstance(info, dict):
        info = {}
    if not isinstance(sale, dict):
        sale = {}
    if not isinstance(access, dict):
        access = {}

    images = info.get("imageLinks", {})
    if not isinstance(images, dict):
        images = {}

    isbn10, isbn13 = extrair_isbns(info)

    return {
        "status": "matched",
        "source": "google_books",
        "goodreads_book_id": str(row["book_id"]).strip(),
        "google_volume_id": limpar(item.get("id")),
        "google_title": limpar(info.get("title")),
        "google_subtitle": limpar(info.get("subtitle")),
        "google_authors": juntar_lista(info.get("authors", [])),
        "google_publisher": limpar(info.get("publisher")),
        "google_published_date": limpar(info.get("publishedDate")),
        "google_description": limpar(info.get("description")),
        "google_page_count": info.get("pageCount"),
        "google_categories": juntar_lista(info.get("categories", [])),
        "google_average_rating": info.get("averageRating"),
        "google_ratings_count": info.get("ratingsCount"),
        "google_language": limpar(info.get("language")),
        "google_isbn10": isbn10,
        "google_isbn13": isbn13,
        "google_maturity_rating": limpar(info.get("maturityRating")),
        "google_print_type": limpar(info.get("printType")),
        "google_thumbnail": limpar(images.get("thumbnail")),
        "google_small_thumbnail": limpar(images.get("smallThumbnail")),
        "google_preview_link": limpar(info.get("previewLink")),
        "google_info_link": limpar(info.get("infoLink")),
        "google_web_reader_link": limpar(access.get("webReaderLink")),
        "google_viewability": limpar(access.get("viewability")),
        "google_public_domain": bool(access.get("publicDomain", False)),
        "google_ebook_available": bool("epub" in access or "pdf" in access),
        "google_saleability": limpar(sale.get("saleability")),
        "google_search_method": sinais.get("query_method", ""),
        "google_queries_used": ";".join(queries),
        "google_candidate_count": candidate_count,
        "google_best_score": round(score, 2),
        "google_title_similarity": sinais.get("title_similarity"),
        "google_author_similarity": sinais.get("author_similarity"),
        "google_year_difference": sinais.get("year_difference"),
        "google_isbn_exact": sinais.get("isbn_exact", False),
        "google_isbn_conflict": sinais.get("isbn_conflict", False),
    }


def consultar_livro(
    session: requests.Session,
    row: pd.Series,
) -> dict[str, Any]:
    book_id = str(row["book_id"]).strip()

    isbn13 = isbn_limpo(row.get("isbn13"))
    isbn10 = isbn_limpo(row.get("isbn"))
    titulo = titulo_busca(limpar(row.get("title_without_series")) or limpar(row.get("title")))
    autor = primeiro_autor(row.get("authors"))

    queries_realizadas = []

    # ETAPA 1: Tenta match direto por ISBN (economiza cota: 1 única chamada)
    query_isbn = f"isbn:{isbn13}" if isbn13 else (f"isbn:{isbn10}" if isbn10 else None)
    if query_isbn:
        queries_realizadas.append(query_isbn)
        params = {
            "q": query_isbn,
            "maxResults": 3,
            "printType": "books",
            "projection": "full"
        }
        if GOOGLE_API_KEY:
            params["key"] = GOOGLE_API_KEY

        resposta = get_retry(session, params)
        if resposta is not None and resposta.status_code == 200:
            try:
                dados = resposta.json()
                itens = dados.get("items", [])
                if isinstance(itens, list) and len(itens) > 0:
                    item = itens[0]
                    score, sinais = score_candidato(row, item, "google_isbn", 0)
                    if sinais.get("isbn_exact"):
                        # RETORNO IMEDIATO: Economiza até 3 chamadas HTTP!
                        return extrair_google(
                            row, item, sinais, queries_realizadas, len(itens), score
                        )
            except ValueError:
                pass

    # ETAPA 2: Fallback para busca por Título + Autor se ISBN não bateu de primeira
    if titulo:
        q_text = f'intitle:"{titulo}"' + (f' inauthor:"{autor}"' if autor else "")
        queries_realizadas.append(q_text)
        params = {
            "q": q_text,
            "maxResults": 10,
            "printType": "books",
            "projection": "full",
            "orderBy": "relevance",
        }
        if GOOGLE_API_KEY:
            params["key"] = GOOGLE_API_KEY

        resposta = get_retry(session, params)
        if resposta is not None and resposta.status_code == 200:
            try:
                dados = resposta.json()
                itens = dados.get("items", [])
                if isinstance(itens, list) and len(itens) > 0:
                    candidatos = {}
                    for rank, item in enumerate(itens):
                        if not isinstance(item, dict) or not limpar(item.get("id")):
                            continue
                        score, sinais = score_candidato(row, item, "google_text", rank)
                        volume_id = limpar(item.get("id"))
                        candidatos[volume_id] = {
                            "item": item,
                            "score": score,
                            "sinais": sinais,
                        }

                    if candidatos:
                        melhor = max(candidatos.values(), key=lambda x: x["score"])
                        score = melhor["score"]
                        sinais = melhor["sinais"]
                        aceito = bool(sinais.get("isbn_exact")) or (
                            sinais.get("title_similarity", 0.0) >= 0.90
                            and sinais.get("author_similarity", 0.0) >= 0.70
                            and score >= 70
                            and not sinais.get("isbn_conflict", False)
                        )
                        if aceito:
                            return extrair_google(
                                row, melhor["item"], sinais, queries_realizadas, len(itens), score
                            )
            except ValueError:
                pass

    return {
        "status": "no_match",
        "source": "google_books",
        "goodreads_book_id": book_id,
        "google_queries_used": ";".join(queries_realizadas),
    }


# ============================================================
# SAÍDA
# ============================================================

def construir_api_df(books: pd.DataFrame, cache: dict[str, Any]) -> pd.DataFrame:
    registros = []
    for _, row in books.iterrows():
        book_id = str(row["book_id"]).strip()
        dados = cache.get(book_id)
        registro = {
            "book_id": book_id,
            "google_status": "not_processed" if dados is None else dados.get("status", "unknown"),
            "google_match": bool(
                isinstance(dados, dict)
                and dados.get("status") == "matched"
            ),
        }
        if isinstance(dados, dict):
            registro.update(
                {k: v for k, v in dados.items() if k.startswith("google_")}
            )
        registros.append(registro)
    return pd.DataFrame(registros)


def normalizar_parquet(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for coluna in df.columns:
        if coluna.startswith("google_"):
            if not pd.api.types.is_bool_dtype(df[coluna]):
                df[coluna] = df[coluna].map(
                    lambda x: None if pd.isna(x) else str(x)
                )
    if "google_match" in df.columns:
        df["google_match"] = df["google_match"].fillna(False).astype(bool)
    return df


def salvar_saidas(
    books: pd.DataFrame,
    reviews: pd.DataFrame,
    cache: dict[str, Any],
) -> None:
    api_df = construir_api_df(books, cache)

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


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    PASTA_PROCESSED.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("04 - GOODREADS + GOOGLE BOOKS API (MODO ALTA PRODUÇÃO)")
    print("=" * 70)

    if not GOOGLE_API_KEY:
        raise RuntimeError(
            "GOOGLE_BOOKS_API_KEY não encontrada.\n"
            "Defina no ambiente ou em .env antes de executar."
        )

    if not ARQUIVO_BOOKS.exists() or not ARQUIVO_REVIEWS.exists():
        raise FileNotFoundError("Arquivos Parquet de entrada não encontrados em /processed.")

    books = pd.read_parquet(ARQUIVO_BOOKS)
    reviews = pd.read_parquet(ARQUIVO_REVIEWS)
    books["book_id"] = books["book_id"].astype(str).str.strip()

    cache = carregar_cache()

    pendentes = [
        row
        for _, row in books.iterrows()
        if str(row["book_id"]).strip() not in cache
    ]

    lote = pendentes[:BATCH_SIZE]

    print(f"Total de livros Goodreads: {len(books):,}")
    print(f"Já processados no cache: {len(cache):,}")
    print(f"Ainda pendentes: {len(pendentes):,}")
    print(f"Lote desta execução: {len(lote):,}")

    if not lote:
        salvar_saidas(books, reviews, cache)
        print("\nTodos os livros já possuem registro no cache.")
        return

    encontrados = 0
    no_match = 0
    contador_salvamento = 0

    # Sessão HTTP reutilizável por thread
    session = requests.Session()

    def worker(row: pd.Series):
        nonlocal encontrados, no_match, contador_salvamento

        resultado = consultar_livro(session, row)
        book_id = str(row["book_id"]).strip()
        status = resultado.get("status")

        with cache_lock:
            cache[book_id] = resultado
            if status == "matched":
                encontrados += 1
            elif status == "no_match":
                no_match += 1

            contador_salvamento += 1
            # Salva em disco de forma fracionada (a cada N livros) para não estressar a I/O
            if contador_salvamento % SAVE_INTERVAL == 0:
                salvar_json(ARQUIVO_CACHE, cache)

        time.sleep(GOOGLE_DELAY)

    try:
        # Execução concorrente com multithreading
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            list(tqdm(executor.map(worker, lote), total=len(lote), desc="Google Books"))

    except KeyboardInterrupt:
        print("\nInterrupção detectada. Salvando cache...")
    finally:
        salvar_json(ARQUIVO_CACHE, cache)
        salvar_saidas(books, reviews, cache)

    matched_total = sum(
        1 for x in cache.values() if isinstance(x, dict) and x.get("status") == "matched"
    )

    print("\n" + "=" * 70)
    print("LOTE GOOGLE BOOKS CONCLUÍDO")
    print("=" * 70)
    print(f"Processados nesta execução: {encontrados + no_match:,}")
    print(f"Matches nesta execução: {encontrados:,}")
    print(f"Sem match nesta execução: {no_match:,}")
    print(f"Total acumulado no cache: {len(cache):,}")
    print(f"Total de matches no cache: {matched_total:,}")
    print(f"Próximos pendentes: {max(0, len(books) - len(cache)):,}")


if __name__ == "__main__":
    main()

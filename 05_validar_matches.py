"""
04_validar_matches.py

Audita os cruzamentos dos livros entre:
- Goodreads
- Open Library
- Google Books

A ideia NÃO é consultar APIs.
Este script trabalha somente com os dados/cache já coletados pelo 03.

Classificação:
- MUITO_ALTO
- ALTO
- MEDIO
- BAIXO
- REJEITAR
- SEM_MATCH

Saídas:
processed/goodreads_books_validated_100k.parquet
processed/goodreads_reviews_validated_100k.parquet
processed/matches_para_revisao.csv
processed/relatorio_qualidade_matches.json
processed/relatorio_qualidade_matches.txt
"""

from __future__ import annotations

import json
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from tqdm import tqdm
import pandas as pd


# ============================================================
# CONFIGURAÇÃO
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
PASTA_PROJETO = Path(
    __import__("os").getenv(
        "PASTA_PROJETO",
        str(BASE_DIR),
    )
)
PASTA_PROCESSED = PASTA_PROJETO / "processed"

ARQUIVO_BOOKS = (
    PASTA_PROCESSED
    / "goodreads_books_enriquecido_100k.parquet"
)

ARQUIVO_REVIEWS = (
    PASTA_PROCESSED
    / "goodreads_reviews_enriquecidas_100k.parquet"
)

ARQUIVO_BOOKS_SAIDA = (
    PASTA_PROCESSED
    / "goodreads_books_validated_100k.parquet"
)

ARQUIVO_REVIEWS_SAIDA = (
    PASTA_PROCESSED
    / "goodreads_reviews_validated_100k.parquet"
)

ARQUIVO_REVISAR = (
    PASTA_PROCESSED
    / "matches_para_revisao.csv"
)

ARQUIVO_RELATORIO_JSON = (
    PASTA_PROCESSED
    / "relatorio_qualidade_matches.json"
)

ARQUIVO_RELATORIO_TXT = (
    PASTA_PROCESSED
    / "relatorio_qualidade_matches.txt"
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
    except (TypeError, ValueError):
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
        if not unicodedata.combining(caractere)
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


def similaridade(a: Any, b: Any) -> float:
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


def primeiro_autor(valor: Any) -> str:
    texto = limpar(valor)

    if "|" in texto:
        return texto.split("|")[0].strip()

    return texto


def ano(valor: Any) -> int | None:
    texto = limpar(valor)

    if not texto:
        return None

    match = re.search(
        r"(19|20)\d{2}",
        texto,
    )

    if not match:
        return None

    try:
        return int(
            match.group(0)
        )
    except ValueError:
        return None


def conter_isbn(
    valor: Any,
    isbn: str,
) -> bool:
    if not isbn:
        return False

    texto = limpar(valor)

    partes = re.split(
        r"[|,;\s]+",
        texto,
    )

    return any(
        isbn_limpo(parte) == isbn
        for parte in partes
        if isbn_limpo(parte)
    )


def autores_têm_interseção(
    autores_a: Any,
    autores_b: Any,
) -> bool:
    a = {
        normalizar_texto(item)
        for item in re.split(
            r"[|;,]",
            limpar(autores_a),
        )
        if normalizar_texto(item)
    }

    b = {
        normalizar_texto(item)
        for item in re.split(
            r"[|;,]",
            limpar(autores_b),
        )
        if normalizar_texto(item)
    }

    if not a or not b:
        return False

    return bool(a & b)


# ============================================================
# VALIDAÇÃO DE UM LIVRO
# ============================================================

def validar_livro(
    row: pd.Series,
) -> dict[str, Any]:

    gr_title = (
        limpar(
            row.get("title_without_series")
        )
        or limpar(
            row.get("title")
        )
    )

    gr_isbn13 = isbn_limpo(
        row.get("isbn13")
    )

    gr_isbn10 = isbn_limpo(
        row.get("isbn")
    )

    gr_year = ano(
        row.get("publication_year")
    )

    ol_title = limpar(
        row.get("openlibrary_title")
    )

    ol_authors = limpar(
        row.get("openlibrary_authors")
    )

    ol_isbn13 = isbn_limpo(
        row.get("openlibrary_isbn13")
    )

    ol_isbn10 = isbn_limpo(
        row.get("openlibrary_isbn10")
    )

    ol_year = ano(
        row.get(
            "openlibrary_published_date"
        )
    )

    gb_title = limpar(
        row.get("google_title")
    )

    gb_authors = limpar(
        row.get("google_authors")
    )

    gb_isbn13 = isbn_limpo(
        row.get("google_isbn13")
    )

    gb_isbn10 = isbn_limpo(
        row.get("google_isbn10")
    )

    gb_year = ano(
        row.get("google_published_date")
    )

    title_sim_ol = similaridade(
        gr_title,
        ol_title,
    )

    title_sim_google = similaridade(
        gr_title,
        gb_title,
    )

    title_sim_cross = similaridade(
        ol_title,
        gb_title,
    )

    isbn13_ol = (
        bool(gr_isbn13)
        and gr_isbn13 == ol_isbn13
    )

    isbn13_google = (
        bool(gr_isbn13)
        and gr_isbn13 == gb_isbn13
    )

    isbn10_ol = (
        bool(gr_isbn10)
        and gr_isbn10 == ol_isbn10
    )

    isbn10_google = (
        bool(gr_isbn10)
        and gr_isbn10 == gb_isbn10
    )

    fontes_mesmo_isbn13 = (
        bool(ol_isbn13)
        and bool(gb_isbn13)
        and ol_isbn13 == gb_isbn13
    )

    autores_coincidem = (
        autores_têm_interseção(
            ol_authors,
            gb_authors,
        )
    )

    ano_ol_ok = (
        gr_year is not None
        and ol_year is not None
        and abs(gr_year - ol_year) <= 3
    )

    ano_google_ok = (
        gr_year is not None
        and gb_year is not None
        and abs(gr_year - gb_year) <= 3
    )

    tem_ol = bool(
        limpar(
            row.get("openlibrary_status")
        ) == "matched"
        or row.get("openlibrary_match")
        is True
    )

    tem_google = bool(
        limpar(
            row.get("google_status")
        ) == "matched"
        or row.get("google_match")
        is True
    )

    score = 0

    # Evidências fortes.
    if isbn13_ol:
        score += 50

    if isbn13_google:
        score += 50

    if isbn10_ol:
        score += 40

    if isbn10_google:
        score += 40

    if fontes_mesmo_isbn13:
        score += 20

    # Evidência textual.
    melhor_title = max(
        title_sim_ol,
        title_sim_google,
        title_sim_cross,
    )

    if melhor_title >= 0.97:
        score += 30
    elif melhor_title >= 0.92:
        score += 22
    elif melhor_title >= 0.85:
        score += 14
    elif melhor_title >= 0.75:
        score += 5
    else:
        score -= 25

    if autores_coincidem:
        score += 15

    if ano_ol_ok:
        score += 8

    if ano_google_ok:
        score += 8

    tem_evidencia_isbn = any(
        [
            isbn13_ol,
            isbn13_google,
            isbn10_ol,
            isbn10_google,
            fontes_mesmo_isbn13,
        ]
    )

    tem_evidencia_forte_textual = (
        melhor_title >= 0.92
        and (
            autores_coincidem
            or ano_ol_ok
            or ano_google_ok
        )
    )

    if not tem_ol and not tem_google:
        status = "SEM_MATCH"

    elif (
        tem_evidencia_isbn
        and (
            melhor_title >= 0.85
            or fontes_mesmo_isbn13
        )
    ):
        status = "MUITO_ALTO"

    elif (
        tem_evidencia_isbn
        or tem_evidencia_forte_textual
    ):
        status = "ALTO"

    elif melhor_title >= 0.90:
        status = "MEDIO"

    elif melhor_title >= 0.75:
        status = "BAIXO"

    else:
        status = "REJEITAR"

    # Conflito explícito entre as fontes.
    conflito_fontes = (
        bool(ol_title)
        and bool(gb_title)
        and title_sim_cross < 0.65
    )

    if conflito_fontes:
        status = "REJEITAR"

    return {
        "title_similarity_openlibrary": round(
            title_sim_ol,
            4,
        ),
        "title_similarity_google": round(
            title_sim_google,
            4,
        ),
        "title_similarity_between_apis": round(
            title_sim_cross,
            4,
        ),
        "isbn13_match_openlibrary": isbn13_ol,
        "isbn13_match_google": isbn13_google,
        "isbn10_match_openlibrary": isbn10_ol,
        "isbn10_match_google": isbn10_google,
        "same_isbn13_between_apis": fontes_mesmo_isbn13,
        "authors_match_between_apis": autores_coincidem,
        "year_match_openlibrary": ano_ol_ok,
        "year_match_google": ano_google_ok,
        "match_score": score,
        "match_status": status,
    }


# ============================================================
# RELATÓRIO
# ============================================================

def pct(
    numerador: int,
    denominador: int,
) -> float:
    if denominador == 0:
        return 0.0

    return round(
        numerador / denominador * 100,
        2,
    )


def gerar_relatorio(
    livros: pd.DataFrame,
    reviews: pd.DataFrame,
) -> dict[str, Any]:

    total_livros = len(livros)
    total_reviews = len(reviews)

    status_counts = (
        livros[
            "match_status"
        ]
        .value_counts()
        .to_dict()
    )

    google_matches = int(
        livros[
            "google_match"
        ]
        .fillna(False)
        .astype(bool)
        .sum()
    )

    ol_matches = int(
        livros[
            "openlibrary_match"
        ]
        .fillna(False)
        .astype(bool)
        .sum()
    )

    ambos = int(
        (
            livros[
                "google_match"
            ]
            .fillna(False)
            .astype(bool)
            &
            livros[
                "openlibrary_match"
            ]
            .fillna(False)
            .astype(bool)
        ).sum()
    )

    nenhum = int(
        (
            ~livros[
                "google_match"
            ]
            .fillna(False)
            .astype(bool)
            &
            ~livros[
                "openlibrary_match"
            ]
            .fillna(False)
            .astype(bool)
        ).sum()
    )

    return {
        "total_livros": total_livros,
        "total_reviews": total_reviews,

        "google_books": {
            "matches": google_matches,
            "cobertura_percentual": pct(
                google_matches,
                total_livros,
            ),
        },

        "open_library": {
            "matches": ol_matches,
            "cobertura_percentual": pct(
                ol_matches,
                total_livros,
            ),
        },

        "fontes": {
            "ambas": ambos,
            "nenhuma": nenhum,
            "somente_google": (
                google_matches - ambos
            ),
            "somente_openlibrary": (
                ol_matches - ambos
            ),
        },

        "qualidade_matches": {
            "muito_alto": status_counts.get(
                "MUITO_ALTO",
                0,
            ),
            "alto": status_counts.get(
                "ALTO",
                0,
            ),
            "medio": status_counts.get(
                "MEDIO",
                0,
            ),
            "baixo": status_counts.get(
                "BAIXO",
                0,
            ),
            "rejeitar": status_counts.get(
                "REJEITAR",
                0,
            ),
            "sem_match": status_counts.get(
                "SEM_MATCH",
                0,
            ),
        },

        "livros_prontos_para_eda": int(
            livros[
                "match_status"
            ].isin(
                [
                    "MUITO_ALTO",
                    "ALTO",
                    "MEDIO",
                ]
            ).sum()
        ),
    }


def salvar_relatorio_txt(
    caminho: Path,
    relatorio: dict[str, Any],
) -> None:

    linhas = [
        "=" * 70,
        "RELATÓRIO DE QUALIDADE DOS MATCHES",
        "=" * 70,
        "",
        (
            f"Livros: "
            f"{relatorio['total_livros']:,}"
        ),
        (
            f"Reviews: "
            f"{relatorio['total_reviews']:,}"
        ),
        "",
        "COBERTURA DAS FONTES",
        "-" * 70,
        (
            f"Google Books: "
            f"{relatorio['google_books']['matches']:,} "
            f"({relatorio['google_books']['cobertura_percentual']:.2f}%)"
        ),
        (
            f"Open Library: "
            f"{relatorio['open_library']['matches']:,} "
            f"({relatorio['open_library']['cobertura_percentual']:.2f}%)"
        ),
        "",
        "INTERSEÇÃO DAS FONTES",
        "-" * 70,
        (
            f"Ambas: "
            f"{relatorio['fontes']['ambas']:,}"
        ),
        (
            f"Somente Google: "
            f"{relatorio['fontes']['somente_google']:,}"
        ),
        (
            f"Somente Open Library: "
            f"{relatorio['fontes']['somente_openlibrary']:,}"
        ),
        (
            f"Nenhuma: "
            f"{relatorio['fontes']['nenhuma']:,}"
        ),
        "",
        "QUALIDADE DOS MATCHES",
        "-" * 70,
    ]

    qualidade = (
        relatorio["qualidade_matches"]
    )

    for chave, valor in qualidade.items():
        linhas.append(
            f"{chave}: {valor:,}"
        )

    linhas.extend(
        [
            "",
            (
                "Livros considerados adequados "
                "para EDA: "
                f"{relatorio['livros_prontos_para_eda']:,}"
            ),
            "",
            "=" * 70,
        ]
    )

    caminho.write_text(
        "\n".join(linhas),
        encoding="utf-8",
    )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    print("=" * 70)
    print(
        "04 - VALIDAÇÃO DOS MATCHES"
    )
    print("=" * 70)

    if not ARQUIVO_BOOKS.exists():
        raise FileNotFoundError(
            f"Execute primeiro o 03_cruzar_api.py.\n"
            f"Arquivo ausente: {ARQUIVO_BOOKS}"
        )

    if not ARQUIVO_REVIEWS.exists():
        raise FileNotFoundError(
            f"Execute primeiro o 03_cruzar_api.py.\n"
            f"Arquivo ausente: {ARQUIVO_REVIEWS}"
        )

    livros = pd.read_parquet(
        ARQUIVO_BOOKS
    )

    reviews = pd.read_parquet(
        ARQUIVO_REVIEWS
    )

    print(
        f"Livros carregados: "
        f"{len(livros):,}"
    )

    print(
        f"Reviews carregadas: "
        f"{len(reviews):,}"
    )

    resultados = []

    for _, row in tqdm(
        livros.iterrows(),
        total=len(livros),
        desc="Validando matches",
    ):
        resultado = validar_livro(
            row
        )

        resultados.append(
            resultado
        )

    validacao = pd.DataFrame(
        resultados
    )

    livros_final = pd.concat(
        [
            livros.reset_index(drop=True),
            validacao.reset_index(drop=True),
        ],
        axis=1,
    )

    # --------------------------------------------------------
    # Arquivo para inspeção manual
    # --------------------------------------------------------

    suspeitos = livros_final[
        livros_final[
            "match_status"
        ].isin(
            [
                "BAIXO",
                "REJEITAR",
            ]
        )
    ].copy()

    colunas_revisao = [
        "book_id",
        "title",
        "title_without_series",
        "isbn",
        "isbn13",

        "openlibrary_title",
        "openlibrary_authors",
        "openlibrary_isbn10",
        "openlibrary_isbn13",

        "google_title",
        "google_authors",
        "google_isbn10",
        "google_isbn13",

        "title_similarity_openlibrary",
        "title_similarity_google",
        "title_similarity_between_apis",

        "isbn13_match_openlibrary",
        "isbn13_match_google",
        "same_isbn13_between_apis",

        "match_score",
        "match_status",
    ]

    colunas_revisao = [
        coluna
        for coluna in colunas_revisao
        if coluna in suspeitos.columns
    ]

    suspeitos[
        colunas_revisao
    ].to_csv(
        ARQUIVO_REVISAR,
        index=False,
        encoding="utf-8-sig",
    )

    # --------------------------------------------------------
    # Salvar livros
    # --------------------------------------------------------

    livros_final.to_parquet(
        ARQUIVO_BOOKS_SAIDA,
        index=False,
    )

    # --------------------------------------------------------
    # Salvar reviews
    # --------------------------------------------------------

    reviews_final = reviews.copy()

    reviews_final[
        "match_status"
    ] = reviews_final[
        "book_id"
    ].astype(str).map(
        livros_final.set_index(
            "book_id"
        )[
            "match_status"
        ].to_dict()
    )

    reviews_final[
        "final_match_confidence"
    ] = reviews_final[
        "match_status"
    ].map(
        {
            "MUITO_ALTO": "muito_alta",
            "ALTO": "alta",
            "MEDIO": "media",
            "BAIXO": "baixa",
            "REJEITAR": "rejeitar",
            "SEM_MATCH": "sem_match",
        }
    )

    reviews_final.to_parquet(
        ARQUIVO_REVIEWS_SAIDA,
        index=False,
    )

    # --------------------------------------------------------
    # Relatório
    # --------------------------------------------------------

    relatorio = gerar_relatorio(
        livros_final,
        reviews_final,
    )

    with ARQUIVO_RELATORIO_JSON.open(
        "w",
        encoding="utf-8",
    ) as arquivo:
        json.dump(
            relatorio,
            arquivo,
            ensure_ascii=False,
            indent=2,
        )

    salvar_relatorio_txt(
        ARQUIVO_RELATORIO_TXT,
        relatorio,
    )

    # --------------------------------------------------------
    # Terminal
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("VALIDAÇÃO CONCLUÍDA")
    print("=" * 70)

    qualidade = (
        relatorio["qualidade_matches"]
    )

    for chave, valor in qualidade.items():
        print(
            f"{chave:20s}: {valor:,}"
        )

    print(
        "\nLivros adequados para EDA: "
        f"{relatorio['livros_prontos_para_eda']:,}"
    )

    print(
        f"\nRevisão manual:\n"
        f"{ARQUIVO_REVISAR}"
    )

    print(
        f"\nRelatório:\n"
        f"{ARQUIVO_RELATORIO_TXT}"
    )

    print(
        f"\nBase final:\n"
        f"{ARQUIVO_REVIEWS_SAIDA}"
    )


if __name__ == "__main__":
    main()

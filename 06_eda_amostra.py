#!/usr/bin/env python3
"""Gera uma EDA inicial da amostra validada de reviews e livros."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
PASTA_PROCESSED = BASE_DIR / "processed"
ARQUIVO_BOOKS = PASTA_PROCESSED / "goodreads_books_validated_100k.parquet"
ARQUIVO_REVIEWS = PASTA_PROCESSED / "goodreads_reviews_validated_100k.parquet"
PASTA_SAIDA = PASTA_PROCESSED / "eda_amostra"

STATUS_CONFIAVEIS = ["MUITO_ALTO", "ALTO", "MEDIO"]
SHELVES_NAO_GENERO = {
    "to-read",
    "currently-reading",
    "owned",
    "books-i-own",
    "favorites",
    "ebook",
    "ebooks",
    "kindle",
    "audiobook",
    "audio",
    "library",
    "books-about-books",
    "my-books",
    "read-in-2014",
    "read-in-2015",
    "read-in-2016",
    "read-in-2017",
    "read-in-2018",
    "read-in-2019",
    "read-in-2020",
    "read-in-2021",
    "read-in-2022",
    "read-in-2023",
    "read-in-2024",
    "read-in-2025",
    "read-in-2026",
}


def argumentos() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera tabelas, relatório e gráficos da EDA da amostra."
    )
    parser.add_argument(
        "--only-confident",
        action="store_true",
        help="Usa somente reviews com match_status MUITO_ALTO, ALTO ou MEDIO.",
    )
    return parser.parse_args()


def numero(serie: pd.Series) -> pd.Series:
    return pd.to_numeric(serie, errors="coerce")


def proporcao(valor: int, total: int) -> float:
    return round(valor / total * 100, 2) if total else 0.0


def preparar_reviews(reviews: pd.DataFrame, only_confident: bool) -> pd.DataFrame:
    reviews = reviews.copy()
    if only_confident:
        reviews = reviews[reviews["match_status"].isin(STATUS_CONFIAVEIS)].copy()

    reviews["rating"] = numero(reviews["rating"])
    reviews["n_votes"] = numero(reviews["n_votes"]).fillna(0)
    reviews["n_comments"] = numero(reviews["n_comments"]).fillna(0)
    reviews["review_text"] = reviews["review_text"].fillna("").astype(str)
    reviews["review_chars"] = reviews["review_text"].str.len()
    reviews["review_words"] = reviews["review_text"].str.split().str.len()
    reviews["date_added"] = pd.to_datetime(
        reviews["date_added"], errors="coerce", format="mixed", utc=True
    )
    reviews["review_year"] = reviews["date_added"].dt.year
    return reviews


def preparar_books(books: pd.DataFrame) -> pd.DataFrame:
    books = books.copy()
    for column in [
        "publication_year",
        "num_pages",
        "average_rating",
        "ratings_count",
        "text_reviews_count",
    ]:
        books[column] = numero(books[column])
    books["is_ebook"] = books["is_ebook"].astype(str).str.lower().eq("true")
    return books


def salvar_csv(dataframe: pd.DataFrame, nome: str) -> None:
    dataframe.to_csv(PASTA_SAIDA / nome, index=False, encoding="utf-8")


def contar_shelves(books: pd.DataFrame) -> pd.DataFrame:
    shelves = (
        books[["book_id", "popular_shelves"]]
        .assign(popular_shelves=lambda df: df["popular_shelves"].fillna(""))
        .assign(popular_shelves=lambda df: df["popular_shelves"].str.split("|"))
        .explode("popular_shelves")
        .rename(columns={"popular_shelves": "shelf"})
    )
    shelves["shelf"] = shelves["shelf"].fillna("").astype(str).str.strip()
    shelves = shelves[shelves["shelf"].ne("")]
    shelves = shelves.drop_duplicates(["book_id", "shelf"])
    shelves["is_genre_proxy"] = ~shelves["shelf"].str.lower().isin(SHELVES_NAO_GENERO)
    return shelves


def escrever_grafico(figura: plt.Figure, nome: str) -> None:
    figura.tight_layout()
    figura.savefig(PASTA_SAIDA / nome, dpi=150, bbox_inches="tight")
    plt.close(figura)


def gerar_graficos(reviews: pd.DataFrame, books: pd.DataFrame) -> None:
    figura, eixo = plt.subplots(figsize=(8, 5))
    reviews["rating"].value_counts().sort_index().plot.bar(ax=eixo, color="#2f6f8f")
    eixo.set_title("Distribuição das notas")
    eixo.set_xlabel("Nota")
    eixo.set_ylabel("Número de reviews")
    escrever_grafico(figura, "01_distribuicao_notas.png")

    figura, eixos = plt.subplots(1, 2, figsize=(12, 5))
    reviews["review_chars"].clip(upper=reviews["review_chars"].quantile(0.99)).plot.hist(
        bins=40, ax=eixos[0], color="#d97941"
    )
    eixos[0].set_title("Tamanho das reviews (até P99)")
    eixos[0].set_xlabel("Caracteres")
    eixos[0].set_ylabel("Número de reviews")
    reviews["n_votes"].clip(upper=reviews["n_votes"].quantile(0.99)).plot.hist(
        bins=30, ax=eixos[1], color="#597a55"
    )
    eixos[1].set_title("Votos de utilidade (até P99)")
    eixos[1].set_xlabel("Votos")
    eixos[1].set_ylabel("Número de reviews")
    escrever_grafico(figura, "02_texto_e_votos.png")

    top_books = reviews["book_id"].value_counts().head(15).sort_values()
    figura, eixo = plt.subplots(figsize=(9, 6))
    rotulos = top_books.index.astype(str)
    top_books.index = rotulos
    top_books.plot.barh(ax=eixo, color="#6d597a")
    eixo.set_title("Livros com mais reviews na amostra")
    eixo.set_xlabel("Número de reviews")
    eixo.set_ylabel("book_id")
    escrever_grafico(figura, "03_livros_mais_resenhados.png")

    rating_books = (
        reviews.groupby("book_id", as_index=False)
        .agg(review_count=("review_id", "count"), mean_rating=("rating", "mean"))
        .query("review_count >= 2")
    )
    figura, eixo = plt.subplots(figsize=(8, 6))
    eixo.scatter(
        rating_books["review_count"],
        rating_books["mean_rating"],
        alpha=0.45,
        s=18,
        color="#355070",
    )
    eixo.set_xscale("log")
    eixo.set_title("Volume de reviews e nota média por livro")
    eixo.set_xlabel("Reviews por livro (escala log)")
    eixo.set_ylabel("Nota média")
    escrever_grafico(figura, "04_volume_e_nota_por_livro.png")

    language = books["language_code"].replace("", pd.NA).dropna().value_counts().head(12)
    figura, eixo = plt.subplots(figsize=(9, 5))
    language.sort_values().plot.barh(ax=eixo, color="#457b9d")
    eixo.set_title("Idiomas mais frequentes dos livros")
    eixo.set_xlabel("Número de livros")
    eixo.set_ylabel("language_code")
    escrever_grafico(figura, "05_idiomas.png")


def gerar_relatorio(
    reviews: pd.DataFrame,
    books: pd.DataFrame,
    shelves: pd.DataFrame,
    only_confident: bool,
) -> None:
    total_reviews = len(reviews)
    reviews_por_livro = reviews.groupby("book_id").size()
    reviews_por_usuario = reviews.groupby("user_id").size()
    genre_shelves = shelves[shelves["is_genre_proxy"]]
    top_genres = genre_shelves["shelf"].value_counts().head(20)

    linhas = [
        "# EDA inicial da amostra",
        "",
        f"Filtro aplicado: {'somente matches confiáveis' if only_confident else 'todas as reviews da amostra' }.",
        "",
        "## Perfil da base",
        "",
        f"- Reviews analisadas: {total_reviews:,}",
        f"- Livros únicos analisados: {reviews['book_id'].nunique():,}",
        f"- Usuários únicos: {reviews['user_id'].nunique():,}",
        f"- Média de reviews por livro: {reviews_por_livro.mean():.2f}",
        f"- Mediana de reviews por livro: {reviews_por_livro.median():.0f}",
        f"- Média de reviews por usuário: {reviews_por_usuario.mean():.2f}",
        f"- Mediana de reviews por usuário: {reviews_por_usuario.median():.0f}",
        "",
        "## Notas e comportamento",
        "",
        f"- Nota média: {reviews['rating'].mean():.2f}",
        f"- Mediana da nota: {reviews['rating'].median():.1f}",
        f"- Percentual sem nota (`rating == 0`): {proporcao(int(reviews['rating'].eq(0).sum()), total_reviews):.2f}%",
        f"- Média de caracteres por review: {reviews['review_chars'].mean():.1f}",
        f"- Mediana de caracteres por review: {reviews['review_chars'].median():.0f}",
        f"- Média de votos: {reviews['n_votes'].mean():.2f}",
        f"- Média de comentários: {reviews['n_comments'].mean():.2f}",
        "",
        "## Livros e metadados",
        "",
        f"- Livros com ano de publicação: {proporcao(int(books['publication_year'].notna().sum()), len(books)):.2f}%",
        f"- Livros com número de páginas: {proporcao(int(books['num_pages'].notna().sum()), len(books)):.2f}%",
        f"- Livros marcados como ebook: {proporcao(int(books['is_ebook'].sum()), len(books)):.2f}%",
        f"- Número mediano de páginas: {books['num_pages'].median():.0f}",
        f"- Nota média Goodreads dos livros: {books['average_rating'].mean():.2f}",
        "",
        "## Gêneros e shelves",
        "",
        "As `popular_shelves` são usadas como proxy exploratório de gênero. Elas são",
        "rótulos sociais dos usuários, podem se sobrepor e também incluem status de",
        "leitura; por isso não devem ser interpretadas como uma classificação editorial",
        "exclusiva. Os status e formatos mais óbvios foram removidos da tabela de proxy.",
        "",
        "## Artefatos",
        "",
        "- `resumo_geral.csv`: indicadores gerais da amostra.",
        "- `distribuicao_notas.csv`: frequência e percentual de cada nota.",
        "- `top_usuarios.csv`: usuários com mais reviews na amostra.",
        "- `top_livros.csv`: livros com mais reviews e nota média.",
        "- `top_shelves.csv` e `top_generos_proxy.csv`: rótulos mais frequentes.",
        "- `01_` a `05_*.png`: gráficos para inspeção inicial.",
        "",
        "## Perguntas para a próxima análise",
        "",
        "- A nota está associada ao tamanho da review ou aos votos recebidos?",
        "- A distribuição de notas muda entre livros com diferentes volumes de reviews?",
        "- Usuários mais ativos avaliam de forma diferente dos usuários com uma única review?",
        "- Quais gêneros/shelves concentram mais reviews e quais têm maior nota média?",
    ]
    (PASTA_SAIDA / "relatorio_eda.md").write_text("\n".join(linhas) + "\n", encoding="utf-8")
    salvar_csv(pd.DataFrame([{
        "reviews": total_reviews,
        "livros_unicos": reviews["book_id"].nunique(),
        "usuarios_unicos": reviews["user_id"].nunique(),
        "nota_media": round(reviews["rating"].mean(), 4),
        "mediana_caracteres_review": reviews["review_chars"].median(),
        "media_votos": round(reviews["n_votes"].mean(), 4),
        "media_comentarios": round(reviews["n_comments"].mean(), 4),
    }]), "resumo_geral.csv")
    salvar_csv(
        reviews["rating"].value_counts().sort_index().rename("quantidade").rename_axis("rating").reset_index().assign(
            percentual=lambda df: (df["quantidade"] / total_reviews * 100).round(2)
        ),
        "distribuicao_notas.csv",
    )
    salvar_csv(
        reviews["user_id"].value_counts().head(30).rename("reviews").rename_axis("user_id").reset_index(),
        "top_usuarios.csv",
    )
    top_books = reviews.groupby("book_id").agg(
        reviews=("review_id", "count"), nota_media=("rating", "mean")
    ).sort_values(["reviews", "nota_media"], ascending=False).head(30).reset_index()
    salvar_csv(top_books, "top_livros.csv")
    salvar_csv(
        shelves["shelf"].value_counts().head(50).rename("livros").rename_axis("shelf").reset_index(),
        "top_shelves.csv",
    )
    salvar_csv(
        genre_shelves["shelf"].value_counts().head(50).rename("livros").rename_axis("shelf").reset_index(),
        "top_generos_proxy.csv",
    )
    genre_review_counts = (
        reviews[["book_id"]].merge(genre_shelves[["book_id", "shelf"]], on="book_id", how="inner")
        .groupby("shelf")
        .size()
        .sort_values(ascending=False)
        .head(50)
        .rename("reviews")
        .rename_axis("shelf")
        .reset_index()
    )
    salvar_csv(genre_review_counts, "top_generos_proxy_por_reviews.csv")


def main() -> None:
    global PASTA_SAIDA

    args = argumentos()
    if not ARQUIVO_BOOKS.exists() or not ARQUIVO_REVIEWS.exists():
        raise FileNotFoundError(
            "Execute as etapas 01 a 05 antes da EDA para gerar os Parquets validados."
        )
    nome_saida = "eda_amostra_confiante" if args.only_confident else "eda_amostra"
    PASTA_SAIDA = PASTA_PROCESSED / nome_saida
    PASTA_SAIDA.mkdir(parents=True, exist_ok=True)
    books = preparar_books(pd.read_parquet(ARQUIVO_BOOKS))
    reviews = preparar_reviews(pd.read_parquet(ARQUIVO_REVIEWS), args.only_confident)
    shelves = contar_shelves(books)
    gerar_relatorio(reviews, books, shelves, args.only_confident)
    gerar_graficos(reviews, books)
    print(f"EDA concluída: {PASTA_SAIDA}")
    print(f"Reviews analisadas: {len(reviews):,}")
    print(f"Livros analisados: {reviews['book_id'].nunique():,}")


if __name__ == "__main__":
    main()

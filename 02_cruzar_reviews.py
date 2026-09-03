import os
import pandas as pd


# ============================================================
# CONFIGURAÇÃO
# ============================================================

PASTA_PROJETO = (
    "/home/joao/Projetos/Book-Review-Data-Analysis"
)

PASTA_PROCESSED = os.path.join(
    PASTA_PROJETO,
    "processed"
)


# ------------------------------------------------------------
# Arquivo de reviews
# ------------------------------------------------------------

ARQUIVO_REVIEWS = os.path.join(
    PASTA_PROCESSED,
    "goodreads_reviews_100k.parquet"
)


# ------------------------------------------------------------
# Arquivo de livros
# ------------------------------------------------------------

ARQUIVO_BOOKS = os.path.join(
    PASTA_PROCESSED,
    "goodreads_books_100k.parquet"
)


# ------------------------------------------------------------
# Arquivo final
# ------------------------------------------------------------

ARQUIVO_SAIDA = os.path.join(
    PASTA_PROCESSED,
    "goodreads_reviews_with_books_100k.parquet"
)


# ============================================================
# VERIFICAÇÃO DOS ARQUIVOS
# ============================================================

print("=" * 70)
print("GOODREADS — CRUZAMENTO REVIEWS + BOOKS")
print("=" * 70)

print()

if not os.path.exists(ARQUIVO_REVIEWS):

    raise FileNotFoundError(
        f"Arquivo de reviews não encontrado:\n"
        f"{ARQUIVO_REVIEWS}"
    )


if not os.path.exists(ARQUIVO_BOOKS):

    raise FileNotFoundError(
        f"Arquivo de livros não encontrado:\n"
        f"{ARQUIVO_BOOKS}"
    )


# ============================================================
# CARREGAR REVIEWS
# ============================================================

print("Carregando reviews...")

df_reviews = pd.read_parquet(
    ARQUIVO_REVIEWS
)

print(
    f"Reviews carregados: "
    f"{len(df_reviews):,}"
)


# ============================================================
# CARREGAR BOOKS
# ============================================================

print()
print("Carregando livros...")

df_books = pd.read_parquet(
    ARQUIVO_BOOKS
)

print(
    f"Livros carregados: "
    f"{len(df_books):,}"
)


# ============================================================
# PADRONIZAR BOOK_ID
# ============================================================

print()
print("Padronizando book_id...")

df_reviews["book_id"] = (
    df_reviews["book_id"]
    .astype(str)
    .str.strip()
)

df_books["book_id"] = (
    df_books["book_id"]
    .astype(str)
    .str.strip()
)


# ============================================================
# VERIFICAR DUPLICATAS
# ============================================================

duplicados_reviews = (
    df_reviews["book_id"]
    .duplicated()
    .sum()
)

duplicados_books = (
    df_books["book_id"]
    .duplicated()
    .sum()
)

print()
print(
    f"Duplicatas de book_id nos reviews: "
    f"{duplicados_reviews:,}"
)

print(
    f"Duplicatas de book_id nos books: "
    f"{duplicados_books:,}"
)


# ============================================================
# CRUZAMENTO
# ============================================================

print()
print("=" * 70)
print("REALIZANDO LEFT JOIN")
print("=" * 70)

print()

df_final = df_reviews.merge(
    df_books,
    on="book_id",
    how="left",
    suffixes=(
        "",
        "_book"
    )
)


# ============================================================
# ESTATÍSTICAS DO CRUZAMENTO
# ============================================================

total_reviews = len(
    df_final
)

reviews_com_livro = (
    df_final["title"]
    .notna()
    .sum()
)

reviews_sem_livro = (
    total_reviews -
    reviews_com_livro
)


if total_reviews > 0:

    percentual = (
        reviews_com_livro /
        total_reviews
    ) * 100

else:

    percentual = 0


print()
print("=" * 70)
print("RESULTADO DO CRUZAMENTO")
print("=" * 70)

print()

print(
    f"Total de reviews: "
    f"{total_reviews:,}"
)

print(
    f"Reviews com informações do livro: "
    f"{reviews_com_livro:,}"
)

print(
    f"Reviews sem informações do livro: "
    f"{reviews_sem_livro:,}"
)

print(
    f"Taxa de correspondência: "
    f"{percentual:.2f}%"
)


# ============================================================
# SALVAR
# ============================================================

print()
print("=" * 70)
print("SALVANDO DATASET FINAL")
print("=" * 70)

df_final.to_parquet(
    ARQUIVO_SAIDA,
    index=False
)

print()

print(
    f"Arquivo salvo em:\n"
    f"{ARQUIVO_SAIDA}"
)

print()

print(
    f"Linhas: "
    f"{len(df_final):,}"
)

print(
    f"Colunas: "
    f"{len(df_final.columns):,}"
)


# ============================================================
# AMOSTRA
# ============================================================

print()
print("=" * 70)
print("AMOSTRA DO DATASET FINAL")
print("=" * 70)

colunas_interesse = [
    "user_id",
    "book_id",
    "rating",
    "review_text",
    "title",
    "author_ids",
    "publication_year",
    "num_pages",
    "publisher",
    "language_code",
    "average_rating",
    "ratings_count",
    "popular_shelves"
]


colunas_existentes = [
    coluna
    for coluna in colunas_interesse
    if coluna in df_final.columns
]


print(
    df_final[
        colunas_existentes
    ]
    .head(10)
    .to_string(index=False)
)


# ============================================================
# FINAL
# ============================================================

print()
print("=" * 70)
print("PROCESSO CONCLUÍDO")
print("=" * 70)

print()

print(
    "Dataset final pronto para análise:"
)

print(
    ARQUIVO_SAIDA
)

print()


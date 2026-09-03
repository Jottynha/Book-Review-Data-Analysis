import gzip
import json
import random
import os
import pandas as pd


# ============================================================
# CONFIGURAÇÃO
# ============================================================

# Arquivo original dos reviews
ARQUIVO_REVIEWS = (
    "/media/joao/8cb5cf8c-261d-4ef9-965c-485a78e63f21/kaggle/"
    "goodreads_reviews_dedup.json.gz"
)

# Arquivo original dos livros
ARQUIVO_BOOKS = (
    "/media/joao/8cb5cf8c-261d-4ef9-965c-485a78e63f21/kaggle/"
    "goodreads_books.json.gz"
)

# Pasta do projeto
PASTA_PROJETO = (
    "/home/joao/Projetos/Book-Review-Data-Analysis"
)

PASTA_PROCESSED = os.path.join(
    PASTA_PROJETO,
    "processed"
)

# Saída dos reviews
ARQUIVO_REVIEWS_SAIDA = os.path.join(
    PASTA_PROCESSED,
    "goodreads_reviews_100k.parquet"
)

# Saída dos livros
ARQUIVO_BOOKS_SAIDA = os.path.join(
    PASTA_PROCESSED,
    "goodreads_books_100k.parquet"
)

# Quantidade de reviews desejada
TAMANHO_AMOSTRA = 10_000

# Seed para tornar o resultado reproduzível
SEED = 42


# ============================================================
# PREPARAÇÃO
# ============================================================

os.makedirs(
    PASTA_PROCESSED,
    exist_ok=True
)

random.seed(SEED)


# ============================================================
# ETAPA 1 — AMOSTRAGEM DOS REVIEWS
# ============================================================

print("=" * 70)
print("ETAPA 1 — AMOSTRAGEM DOS REVIEWS")
print("=" * 70)

print()
print(f"Arquivo:")
print(ARQUIVO_REVIEWS)

print()
print(
    f"Quantidade desejada: "
    f"{TAMANHO_AMOSTRA:,}"
)

print()
print(
    "Iniciando leitura do dataset de reviews..."
)

print(
    "Isso pode levar alguns minutos.\n"
)


reviews = []


with gzip.open(
    ARQUIVO_REVIEWS,
    "rt",
    encoding="utf-8"
) as arquivo:

    for i, linha in enumerate(arquivo):

        try:
            review = json.loads(linha)

        except json.JSONDecodeError:
            print(
                f"Erro ao ler a linha {i}. "
                "Pulando..."
            )
            continue

        # ----------------------------------------------------
        # Reservoir Sampling
        # ----------------------------------------------------

        if len(reviews) < TAMANHO_AMOSTRA:

            reviews.append(review)

        else:

            indice = random.randint(
                0,
                i
            )

            if indice < TAMANHO_AMOSTRA:

                reviews[indice] = review

        # ----------------------------------------------------
        # Progresso
        # ----------------------------------------------------

        if (i + 1) % 1_000_000 == 0:

            print(
                f"{i + 1:,} reviews processadas..."
            )


# ============================================================
# DATAFRAME DOS REVIEWS
# ============================================================

print()
print(
    "Convertendo amostra de reviews "
    "para DataFrame..."
)

df_reviews = pd.DataFrame(
    reviews
)


# ============================================================
# SALVAR REVIEWS
# ============================================================

print()
print("Salvando reviews...")

df_reviews.to_parquet(
    ARQUIVO_REVIEWS_SAIDA,
    index=False
)

print()
print(
    f"Reviews salvos em:\n"
    f"{ARQUIVO_REVIEWS_SAIDA}"
)


# ============================================================
# ETAPA 2 — EXTRAIR BOOK_ID
# ============================================================

print()
print("=" * 70)
print("ETAPA 2 — IDENTIFICANDO LIVROS")
print("=" * 70)


if "book_id" not in df_reviews.columns:

    raise ValueError(
        "A coluna 'book_id' não foi encontrada "
        "no dataset de reviews."
    )


# Garantir que book_id seja string
df_reviews["book_id"] = (
    df_reviews["book_id"]
    .astype(str)
    .str.strip()
)


book_ids = set(
    df_reviews["book_id"]
    .dropna()
    .tolist()
)


print()
print(
    f"Reviews selecionados: "
    f"{len(df_reviews):,}"
)

print(
    f"Livros únicos encontrados nos reviews: "
    f"{len(book_ids):,}"
)


# ============================================================
# ETAPA 3 — BUSCAR OS LIVROS
# ============================================================

print()
print("=" * 70)
print("ETAPA 3 — BUSCANDO LIVROS")
print("=" * 70)

print()
print(
    "Agora o arquivo goodreads_books.json.gz "
    "será percorrido."
)

print(
    "Somente os livros presentes na amostra "
    "serão mantidos."
)

print()


livros = []

livros_encontrados = set()


with gzip.open(
    ARQUIVO_BOOKS,
    "rt",
    encoding="utf-8"
) as arquivo:

    for i, linha in enumerate(arquivo):

        try:
            book = json.loads(linha)

        except json.JSONDecodeError:

            print(
                f"Erro ao ler livro na linha {i}. "
                "Pulando..."
            )

            continue

        book_id = str(
            book.get(
                "book_id",
                ""
            )
        ).strip()

        # ----------------------------------------------------
        # Verificar se esse livro está na nossa amostra
        # ----------------------------------------------------

        if book_id not in book_ids:
            continue


        # ----------------------------------------------------
        # Extrair autores
        # ----------------------------------------------------

        authors = book.get(
            "authors",
            []
        )

        author_ids = []

        if isinstance(authors, list):

            for author in authors:

                if isinstance(author, dict):

                    author_id = author.get(
                        "author_id"
                    )

                    if author_id:
                        author_ids.append(
                            str(author_id)
                        )


        # ----------------------------------------------------
        # Extrair popular shelves
        # ----------------------------------------------------

        popular_shelves = book.get(
            "popular_shelves",
            []
        )

        shelf_names = []

        if isinstance(
            popular_shelves,
            list
        ):

            for shelf in popular_shelves:

                if isinstance(
                    shelf,
                    dict
                ):

                    shelf_name = shelf.get(
                        "name"
                    )

                    if shelf_name:
                        shelf_names.append(
                            str(shelf_name)
                        )


        # ----------------------------------------------------
        # Criar registro simplificado
        # ----------------------------------------------------

        livro_processado = {

            "book_id": book_id,

            "title": book.get(
                "title",
                ""
            ),

            "title_without_series": book.get(
                "title_without_series",
                ""
            ),

            "author_ids": "|".join(
                author_ids
            ),

            "isbn": book.get(
                "isbn",
                ""
            ),

            "isbn13": book.get(
                "isbn13",
                ""
            ),

            "publication_year": book.get(
                "publication_year",
                ""
            ),

            "publication_month": book.get(
                "publication_month",
                ""
            ),

            "publication_day": book.get(
                "publication_day",
                ""
            ),

            "num_pages": book.get(
                "num_pages",
                ""
            ),

            "description": book.get(
                "description",
                ""
            ),

            "publisher": book.get(
                "publisher",
                ""
            ),

            "language_code": book.get(
                "language_code",
                ""
            ),

            "country_code": book.get(
                "country_code",
                ""
            ),

            "format": book.get(
                "format",
                ""
            ),

            "is_ebook": book.get(
                "is_ebook",
                ""
            ),

            "average_rating": book.get(
                "average_rating",
                ""
            ),

            "ratings_count": book.get(
                "ratings_count",
                ""
            ),

            "text_reviews_count": book.get(
                "text_reviews_count",
                ""
            ),

            "popular_shelves": "|".join(
                shelf_names
            ),

            "series": "|".join(
                str(series)
                for series in book.get(
                    "series",
                    []
                )
                if series
            ),

            "work_id": book.get(
                "work_id",
                ""
            )
        }


        livros.append(
            livro_processado
        )

        livros_encontrados.add(
            book_id
        )


        # ----------------------------------------------------
        # Progresso
        # ----------------------------------------------------

        if (i + 1) % 1_000_000 == 0:

            print(
                f"{i + 1:,} livros processados | "
                f"{len(livros_encontrados):,} "
                f"livros encontrados"
            )


        # ----------------------------------------------------
        # Se todos foram encontrados,
        # não precisamos continuar.
        # ----------------------------------------------------

        if len(livros_encontrados) == len(book_ids):

            print()
            print(
                "Todos os livros necessários "
                "foram encontrados."
            )

            break


# ============================================================
# DATAFRAME DOS LIVROS
# ============================================================

print()
print(
    "Convertendo livros encontrados "
    "para DataFrame..."
)

df_books = pd.DataFrame(
    livros
)


# ============================================================
# SALVAR LIVROS
# ============================================================

print()
print("Salvando livros...")

df_books.to_parquet(
    ARQUIVO_BOOKS_SAIDA,
    index=False
)


# ============================================================
# ESTATÍSTICAS
# ============================================================

livros_nao_encontrados = (
    book_ids -
    livros_encontrados
)

print()
print("=" * 70)
print("AMOSTRAGEM E EXTRAÇÃO CONCLUÍDAS")
print("=" * 70)

print()

print(
    f"Reviews selecionados: "
    f"{len(df_reviews):,}"
)

print(
    f"Livros únicos necessários: "
    f"{len(book_ids):,}"
)

print(
    f"Livros encontrados: "
    f"{len(livros_encontrados):,}"
)

print(
    f"Livros não encontrados: "
    f"{len(livros_nao_encontrados):,}"
)

print()

print(
    f"Reviews:\n"
    f"{ARQUIVO_REVIEWS_SAIDA}"
)

print()

print(
    f"Books:\n"
    f"{ARQUIVO_BOOKS_SAIDA}"
)

print()

print("Dimensões dos reviews:")
print(df_reviews.shape)

print()

print("Dimensões dos books:")
print(df_books.shape)

print()

print("Colunas dos reviews:")
print(df_reviews.columns.tolist())

print()

print("Colunas dos books:")
print(df_books.columns.tolist())

print()

print("Primeiros reviews:")
print(df_reviews.head())

print()

print("Primeiros livros:")
print(df_books.head())

print()



import os
import time
import json
import requests
import pandas as pd
from tqdm import tqdm


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
# Dataset Goodreads
# ------------------------------------------------------------

ARQUIVO_BOOKS = os.path.join(
    PASTA_PROCESSED,
    "goodreads_books_100k.parquet"
)


ARQUIVO_REVIEWS = os.path.join(
    PASTA_PROCESSED,
    "goodreads_reviews_100k.parquet"
)


# ------------------------------------------------------------
# Arquivo com dados da Google Books API
# ------------------------------------------------------------

ARQUIVO_GOOGLE_BOOKS = os.path.join(
    PASTA_PROCESSED,
    "google_books_100k.parquet"
)


# ------------------------------------------------------------
# Dataset final
# ------------------------------------------------------------

ARQUIVO_FINAL = os.path.join(
    PASTA_PROCESSED,
    "goodreads_reviews_google_books_100k.parquet"
)


# ------------------------------------------------------------
# Cache JSON
# ------------------------------------------------------------

ARQUIVO_CACHE = os.path.join(
    PASTA_PROCESSED,
    "google_books_cache.json"
)


# ------------------------------------------------------------
# API
# ------------------------------------------------------------

GOOGLE_BOOKS_URL = (
    "https://www.googleapis.com/books/v1/volumes"
)


API_KEY = os.getenv(
    "GOOGLE_BOOKS_API_KEY"
)


# ------------------------------------------------------------
# Configurações de requisição
# ------------------------------------------------------------

TEMPO_ENTRE_REQUISICOES = 0.15

TIMEOUT = 15

MAX_RESULTADOS = 5


# ============================================================
# VERIFICAÇÕES
# ============================================================

print("=" * 70)
print("GOODREADS + GOOGLE BOOKS API")
print("=" * 70)


if not API_KEY:

    raise RuntimeError(
        "\nA variável GOOGLE_BOOKS_API_KEY não foi encontrada.\n\n"
        "Configure sua chave com:\n\n"
        'export GOOGLE_BOOKS_API_KEY="SUA_CHAVE_AQUI"\n'
    )


if not os.path.exists(ARQUIVO_BOOKS):

    raise FileNotFoundError(
        f"\nArquivo não encontrado:\n{ARQUIVO_BOOKS}\n"
    )


if not os.path.exists(ARQUIVO_REVIEWS):

    raise FileNotFoundError(
        f"\nArquivo não encontrado:\n{ARQUIVO_REVIEWS}\n"
    )


os.makedirs(
    PASTA_PROCESSED,
    exist_ok=True
)


# ============================================================
# CACHE
# ============================================================

def carregar_cache():

    if not os.path.exists(
        ARQUIVO_CACHE
    ):
        return {}

    try:

        with open(
            ARQUIVO_CACHE,
            "r",
            encoding="utf-8"
        ) as arquivo:

            return json.load(
                arquivo
            )

    except Exception:

        print(
            "Não foi possível carregar o cache."
        )

        return {}


def salvar_cache(cache):

    with open(
        ARQUIVO_CACHE,
        "w",
        encoding="utf-8"
    ) as arquivo:

        json.dump(
            cache,
            arquivo,
            ensure_ascii=False,
            indent=2
        )


cache = carregar_cache()

print()
print(
    f"Entradas no cache: "
    f"{len(cache):,}"
)


# ============================================================
# CARREGAR DATASET
# ============================================================

print()
print("=" * 70)
print("CARREGANDO DATASET GOODREADS")
print("=" * 70)


df_books = pd.read_parquet(
    ARQUIVO_BOOKS
)


df_reviews = pd.read_parquet(
    ARQUIVO_REVIEWS
)


print()
print(
    f"Livros: "
    f"{len(df_books):,}"
)

print(
    f"Reviews: "
    f"{len(df_reviews):,}"
)


# ============================================================
# NORMALIZAÇÃO
# ============================================================

def limpar_valor(valor):

    if pd.isna(valor):
        return ""

    valor = str(valor).strip()

    if valor.lower() in [
        "nan",
        "none",
        "null"
    ]:
        return ""

    return valor


for coluna in [
    "isbn",
    "isbn13",
    "title",
    "title_without_series",
    "author_ids"
]:

    if coluna not in df_books.columns:

        df_books[coluna] = ""

    df_books[coluna] = (
        df_books[coluna]
        .apply(limpar_valor)
    )


# ============================================================
# FUNÇÕES DE BUSCA
# ============================================================

def requisicao_google_books(
    query
):

    parametros = {
        "q": query,
        "maxResults": MAX_RESULTADOS,
        "printType": "books",
        "key": API_KEY
    }

    try:

        resposta = requests.get(
            GOOGLE_BOOKS_URL,
            params=parametros,
            timeout=TIMEOUT
        )

    except requests.RequestException as erro:

        return {
            "status": "request_error",
            "error": str(erro)
        }


    # --------------------------------------------------------
    # Rate limit
    # --------------------------------------------------------

    if resposta.status_code == 429:

        return {
            "status": "rate_limit",
            "error": "HTTP 429"
        }


    # --------------------------------------------------------
    # Outros erros
    # --------------------------------------------------------

    if resposta.status_code != 200:

        return {
            "status": "http_error",
            "http_status": resposta.status_code,
            "error": resposta.text[:500]
        }


    try:

        dados = resposta.json()

    except ValueError:

        return {
            "status": "invalid_json"
        }


    return {
        "status": "success",
        "data": dados
    }


# ============================================================
# EXTRAÇÃO DOS DADOS DA GOOGLE BOOKS
# ============================================================

def extrair_volume(
    volume,
    metodo_busca
):

    volume_info = volume.get(
        "volumeInfo",
        {}
    )

    sale_info = volume.get(
        "saleInfo",
        {}
    )

    access_info = volume.get(
        "accessInfo",
        {}
    )


    # --------------------------------------------------------
    # Autores
    # --------------------------------------------------------

    autores = volume_info.get(
        "authors",
        []
    )

    if isinstance(
        autores,
        list
    ):

        autores = "|".join(
            str(autor)
            for autor in autores
        )

    else:

        autores = ""


    # --------------------------------------------------------
    # Categorias
    # --------------------------------------------------------

    categorias = volume_info.get(
        "categories",
        []
    )

    if isinstance(
        categorias,
        list
    ):

        categorias = "|".join(
            str(categoria)
            for categoria in categorias
        )

    else:

        categorias = ""


    # --------------------------------------------------------
    # Identificadores
    # --------------------------------------------------------

    identifiers = volume_info.get(
        "industryIdentifiers",
        []
    )

    isbn_10 = ""
    isbn_13 = ""

    if isinstance(
        identifiers,
        list
    ):

        for identifier in identifiers:

            if not isinstance(
                identifier,
                dict
            ):
                continue

            tipo = identifier.get(
                "type",
                ""
            )

            valor = identifier.get(
                "identifier",
                ""
            )

            if tipo == "ISBN_10":
                isbn_10 = valor

            elif tipo == "ISBN_13":
                isbn_13 = valor


    # --------------------------------------------------------
    # Imagem
    # --------------------------------------------------------

    image_links = volume_info.get(
        "imageLinks",
        {}
    )

    if isinstance(
        image_links,
        dict
    ):

        thumbnail = image_links.get(
            "thumbnail",
            ""
        )

        small_thumbnail = image_links.get(
            "smallThumbnail",
            ""
        )

    else:

        thumbnail = ""
        small_thumbnail = ""


    # --------------------------------------------------------
    # Retorno
    # --------------------------------------------------------

    return {

        "google_volume_id": volume.get(
            "id",
            ""
        ),

        "google_kind": volume.get(
            "kind",
            ""
        ),

        "search_method": metodo_busca,

        "google_title": volume_info.get(
            "title",
            ""
        ),

        "google_subtitle": volume_info.get(
            "subtitle",
            ""
        ),

        "google_authors": autores,

        "google_publisher": volume_info.get(
            "publisher",
            ""
        ),

        "google_published_date": volume_info.get(
            "publishedDate",
            ""
        ),

        "google_description": volume_info.get(
            "description",
            ""
        ),

        "google_page_count": volume_info.get(
            "pageCount",
            ""
        ),

        "google_categories": categorias,

        "google_average_rating": volume_info.get(
            "averageRating",
            ""
        ),

        "google_ratings_count": volume_info.get(
            "ratingsCount",
            ""
        ),

        "google_language": volume_info.get(
            "language",
            ""
        ),

        "google_isbn10": isbn_10,

        "google_isbn13": isbn_13,

        "google_maturity_rating": volume_info.get(
            "maturityRating",
            ""
        ),

        "google_print_type": volume_info.get(
            "printType",
            ""
        ),

        "google_text_snippet": volume_info.get(
            "textSnippet",
            ""
        ),

        "google_thumbnail": thumbnail,

        "google_small_thumbnail": small_thumbnail,

        "google_preview_link": volume_info.get(
            "previewLink",
            ""
        ),

        "google_info_link": volume_info.get(
            "infoLink",
            ""
        ),

        "google_web_reader_link": access_info.get(
            "webReaderLink",
            ""
        ),

        "google_viewability": access_info.get(
            "viewability",
            ""
        ),

        "google_public_domain": access_info.get(
            "publicDomain",
            ""
        ),

        "google_ebook_available": (
            "epub" in access_info or
            "pdf" in access_info
        ),

        "google_saleability": sale_info.get(
            "saleability",
            ""
        )
    }


# ============================================================
# BUSCA DE UM LIVRO
# ============================================================

def buscar_livro(
    livro
):

    isbn13 = limpar_valor(
        livro.get(
            "isbn13",
            ""
        )
    )

    isbn = limpar_valor(
        livro.get(
            "isbn",
            ""
        )
    )

    titulo = limpar_valor(
        livro.get(
            "title",
            ""
        )
    )

    autor_ids = limpar_valor(
        livro.get(
            "author_ids",
            ""
        )
    )


    # --------------------------------------------------------
    # 1. ISBN-13
    # --------------------------------------------------------

    if isbn13:

        resultado = requisicao_google_books(
            f"isbn:{isbn13}"
        )

        if resultado["status"] == "success":

            itens = resultado["data"].get(
                "items",
                []
            )

            if itens:

                return extrair_volume(
                    itens[0],
                    "isbn13"
                )


        elif resultado["status"] == "rate_limit":

            time.sleep(2)


    # --------------------------------------------------------
    # 2. ISBN
    # --------------------------------------------------------

    if isbn:

        resultado = requisicao_google_books(
            f"isbn:{isbn}"
        )

        if resultado["status"] == "success":

            itens = resultado["data"].get(
                "items",
                []
            )

            if itens:

                return extrair_volume(
                    itens[0],
                    "isbn"
                )


        elif resultado["status"] == "rate_limit":

            time.sleep(2)


    # --------------------------------------------------------
    # 3. Título
    # --------------------------------------------------------

    if titulo:

        consulta = (
            f'intitle:"{titulo}"'
        )

        resultado = requisicao_google_books(
            consulta
        )

        if resultado["status"] == "success":

            itens = resultado["data"].get(
                "items",
                []
            )

            if itens:

                return extrair_volume(
                    itens[0],
                    "title"
                )


        elif resultado["status"] == "rate_limit":

            time.sleep(2)


    # --------------------------------------------------------
    # Nenhum resultado
    # --------------------------------------------------------

    return None


# ============================================================
# PROCESSAMENTO
# ============================================================

print()
print("=" * 70)
print("CONSULTANDO GOOGLE BOOKS API")
print("=" * 70)

print()
print(
    "Estratégia:"
)

print(
    "1. ISBN-13"
)

print(
    "2. ISBN"
)

print(
    "3. Título"
)

print()


resultados = []


total = len(
    df_books
)

encontrados = 0
nao_encontrados = 0
erros = 0


for indice, livro in tqdm(
    df_books.iterrows(),
    total=total,
    desc="Consultando livros"
):

    book_id = limpar_valor(
        livro["book_id"]
    )


    # --------------------------------------------------------
    # Cache
    # --------------------------------------------------------

    if book_id in cache:

        resultado = cache[
            book_id
        ]

        if resultado:

            resultado["goodreads_book_id"] = (
                book_id
            )

            resultados.append(
                resultado
            )

            encontrados += 1

        else:

            nao_encontrados += 1

        continue


    # --------------------------------------------------------
    # API
    # --------------------------------------------------------

    resultado = buscar_livro(
        livro
    )


    # --------------------------------------------------------
    # Salvar cache
    # --------------------------------------------------------

    cache[
        book_id
    ] = resultado


    # --------------------------------------------------------
    # Resultado
    # --------------------------------------------------------

    if resultado:

        resultado[
            "goodreads_book_id"
        ] = book_id

        resultados.append(
            resultado
        )

        encontrados += 1

    else:

        nao_encontrados += 1


    # --------------------------------------------------------
    # Salvar cache periodicamente
    # --------------------------------------------------------

    if (
        encontrados +
        nao_encontrados
    ) % 50 == 0:

        salvar_cache(
            cache
        )


    # --------------------------------------------------------
    # Intervalo
    # --------------------------------------------------------

    time.sleep(
        TEMPO_ENTRE_REQUISICOES
    )


# ============================================================
# SALVAR CACHE FINAL
# ============================================================

salvar_cache(
    cache
)


# ============================================================
# DATAFRAME GOOGLE BOOKS
# ============================================================

print()
print("=" * 70)
print("PROCESSANDO RESULTADOS")
print("=" * 70)


df_google = pd.DataFrame(
    resultados
)


if len(df_google) > 0:

    # Evitar duplicações
    df_google = (
        df_google
        .drop_duplicates(
            subset=[
                "goodreads_book_id"
            ]
        )
    )


# ============================================================
# SALVAR GOOGLE BOOKS
# ============================================================

df_google.to_parquet(
    ARQUIVO_GOOGLE_BOOKS,
    index=False
)


print()
print(
    f"Dados Google Books salvos em:\n"
    f"{ARQUIVO_GOOGLE_BOOKS}"
)


# ============================================================
# CRUZAMENTO COM GOODREADS
# ============================================================

print()
print("=" * 70)
print("CRUZANDO GOODREADS + GOOGLE BOOKS")
print("=" * 70)


if len(df_google) > 0:

    df_google[
        "goodreads_book_id"
    ] = (
        df_google[
            "goodreads_book_id"
        ]
        .astype(str)
        .str.strip()
    )


    df_books[
        "book_id"
    ] = (
        df_books[
            "book_id"
        ]
        .astype(str)
        .str.strip()
    )


    df_enriquecido = df_books.merge(
        df_google,
        left_on="book_id",
        right_on="goodreads_book_id",
        how="left"
    )

else:

    df_enriquecido = df_books.copy()


# ============================================================
# CRUZAR COM REVIEWS
# ============================================================

print()
print(
    "Adicionando informações aos reviews..."
)


df_reviews[
    "book_id"
] = (
    df_reviews[
        "book_id"
    ]
    .astype(str)
    .str.strip()
)


df_enriquecido[
    "book_id"
] = (
    df_enriquecido[
        "book_id"
    ]
    .astype(str)
    .str.strip()
)


df_final = df_reviews.merge(
    df_enriquecido,
    on="book_id",
    how="left",
    suffixes=(
        "",
        "_book"
    )
)


# ============================================================
# ESTATÍSTICAS
# ============================================================

total_livros = len(
    df_books
)

total_google = len(
    df_google
)

total_reviews = len(
    df_reviews
)


reviews_com_google = 0


if (
    "google_volume_id"
    in df_final.columns
):

    reviews_com_google = (
        df_final[
            "google_volume_id"
        ]
        .notna()
        .sum()
    )


print()
print("=" * 70)
print("RESULTADO")
print("=" * 70)

print()

print(
    f"Livros Goodreads: "
    f"{total_livros:,}"
)

print(
    f"Livros encontrados na Google Books: "
    f"{total_google:,}"
)

print(
    f"Reviews: "
    f"{total_reviews:,}"
)

print(
    f"Reviews com dados Google Books: "
    f"{reviews_com_google:,}"
)


if total_reviews > 0:

    percentual = (
        reviews_com_google /
        total_reviews
    ) * 100

else:

    percentual = 0


print(
    f"Taxa de enriquecimento: "
    f"{percentual:.2f}%"
)


# ============================================================
# SALVAR DATASET FINAL
# ============================================================

print()
print(
    "Salvando dataset final..."
)


df_final.to_parquet(
    ARQUIVO_FINAL,
    index=False
)


# ============================================================
# AMOSTRA
# ============================================================

print()
print("=" * 70)
print("AMOSTRA DO DATASET FINAL")
print("=" * 70)


colunas_amostra = [

    "user_id",

    "book_id",

    "rating",

    "review_text",

    "title",

    "publication_year",

    "publisher",

    "language_code",

    "average_rating",

    "ratings_count",

    "google_volume_id",

    "google_title",

    "google_authors",

    "google_publisher",

    "google_published_date",

    "google_categories",

    "google_page_count",

    "google_average_rating",

    "google_ratings_count",

    "google_language",

    "google_isbn10",

    "google_isbn13",

    "google_preview_link"
]


colunas_existentes = [

    coluna

    for coluna in colunas_amostra

    if coluna in df_final.columns
]


print(
    df_final[
        colunas_existentes
    ]
    .head(10)
    .to_string(
        index=False
    )
)


# ============================================================
# FINAL
# ============================================================

print()
print("=" * 70)
print("CRUZAMENTO CONCLUÍDO")
print("=" * 70)

print()

print(
    "Arquivos gerados:"
)

print()

print(
    f"Google Books:\n"
    f"{ARQUIVO_GOOGLE_BOOKS}"
)

print()

print(
    f"Dataset final:\n"
    f"{ARQUIVO_FINAL}"
)

print()

print(
    f"Cache:\n"
    f"{ARQUIVO_CACHE}"
)

print()

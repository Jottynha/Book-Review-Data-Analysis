import os
import sys
import time
import json
import re
from pathlib import Path
import concurrent.futures
import requests
import pandas as pd
from tqdm import tqdm

# Configura encoding do terminal Windows para suportar caracteres especiais/emojis
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


# ============================================================
# CONFIGURAÇÃO
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
PASTA_PROJETO = Path(os.getenv("PASTA_PROJETO", str(BASE_DIR)))
PASTA_PROCESSED = PASTA_PROJETO / "processed"

# Carregar variáveis de ambiente de arquivo .env local se existir
ENV_FILE = BASE_DIR / ".env"
if ENV_FILE.exists():
    try:
        with open(ENV_FILE, "r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    k = k.lstrip("\ufeff").strip()
                    v = v.strip().strip('"').strip("'")
                    if k and k not in os.environ:
                        os.environ[k] = v
    except Exception:
        pass

# Flags de linha de comando
SKIP_OPENLIBRARY = "--skip-openlibrary" in sys.argv or "--only-google" in sys.argv

# Arquivos de entrada
ARQUIVO_BOOKS = PASTA_PROCESSED / "goodreads_books_100k.parquet"
ARQUIVO_REVIEWS = PASTA_PROCESSED / "goodreads_reviews_100k.parquet"

# Arquivos de saída
ARQUIVO_GOOGLE_BOOKS = PASTA_PROCESSED / "google_books_100k.parquet"
ARQUIVO_FINAL = PASTA_PROCESSED / "goodreads_reviews_google_books_100k.parquet"
ARQUIVO_CACHE = PASTA_PROCESSED / "google_books_cache.json"

# Configurações Open Library API (Batch por ISBN)
OPEN_LIBRARY_URL = "https://openlibrary.org/api/books"
OPEN_LIBRARY_BATCH_SIZE = 50
OPEN_LIBRARY_DELAY = 0.4
OPEN_LIBRARY_HEADERS = {
    "User-Agent": "Book-Review-Data-Analysis/1.0 (CEFET-MG Data Science Project; contact: aluno@cefetmg.br)"
}

# Configurações Open Library Search API (Por Título com Validação)
OPEN_LIBRARY_SEARCH_URL = "https://openlibrary.org/search.json"
MAX_WORKERS_SEARCH = 6  # Concorrência eficiente e respeitosa com a Open Library
THRESHOLD_VALIDACAO = 50

# Configurações Google Books API (Fallback)
GOOGLE_BOOKS_URL = "https://www.googleapis.com/books/v1/volumes"
API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")
TEMPO_ENTRE_REQUISICOES_GOOGLE = 0.2
TIMEOUT = 15
MAX_RESULTADOS = 5


# ============================================================
# VERIFICAÇÕES INICIAIS
# ============================================================

print("=" * 70)
print("ENRIQUECIMENTO DE DADOS: OPEN LIBRARY (BATCH + SEARCH) + GOOGLE BOOKS")
print("=" * 70)

if not os.path.exists(ARQUIVO_BOOKS):
    raise FileNotFoundError(f"\nArquivo de livros não encontrado:\n{ARQUIVO_BOOKS}\n")

if not os.path.exists(ARQUIVO_REVIEWS):
    raise FileNotFoundError(f"\nArquivo de reviews não encontrado:\n{ARQUIVO_REVIEWS}\n")

os.makedirs(PASTA_PROCESSED, exist_ok=True)


# ============================================================
# GERENCIAMENTO DE CACHE
# ============================================================

def carregar_cache():
    if not os.path.exists(ARQUIVO_CACHE):
        return {}
    try:
        with open(ARQUIVO_CACHE, "r", encoding="utf-8") as arquivo:
            return json.load(arquivo)
    except Exception as e:
        print(f"Não foi possível carregar o cache anterior ({e}). Iniciando novo cache.")
        return {}


def salvar_cache(cache):
    try:
        with open(ARQUIVO_CACHE, "w", encoding="utf-8") as arquivo:
            json.dump(cache, arquivo, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Erro ao salvar cache: {e}")


cache = carregar_cache()

def ja_coletado(book_id):
    """Verifica se o livro já foi coletado com sucesso anteriormente."""
    b_id = str(book_id).strip()
    return b_id in cache and bool(cache[b_id])


total_cache_valido = sum(1 for v in cache.values() if v)
print(f"Cache carregado: {len(cache):,} entradas ({total_cache_valido:,} livros válidos já coletados)")


# ============================================================
# CARREGAR DATASETS
# ============================================================

print()
print("=" * 70)
print("CARREGANDO DATASETS GOODREADS")
print("=" * 70)

df_books = pd.read_parquet(ARQUIVO_BOOKS)
df_reviews = pd.read_parquet(ARQUIVO_REVIEWS)

print(f"Livros únicos: {len(df_books):,}")
print(f"Reviews:       {len(df_reviews):,}")


# ============================================================
# NORMALIZAÇÃO DE VALORES E STRINGS
# ============================================================

def limpar_valor(valor):
    if pd.isna(valor):
        return ""
    v = str(valor).strip()
    if v.lower() in ["nan", "none", "null"]:
        return ""
    return v


def normalizar_texto(texto):
    """Normaliza texto para comparações robustas (sem pontuação e minúsculo)."""
    return re.sub(r"[^a-z0-9 ]", "", str(texto).lower()).strip()


def limpar_titulo(titulo):
    """Remove subtítulos e sufixos de série entre parênteses para busca limpa."""
    t = str(titulo).strip()
    t_sem_parenteses = re.sub(r"\s*\(.*?\)\s*", " ", t)
    t_sem_dois_pontos = t_sem_parenteses.split(":")[0]
    return t_sem_dois_pontos.strip()


for col in ["isbn", "isbn13", "title", "title_without_series", "author_ids", "publisher", "publication_year", "num_pages"]:
    if col not in df_books.columns:
        df_books[col] = ""
    df_books[col] = df_books[col].apply(limpar_valor)


# ============================================================
# PARSERS DE RESPOSTA
# ============================================================

def extrair_volume_openlibrary(data, book_id, metodo_busca="openlibrary_batch"):
    authors_list = []
    authors_raw = data.get("authors", [])
    if isinstance(authors_raw, list):
        for a in authors_raw:
            if isinstance(a, dict) and a.get("name"):
                authors_list.append(str(a.get("name")).strip())
    autores = "|".join(authors_list)
    if not autores and data.get("by_statement"):
        autores = str(data.get("by_statement")).replace("by ", "").strip()

    subjects_list = []
    subjects_raw = data.get("subjects", [])
    if isinstance(subjects_raw, list):
        for s in subjects_raw:
            if isinstance(s, dict) and s.get("name"):
                subjects_list.append(str(s.get("name")).strip())
            elif isinstance(s, str):
                subjects_list.append(s.strip())
    categorias = "|".join(subjects_list[:15])

    publishers_list = []
    publishers_raw = data.get("publishers", [])
    if isinstance(publishers_raw, list):
        for p in publishers_raw:
            if isinstance(p, dict) and p.get("name"):
                publishers_list.append(str(p.get("name")).strip())
            elif isinstance(p, str):
                publishers_list.append(p.strip())
    editora = ", ".join(publishers_list)

    cover = data.get("cover", {})
    thumbnail = ""
    small_thumbnail = ""
    if isinstance(cover, dict):
        thumbnail = cover.get("medium", "") or cover.get("large", "")
        small_thumbnail = cover.get("small", "")

    identifiers = data.get("identifiers", {})
    isbn10_list = identifiers.get("isbn_10", []) if isinstance(identifiers, dict) else []
    isbn13_list = identifiers.get("isbn_13", []) if isinstance(identifiers, dict) else []
    isbn10 = isbn10_list[0] if (isinstance(isbn10_list, list) and isbn10_list) else ""
    isbn13 = isbn13_list[0] if (isinstance(isbn13_list, list) and isbn13_list) else ""

    page_count = data.get("number_of_pages", "")
    if not page_count and data.get("pagination"):
        page_count = str(data.get("pagination")).strip()

    return {
        "google_volume_id": data.get("key", ""),
        "google_kind": "openlibrary#book",
        "search_method": metodo_busca,
        "google_title": data.get("title", ""),
        "google_subtitle": data.get("subtitle", ""),
        "google_authors": autores,
        "google_publisher": editora,
        "google_published_date": str(data.get("publish_date", "")),
        "google_description": str(data.get("notes", "") or ""),
        "google_page_count": page_count,
        "google_categories": categorias,
        "google_average_rating": "",
        "google_ratings_count": "",
        "google_language": "",
        "google_isbn10": isbn10,
        "google_isbn13": isbn13,
        "google_maturity_rating": "",
        "google_print_type": "BOOK",
        "google_text_snippet": "",
        "google_thumbnail": thumbnail,
        "google_small_thumbnail": small_thumbnail,
        "google_preview_link": data.get("url", ""),
        "google_info_link": data.get("url", ""),
        "google_web_reader_link": "",
        "google_viewability": "",
        "google_public_domain": False,
        "google_ebook_available": False,
        "google_saleability": "",
        "goodreads_book_id": str(book_id),
        "api_source": "openlibrary"
    }


def extrair_volume_openlibrary_search(doc, book_id, score_validacao=100):
    autores_raw = doc.get("author_name", [])
    autores = "|".join(str(a).strip() for a in autores_raw) if isinstance(autores_raw, list) else str(autores_raw)

    publishers_raw = doc.get("publisher", [])
    editora = ", ".join(str(p).strip() for p in publishers_raw[:3]) if isinstance(publishers_raw, list) else str(publishers_raw)

    subjects_raw = doc.get("subject", [])
    categorias = "|".join(str(s).strip() for s in subjects_raw[:15]) if isinstance(subjects_raw, list) else ""

    cover_id = doc.get("cover_i")
    thumbnail = f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg" if cover_id else ""
    small_thumbnail = f"https://covers.openlibrary.org/b/id/{cover_id}-S.jpg" if cover_id else ""

    isbns = doc.get("isbn", [])
    isbn10 = ""
    isbn13 = ""
    if isinstance(isbns, list):
        for code in isbns:
            code_str = str(code).strip()
            if len(code_str) == 13 and not isbn13:
                isbn13 = code_str
            elif len(code_str) == 10 and not isbn10:
                isbn10 = code_str

    key = doc.get("key", "")
    url = f"https://openlibrary.org{key}" if key else ""

    return {
        "google_volume_id": key,
        "google_kind": "openlibrary#search_doc",
        "search_method": f"openlibrary_search_validated_score_{score_validacao}",
        "google_title": doc.get("title", ""),
        "google_subtitle": doc.get("subtitle", ""),
        "google_authors": autores,
        "google_publisher": editora,
        "google_published_date": str(doc.get("first_publish_year", "")),
        "google_description": "",
        "google_page_count": doc.get("number_of_pages_median", ""),
        "google_categories": categorias,
        "google_average_rating": "",
        "google_ratings_count": "",
        "google_language": "",
        "google_isbn10": isbn10,
        "google_isbn13": isbn13,
        "google_maturity_rating": "",
        "google_print_type": "BOOK",
        "google_text_snippet": "",
        "google_thumbnail": thumbnail,
        "google_small_thumbnail": small_thumbnail,
        "google_preview_link": url,
        "google_info_link": url,
        "google_web_reader_link": "",
        "google_viewability": "",
        "google_public_domain": False,
        "google_ebook_available": False,
        "google_saleability": "",
        "goodreads_book_id": str(book_id),
        "api_source": "openlibrary_search"
    }


def extrair_volume_google(volume, metodo_busca, book_id):
    volume_info = volume.get("volumeInfo", {})
    sale_info = volume.get("saleInfo", {})
    access_info = volume.get("accessInfo", {})

    autores = volume_info.get("authors", [])
    autores = "|".join(str(autor) for autor in autores) if isinstance(autores, list) else ""

    categorias = volume_info.get("categories", [])
    categorias = "|".join(str(categoria) for categoria in categorias) if isinstance(categorias, list) else ""

    identifiers = volume_info.get("industryIdentifiers", [])
    isbn_10 = ""
    isbn_13 = ""
    if isinstance(identifiers, list):
        for identifier in identifiers:
            if isinstance(identifier, dict):
                tipo = identifier.get("type", "")
                valor = identifier.get("identifier", "")
                if tipo == "ISBN_10":
                    isbn_10 = valor
                elif tipo == "ISBN_13":
                    isbn_13 = valor

    image_links = volume_info.get("imageLinks", {})
    thumbnail = ""
    small_thumbnail = ""
    if isinstance(image_links, dict):
        thumbnail = image_links.get("thumbnail", "")
        small_thumbnail = image_links.get("smallThumbnail", "")

    return {
        "google_volume_id": volume.get("id", ""),
        "google_kind": volume.get("kind", ""),
        "search_method": metodo_busca,
        "google_title": volume_info.get("title", ""),
        "google_subtitle": volume_info.get("subtitle", ""),
        "google_authors": autores,
        "google_publisher": volume_info.get("publisher", ""),
        "google_published_date": volume_info.get("publishedDate", ""),
        "google_description": volume_info.get("description", ""),
        "google_page_count": volume_info.get("pageCount", ""),
        "google_categories": categorias,
        "google_average_rating": volume_info.get("averageRating", ""),
        "google_ratings_count": volume_info.get("ratingsCount", ""),
        "google_language": volume_info.get("language", ""),
        "google_isbn10": isbn_10,
        "google_isbn13": isbn_13,
        "google_maturity_rating": volume_info.get("maturityRating", ""),
        "google_print_type": volume_info.get("printType", ""),
        "google_text_snippet": volume_info.get("textSnippet", ""),
        "google_thumbnail": thumbnail,
        "google_small_thumbnail": small_thumbnail,
        "google_preview_link": volume_info.get("previewLink", ""),
        "google_info_link": volume_info.get("infoLink", ""),
        "google_web_reader_link": access_info.get("webReaderLink", ""),
        "google_viewability": access_info.get("viewability", ""),
        "google_public_domain": access_info.get("publicDomain", ""),
        "google_ebook_available": ("epub" in access_info or "pdf" in access_info),
        "google_saleability": sale_info.get("saleability", ""),
        "goodreads_book_id": str(book_id),
        "api_source": "google_books"
    }


# ============================================================
# ALGORITMO DE VALIDAÇÃO CRUZADA (ANTI-HOMÔNIMOS)
# ============================================================

def validar_candidato(gr_title, gr_year, gr_publisher, doc):
    """
    Avalia a compatibilidade de um candidato retornado pela Search API
    com os metadados do Goodreads. Retorna um score de 0 a 100.
    """
    score = 0
    t_gr = normalizar_texto(gr_title)
    t_ol = normalizar_texto(doc.get("title", ""))

    if not t_gr or not t_ol:
        return 0

    # 1. Compatibilidade de Título
    if t_gr == t_ol:
        score += 40
        # Bônus para títulos específicos (>= 3 palavras)
        if len(t_gr.split()) >= 3:
            score += 20
    elif t_gr in t_ol or t_ol in t_gr:
        score += 25
    else:
        return 0  # Títulos incompatíveis: rejeição imediata

    # 2. Compatibilidade de Ano
    ol_year = doc.get("first_publish_year")
    if gr_year and ol_year:
        try:
            diff = abs(int(gr_year) - int(ol_year))
            if diff <= 1:
                score += 30
            elif diff <= 3:
                score += 15
            elif diff > 10:
                score -= 25
        except Exception:
            pass

    # 3. Compatibilidade de Editora
    ol_pubs = [normalizar_texto(p) for p in (doc.get("publisher") or [])]
    gr_pub_norm = normalizar_texto(gr_publisher)
    if gr_pub_norm and ol_pubs:
        matched_pub = False
        for p in ol_pubs:
            if p in gr_pub_norm or gr_pub_norm in p or any(w in p for w in gr_pub_norm.split() if len(w) > 3):
                matched_pub = True
                break
        if matched_pub:
            score += 30

    return score


# ============================================================
# FUNÇÕES DE BUSCA: GOOGLE BOOKS API
# ============================================================

def requisicao_google_books(query, api_key=None, max_retries=4):
    parametros = {
        "q": query,
        "maxResults": MAX_RESULTADOS,
        "printType": "books",
    }
    if api_key:
        parametros["key"] = api_key

    delay = 1.0
    for tentativa in range(max_retries):
        try:
            resp = requests.get(GOOGLE_BOOKS_URL, params=parametros, timeout=TIMEOUT)
            if resp.status_code == 200:
                return {"status": "success", "data": resp.json()}
            elif resp.status_code == 429:
                texto_erro = resp.text
                if "Queries per day" in texto_erro:
                    return {"status": "daily_quota_exceeded", "error": texto_erro}
                # Burst rate limit (QPS) passageiro: aguarda e tenta novamente
                time.sleep(delay)
                delay = min(delay * 2, 6.0)
                continue
            else:
                return {"status": "error", "code": resp.status_code, "error": resp.text[:200]}
        except Exception:
            time.sleep(delay)
            delay = min(delay * 2, 6.0)

    return {"status": "burst_rate_limit", "error": "Limite de rajada excedido após retentativas"}


def buscar_livro_google(row, api_key=None):
    """
    Busca um livro no Google Books usando estratégias em cascata:
    1. ISBN-13 ou ISBN-10
    2. Título com validação cruzada anti-homônimo
    """
    b_id = str(row["book_id"]).strip()
    isbn13 = limpar_valor(row.get("isbn13", ""))
    isbn = limpar_valor(row.get("isbn", ""))
    titulo = limpar_valor(row.get("title_without_series", "")) or limpar_valor(row.get("title", ""))
    titulo_limpo = limpar_titulo(titulo)

    # 1. Busca por ISBN (prioriza ISBN-13, senão ISBN-10)
    isbn_busca = isbn13 or isbn
    if isbn_busca:
        res = requisicao_google_books(f"isbn:{isbn_busca}", api_key=api_key)
        if res.get("status") == "daily_quota_exceeded":
            return None, True
        if res.get("status") == "success" and res["data"].get("items"):
            return extrair_volume_google(res["data"]["items"][0], "google_isbn", b_id), False

    # 2. Busca por Título com validação
    if titulo_limpo:
        res = requisicao_google_books(f'intitle:"{titulo_limpo}"', api_key=api_key)
        if res.get("status") == "daily_quota_exceeded":
            return None, True
        if res.get("status") == "success" and res["data"].get("items"):
            itens = res["data"]["items"]
            gr_year = limpar_valor(row.get("publication_year", ""))
            gr_pub = limpar_valor(row.get("publisher", ""))
            melhor_item = None
            maior_score = 0
            for it in itens:
                v_info = it.get("volumeInfo", {})
                pub_date = v_info.get("publishedDate", "")
                ano_cand = pub_date[:4] if len(pub_date) >= 4 and pub_date[:4].isdigit() else ""
                editora_cand = [v_info.get("publisher", "")] if v_info.get("publisher") else []
                doc_simulado = {
                    "title": v_info.get("title", ""),
                    "first_publish_year": ano_cand,
                    "publisher": editora_cand
                }
                sc = validar_candidato(titulo_limpo, gr_year, gr_pub, doc_simulado)
                if sc > maior_score:
                    maior_score = sc
                    melhor_item = it

            if melhor_item and maior_score >= 40:
                return extrair_volume_google(melhor_item, f"google_title_score_{maior_score}", b_id), False

    return None, False


# ============================================================
# ETAPA 1: BUSCA EM LOTE NA OPEN LIBRARY (POR ISBN)
# ============================================================

print()
print("=" * 70)
print("ETAPA 1: OPEN LIBRARY API (LOTE POR ISBN)")
print("=" * 70)

bibkey_to_books = {}
for _, row in df_books.iterrows():
    b_id = str(row["book_id"]).strip()
    if ja_coletado(b_id):
        continue  # PULA LIVROS JÁ COLETADOS

    isbn13 = limpar_valor(row.get("isbn13", ""))
    isbn = limpar_valor(row.get("isbn", ""))

    if isbn13:
        bibkey_to_books.setdefault(f"ISBN:{isbn13}", []).append((b_id, row))
    elif isbn:
        bibkey_to_books.setdefault(f"ISBN:{isbn}", []).append((b_id, row))

if SKIP_OPENLIBRARY:
    print("\n[INFO] Etapa 1 pulada (--skip-openlibrary ativo).")
    todas_bibkeys = []
else:
    todas_bibkeys = list(bibkey_to_books.keys())
    print(f"Livros pendentes com ISBN a buscar: {len(todas_bibkeys):,} chaves")

encontrados_ol = 0
if todas_bibkeys:
    num_lotes = (len(todas_bibkeys) + OPEN_LIBRARY_BATCH_SIZE - 1) // OPEN_LIBRARY_BATCH_SIZE
    pbar = tqdm(range(num_lotes), desc="Consultando Open Library (Lotes)")

    for i in pbar:
        batch_keys = todas_bibkeys[i * OPEN_LIBRARY_BATCH_SIZE : (i + 1) * OPEN_LIBRARY_BATCH_SIZE]
        bibkeys_param = ",".join(batch_keys)

        try:
            resp = requests.get(
                OPEN_LIBRARY_URL,
                params={"bibkeys": bibkeys_param, "format": "json", "jscmd": "data"},
                headers=OPEN_LIBRARY_HEADERS,
                timeout=TIMEOUT
            )
            if resp.status_code == 200:
                batch_data = resp.json()
                for key in batch_keys:
                    if key in batch_data:
                        dados_livro = batch_data[key]
                        for (b_id, _) in bibkey_to_books[key]:
                            vol = extrair_volume_openlibrary(dados_livro, b_id, metodo_busca="openlibrary_batch")
                            cache[b_id] = vol
                            encontrados_ol += 1
        except Exception:
            pass

        if (i + 1) % 10 == 0:
            salvar_cache(cache)

        time.sleep(OPEN_LIBRARY_DELAY)
        pbar.set_postfix({"Encontrados Lote": encontrados_ol})

    salvar_cache(cache)
    print(f"Novos livros encontrados via lote: {encontrados_ol:,}")
else:
    print("Nenhum livro pendente para a Etapa 1. Todos já estavam em cache!")


# ============================================================
# ETAPA 2: OPEN LIBRARY SEARCH API (POR TÍTULO COM VALIDAÇÃO)
# ============================================================

print()
print("=" * 70)
print("ETAPA 2: OPEN LIBRARY SEARCH API (POR TÍTULO COM VALIDAÇÃO CRUZADA)")
print("=" * 70)

livros_para_busca_titulo = []
for _, row in df_books.iterrows():
    b_id = str(row["book_id"]).strip()
    if ja_coletado(b_id):
        continue  # PULA LIVROS JÁ COLETADOS

    titulo_busca = limpar_valor(row.get("title_without_series", "")) or limpar_valor(row.get("title", ""))
    titulo_busca = limpar_titulo(titulo_busca)
    if titulo_busca and not SKIP_OPENLIBRARY:
        livros_para_busca_titulo.append((b_id, titulo_busca, row))

if SKIP_OPENLIBRARY:
    print("\n[INFO] Etapa 2 pulada (--skip-openlibrary ativo).")
else:
    print(f"Livros pendentes para busca por título: {len(livros_para_busca_titulo):,}")

encontrados_ol_search = 0

def buscar_e_validar_livro(item):
    b_id, titulo_busca, row = item
    gr_year = limpar_valor(row.get("publication_year", ""))
    gr_pub = limpar_valor(row.get("publisher", ""))

    try:
        r = requests.get(
            OPEN_LIBRARY_SEARCH_URL,
            params={
                "title": titulo_busca,
                "limit": 5,
                "fields": "key,title,subtitle,author_name,first_publish_year,publisher,isbn,subject,number_of_pages_median,cover_i"
            },
            headers=OPEN_LIBRARY_HEADERS,
            timeout=12
        )
        if r.status_code == 200:
            docs = r.json().get("docs", [])
            melhor_doc = None
            maior_score = 0

            for doc in docs:
                # Exige que tenha autor
                if not doc.get("author_name"):
                    continue
                score = validar_candidato(titulo_busca, gr_year, gr_pub, doc)
                if score > maior_score:
                    maior_score = score
                    melhor_doc = doc

            if melhor_doc and maior_score >= THRESHOLD_VALIDACAO:
                vol = extrair_volume_openlibrary_search(melhor_doc, b_id, score_validacao=maior_score)
                return b_id, vol
    except Exception:
        pass

    return b_id, None


if livros_para_busca_titulo:
    pbar_search = tqdm(total=len(livros_para_busca_titulo), desc="Buscando por Título (Open Library)")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_SEARCH) as executor:
        futures = {executor.submit(buscar_e_validar_livro, item): item for item in livros_para_busca_titulo}

        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                b_id, vol = future.result()
                if vol:
                    cache[b_id] = vol
                    encontrados_ol_search += 1
            except Exception:
                pass

            pbar_search.update(1)
            pbar_search.set_postfix({"Encontrados Busca": encontrados_ol_search})

            if (i + 1) % 25 == 0:
                salvar_cache(cache)

    salvar_cache(cache)
    print(f"\nBusca por título concluída. Novos livros validados e salvos: {encontrados_ol_search:,}")
else:
    print("Nenhum livro pendente para busca por título. Todos já estavam em cache!")


# ============================================================
# ETAPA 2.1: PREENCHER ISBNS PARA LIVROS ENCONTRADOS POR TÍTULO
# ============================================================

livros_precisando_isbn = []
for b_id, item in cache.items():
    if item and item.get("api_source") == "openlibrary_search":
        if not item.get("google_isbn13") and not item.get("google_isbn10"):
            livros_precisando_isbn.append((b_id, item.get("google_title", "")))

if livros_precisando_isbn:
    print(f"\n[INFO] Recuperando ISBNs para {len(livros_precisando_isbn):,} livros resgatados por título...")
    pbar_isbn = tqdm(total=len(livros_precisando_isbn), desc="Recuperando ISBNs")
    isbns_recuperados = 0

    def buscar_isbn_titulo(par):
        b_id, tit = par
        if not tit:
            return b_id, "", ""
        try:
            r = requests.get(
                OPEN_LIBRARY_SEARCH_URL,
                params={"title": tit, "limit": 1, "fields": "title,isbn"},
                headers=OPEN_LIBRARY_HEADERS,
                timeout=10
            )
            if r.status_code == 200:
                docs = r.json().get("docs", [])
                if docs and docs[0].get("isbn"):
                    isbns = docs[0]["isbn"]
                    i13 = next((str(x).strip() for x in isbns if len(re.sub(r"[^0-9X]", "", str(x).strip())) == 13), "")
                    i10 = next((str(x).strip() for x in isbns if len(re.sub(r"[^0-9X]", "", str(x).strip())) == 10), "")
                    return b_id, i13, i10
        except Exception:
            pass
        return b_id, "", ""

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_SEARCH) as ex:
        futures = {ex.submit(buscar_isbn_titulo, item): item for item in livros_precisando_isbn}
        for i, fut in enumerate(concurrent.futures.as_completed(futures)):
            try:
                b_id, i13, i10 = fut.result()
                if i13 or i10:
                    if b_id in cache and cache[b_id]:
                        cache[b_id]["google_isbn13"] = i13
                        cache[b_id]["google_isbn10"] = i10
                        isbns_recuperados += 1
            except Exception:
                pass
            pbar_isbn.update(1)
            pbar_isbn.set_postfix({"ISBNs OK": isbns_recuperados})
            if (i + 1) % 50 == 0:
                salvar_cache(cache)

    salvar_cache(cache)
    print(f"\nISBNs recuperados com sucesso: {isbns_recuperados:,}")
else:
    print("Todos os livros resgatados já possuem ISBN!")


# ============================================================
# ETAPA 2.2: ATUALIZAR METADADOS VIA LOTE USANDO OS ISBNS RECUPERADOS
# ============================================================

bibkeys_para_atualizar = {}
for b_id, item in cache.items():
    if item and item.get("api_source") == "openlibrary_search":
        isbn13 = item.get("google_isbn13", "").strip()
        isbn10 = item.get("google_isbn10", "").strip()
        if isbn13:
            bibkeys_para_atualizar.setdefault(f"ISBN:{isbn13}", []).append(b_id)
        elif isbn10:
            bibkeys_para_atualizar.setdefault(f"ISBN:{isbn10}", []).append(b_id)

if bibkeys_para_atualizar:
    print(f"\n[INFO] Atualizando metadados completos em lote para {len(bibkeys_para_atualizar):,} ISBNs recuperados...")
    chaves_lista = list(bibkeys_para_atualizar.keys())
    num_lotes_att = (len(chaves_lista) + OPEN_LIBRARY_BATCH_SIZE - 1) // OPEN_LIBRARY_BATCH_SIZE
    pbar_att = tqdm(range(num_lotes_att), desc="Atualizando Metadados em Lote")
    metadados_atualizados = 0

    for idx in pbar_att:
        lote = chaves_lista[idx * OPEN_LIBRARY_BATCH_SIZE : (idx + 1) * OPEN_LIBRARY_BATCH_SIZE]
        param_bibkeys = ",".join(lote)
        try:
            resp = requests.get(
                OPEN_LIBRARY_URL,
                params={"bibkeys": param_bibkeys, "format": "json", "jscmd": "data"},
                headers=OPEN_LIBRARY_HEADERS,
                timeout=TIMEOUT
            )
            if resp.status_code == 200:
                dados_resp = resp.json()
                for bk in lote:
                    if bk in dados_resp:
                        info_livro = dados_resp[bk]
                        for b_id in bibkeys_para_atualizar[bk]:
                            vol_completo = extrair_volume_openlibrary(info_livro, b_id, metodo_busca="openlibrary_batch_recovered_isbn")
                            if not vol_completo.get("google_isbn13"):
                                vol_completo["google_isbn13"] = cache[b_id].get("google_isbn13", "")
                            if not vol_completo.get("google_isbn10"):
                                vol_completo["google_isbn10"] = cache[b_id].get("google_isbn10", "")
                            cache[b_id] = vol_completo
                            metadados_atualizados += 1
        except Exception:
            pass

        time.sleep(OPEN_LIBRARY_DELAY)
        pbar_att.set_postfix({"Metadados Atualizados": metadados_atualizados})

    salvar_cache(cache)
    print(f"\nMetadados detalhados atualizados com sucesso: {metadados_atualizados:,}")
else:
    print("Nenhum metadado pendente para atualização em lote.")


# ============================================================
# ETAPA 2.3: COLETAR NOTAS DA OPEN LIBRARY (RATINGS)
# ============================================================

isbn_to_bids_rating = {}
for b_id, item in cache.items():
    if item and (item.get("google_average_rating") is None or item.get("google_average_rating") == ""):
        i13 = re.sub(r"[^0-9X]", "", str(item.get("google_isbn13", "")).strip().upper())
        i10 = re.sub(r"[^0-9X]", "", str(item.get("google_isbn10", "")).strip().upper())
        if len(i13) == 13:
            isbn_to_bids_rating.setdefault(i13, []).append(b_id)
        elif len(i10) == 10:
            isbn_to_bids_rating.setdefault(i10, []).append(b_id)

if isbn_to_bids_rating:
    all_isbns_rating = list(isbn_to_bids_rating.keys())
    print(f"\n[INFO] Coletando notas da Open Library para {len(all_isbns_rating):,} ISBNs...")
    batch_size_rat = 40
    batches_rat = [all_isbns_rating[i : i + batch_size_rat] for i in range(0, len(all_isbns_rating), batch_size_rat)]
    pbar_rat = tqdm(total=len(batches_rat), desc="Coletando Notas da Open Library")
    notas_coletadas = 0

    def fetch_batch_ratings(batch):
        q_isbns = " OR ".join(batch)
        try:
            r = requests.get(
                OPEN_LIBRARY_SEARCH_URL,
                params={"q": f"isbn:({q_isbns})", "fields": "key,isbn,ratings_average,ratings_count", "limit": 60},
                headers=OPEN_LIBRARY_HEADERS,
                timeout=15
            )
            if r.status_code == 200:
                return r.json().get("docs", [])
        except Exception:
            pass
        return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_SEARCH) as ex:
        futures = {ex.submit(fetch_batch_ratings, b): b for b in batches_rat}
        for fut in concurrent.futures.as_completed(futures):
            docs = fut.result()
            for doc in docs:
                avg_rating = doc.get("ratings_average")
                count_rating = doc.get("ratings_count")
                if avg_rating is not None:
                    doc_isbns = [re.sub(r"[^0-9X]", "", str(x).strip().upper()) for x in doc.get("isbn", [])]
                    matched_bids = set()
                    for i_code in doc_isbns:
                        if i_code in isbn_to_bids_rating:
                            for b_id in isbn_to_bids_rating[i_code]:
                                matched_bids.add(b_id)
                    for b_id in matched_bids:
                        if b_id in cache and cache[b_id]:
                            cache[b_id]["google_average_rating"] = round(float(avg_rating), 2)
                            cache[b_id]["google_ratings_count"] = int(count_rating or 0)
                            notas_coletadas += 1
            pbar_rat.update(1)
            pbar_rat.set_postfix({"Notas Encontradas": notas_coletadas})

    salvar_cache(cache)
    print(f"\nNotas da Open Library atribuídas com sucesso: {notas_coletadas:,}")
else:
    print("Todas as notas da Open Library já foram coletadas anteriormente!")


# ============================================================
# ETAPA 3: FALLBACK GOOGLE BOOKS API (OPCIONAL)
# ============================================================

print()
print("=" * 70)
print("ETAPA 3: FALLBACK GOOGLE BOOKS API")
print("=" * 70)

livros_pendentes_google = []
for _, row in df_books.iterrows():
    b_id = str(row["book_id"]).strip()
    if ja_coletado(b_id):
        continue  # PULA LIVROS JÁ COLETADOS
    livros_pendentes_google.append(row)

print(f"Livros ainda não enriquecidos: {len(livros_pendentes_google):,}")

encontrados_google = 0

if not livros_pendentes_google:
    print("\n[INFO] Todos os livros já foram resolvidos nas etapas anteriores!")
else:
    if not API_KEY:
        print("\n[AVISO] GOOGLE_BOOKS_API_KEY não informada no ambiente.")
        print("Tentando consulta pública (sujeita a cota diária do IP no Google)...")
    else:
        print("\n[INFO] Usando chave configurada em GOOGLE_BOOKS_API_KEY.")

    pbar_google = tqdm(total=len(livros_pendentes_google), desc="Consultando Google Books")
    rate_limit_atingido = False

    def processar_livro_google(row):
        global rate_limit_atingido
        if rate_limit_atingido:
            return None, None, False
        b_id = str(row["book_id"]).strip()
        vol, rate_limited = buscar_livro_google(row, api_key=API_KEY)
        if rate_limited:
            rate_limit_atingido = True
        time.sleep(0.05)
        return b_id, vol, rate_limited

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(processar_livro_google, row): row for row in livros_pendentes_google}
        for i, fut in enumerate(concurrent.futures.as_completed(futures)):
            try:
                b_id, vol, rate_limited = fut.result()
                if rate_limited and rate_limit_atingido:
                    print("\n\n" + "!" * 70)
                    print("[AVISO] GOOGLE BOOKS API RETORNOU HTTP 429 (QUOTA DIÁRIA EXCEDIDA)")
                    print("A cota diária de 1.000 requisições da chave no Google Cloud foi atingida.")
                    print("!" * 70 + "\n")
                    for f in futures:
                        f.cancel()
                    break
                if vol:
                    cache[b_id] = vol
                    encontrados_google += 1
            except Exception:
                pass

            pbar_google.update(1)
            pbar_google.set_postfix({"Encontrados Google": encontrados_google})

            if (i + 1) % 25 == 0:
                salvar_cache(cache)

    salvar_cache(cache)
    print(f"\nEtapa Google Books concluída. Livros enriquecidos pelo Google: {encontrados_google:,}")


# ============================================================
# ETAPA 4: CONSOLIDAR DATASETS FINAIS
# ============================================================

print()
print("=" * 70)
print("CONSOLIDANDO DATASET ENRIQUECIDO")
print("=" * 70)

resultados = []
for _, row in df_books.iterrows():
    b_id = str(row["book_id"]).strip()
    val = cache.get(b_id)
    if val:
        val["goodreads_book_id"] = b_id
        resultados.append(val)

df_enriquecido_api = pd.DataFrame(resultados)

if len(df_enriquecido_api) > 0:
    df_enriquecido_api = df_enriquecido_api.drop_duplicates(subset=["goodreads_book_id"])
    df_enriquecido_api["goodreads_book_id"] = df_enriquecido_api["goodreads_book_id"].astype(str).str.strip()
    df_books["book_id"] = df_books["book_id"].astype(str).str.strip()

    # Normalizar tipos de colunas para PyArrow / Parquet
    for col in ["google_page_count", "google_average_rating", "google_ratings_count"]:
        if col in df_enriquecido_api.columns:
            df_enriquecido_api[col] = pd.to_numeric(df_enriquecido_api[col], errors="coerce")

    for col in ["google_public_domain", "google_ebook_available"]:
        if col in df_enriquecido_api.columns:
            df_enriquecido_api[col] = df_enriquecido_api[col].astype(bool)

    for col in df_enriquecido_api.columns:
        if col not in ["google_page_count", "google_average_rating", "google_ratings_count", "google_public_domain", "google_ebook_available"]:
            df_enriquecido_api[col] = df_enriquecido_api[col].fillna("").astype(str)

    df_books_completo = df_books.merge(
        df_enriquecido_api,
        left_on="book_id",
        right_on="goodreads_book_id",
        how="left"
    )
else:
    df_books_completo = df_books.copy()

# Salvar arquivo intermediário de livros enriquecidos
df_enriquecido_api.to_parquet(ARQUIVO_GOOGLE_BOOKS, index=False)
print(f"Dados enriquecidos salvos em:\n{ARQUIVO_GOOGLE_BOOKS}")

# Cruzar com reviews
print("\nCruzando com reviews...")
df_reviews["book_id"] = df_reviews["book_id"].astype(str).str.strip()
df_books_completo["book_id"] = df_books_completo["book_id"].astype(str).str.strip()

df_final = df_reviews.merge(
    df_books_completo,
    on="book_id",
    how="left",
    suffixes=("", "_book")
)

# Salvar dataset final
df_final.to_parquet(ARQUIVO_FINAL, index=False)
print(f"Dataset final salvo em:\n{ARQUIVO_FINAL}")


# ============================================================
# ESTATÍSTICAS FINAIS
# ============================================================

total_livros = len(df_books)
total_enriquecidos = len(df_enriquecido_api)
total_reviews = len(df_reviews)

reviews_com_enriquecimento = 0
if "google_volume_id" in df_final.columns:
    reviews_com_enriquecimento = df_final["google_volume_id"].notna().sum()

taxa_livros = (total_enriquecidos / total_livros * 100) if total_livros > 0 else 0
taxa_reviews = (reviews_com_enriquecimento / total_reviews * 100) if total_reviews > 0 else 0

fontes = df_enriquecido_api["api_source"].value_counts().to_dict() if "api_source" in df_enriquecido_api.columns else {}

print()
print("=" * 70)
print("RESUMO DO ENRIQUECIMENTO")
print("=" * 70)
print(f"Total de livros Goodreads:         {total_livros:,}")
print(f"Livros enriquecidos com sucesso:    {total_enriquecidos:,} ({taxa_livros:.2f}%)")
for fonte, contagem in fontes.items():
    print(f"  - Fonte '{fonte}': {contagem:,} livros")
print(f"Total de reviews:                  {total_reviews:,}")
print(f"Reviews com metadados externos:    {reviews_com_enriquecimento:,} ({taxa_reviews:.2f}%)")
print("=" * 70)
print("PROCESSO CONCLUÍDO COM SUCESSO!")
print("=" * 70)

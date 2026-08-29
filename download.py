import gzip
import json
import random
import os
import pandas as pd
# Arquivo original do Goodreads
ARQUIVO_ORIGINAL = (
    "/media/joao/8cb5cf8c-261d-4ef9-965c-485a78e63f21/kaggle/"
    "goodreads_reviews_dedup.json.gz"
)
# Pasta do projeto
PASTA_PROJETO = "/home/joao/Projetos/Book-Review-Data-Analysis"
PASTA_PROCESSED = os.path.join(PASTA_PROJETO, "processed")
ARQUIVO_SAIDA = os.path.join(
    PASTA_PROCESSED,
    "goodreads_reviews_100k.parquet"
)
# Quantidade de reviews desejada
TAMANHO_AMOSTRA = 10_000
# Seed para tornar o resultado reproduzível
SEED = 42
os.makedirs(PASTA_PROCESSED, exist_ok=True)
random.seed(SEED)
reviews = []
print("Iniciando leitura do dataset...")
print("Isso pode levar alguns minutos, pois o arquivo possui milhões de reviews.\n")
with gzip.open(ARQUIVO_ORIGINAL, "rt", encoding="utf-8") as arquivo:
    for i, linha in enumerate(arquivo):
        try:
            review = json.loads(linha)
        except json.JSONDecodeError:
            print(f"Erro ao ler a linha {i}. Pulando...")
            continue
        if len(reviews) < TAMANHO_AMOSTRA:
            reviews.append(review)
        else:
            indice = random.randint(0, i)
            if indice < TAMANHO_AMOSTRA:
                reviews[indice] = review
        if (i + 1) % 1_000_000 == 0:
            print(f"{i + 1:,} reviews processadas...")
print("\nConvertendo amostra para DataFrame...")
df = pd.DataFrame(reviews)
print("Salvando dataset processado...")
df.to_parquet(
    ARQUIVO_SAIDA,
    index=False
)
print("\n========================================")
print("AMOSTRAGEM CONCLUÍDA")
print("========================================")
print(f"Reviews selecionadas: {len(df):,}")
print(f"Arquivo salvo em:")
print(ARQUIVO_SAIDA)
print("\nDimensões:")
print(df.shape)
print("\nColunas:")
print(df.columns.tolist())
print("\nPrimeiras reviews:")
print(df.head())

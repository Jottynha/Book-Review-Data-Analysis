import os
import pandas as pd
import matplotlib.pyplot as plt
PASTA_PROJETO = "/home/joao/Projetos/Book-Review-Data-Analysis"
ARQUIVO_ENTRADA = os.path.join(
    PASTA_PROJETO,
    "processed",
    "goodreads_reviews_100k.parquet"
)
ARQUIVO_SAIDA = os.path.join(
    PASTA_PROJETO,
    "processed",
    "goodreads_reviews_clean.parquet"
)
PASTA_RESULTADOS = os.path.join(
    PASTA_PROJETO,
    "results"
)
PASTA_GRAFICOS = os.path.join(
    PASTA_RESULTADOS,
    "figures"
)
ARQUIVO_RELATORIO = os.path.join(
    PASTA_RESULTADOS,
    "eda_summary.txt"
)
os.makedirs(PASTA_RESULTADOS, exist_ok=True)
os.makedirs(PASTA_GRAFICOS, exist_ok=True)
print("=" * 60)
print("PARTE 2 - LIMPEZA E ANÁLISE EXPLORATÓRIA")
print("=" * 60)
print("\nLendo dataset...")
df = pd.read_parquet(ARQUIVO_ENTRADA)
print(f"Reviews carregadas: {len(df):,}")
print(f"Colunas: {len(df.columns)}")
df_clean = df.copy()
print("\n" + "=" * 60)
print("INFORMAÇÕES INICIAIS")
print("=" * 60)
print(df_clean.info())
print("\n" + "=" * 60)
print("VALORES AUSENTES")
print("=" * 60)
missing = df_clean.isnull().sum()
print(missing[missing > 0])
print("\n" + "=" * 60)
print("DUPLICATAS")
print("=" * 60)
duplicatas_antes = df_clean.duplicated().sum()
print(f"Duplicatas encontradas: {duplicatas_antes:,}")
if duplicatas_antes > 0:
    df_clean = df_clean.drop_duplicates()
print(f"Reviews após remoção: {len(df_clean):,}")
print("\n" + "=" * 60)
print("CONVERSÃO DE TIPOS")
print("=" * 60)
df_clean["rating"] = pd.to_numeric(
    df_clean["rating"],
    errors="coerce"
)
df_clean["n_votes"] = pd.to_numeric(
    df_clean["n_votes"],
    errors="coerce"
)
df_clean["n_comments"] = pd.to_numeric(
    df_clean["n_comments"],
    errors="coerce"
)
print("\n" + "=" * 60)
print("LIMPEZA DAS RESENHAS")
print("=" * 60)
# Garantir que review_text seja string
df_clean["review_text"] = df_clean["review_text"].fillna("").astype(str)
# Remove espaços no início/fim
df_clean["review_text"] = df_clean["review_text"].str.strip()
# Identificar e remover reviews sem texto
sem_texto = (df_clean["review_text"] == "").sum()
print(f"Reviews sem texto: {sem_texto:,}")
df_clean = df_clean[df_clean["review_text"] != ""].copy()
print(f"Reviews após remoção: {len(df_clean):,}")
# ============================================================
# 6. CRIAÇÃO DE VARIÁVEIS SOBRE O TEXTO
# ============================================================

print("\nCriando variáveis de texto...")

# Quantidade de caracteres
df_clean["review_length"] = df_clean["review_text"].str.len()

# Quantidade aproximada de palavras
df_clean["word_count"] = (
    df_clean["review_text"]
    .str.split()
    .str.len()
)


# ============================================================
# 7. TRATAMENTO DOS RATINGS
# ============================================================

print("\n" + "=" * 60)
print("RATINGS")
print("=" * 60)

print("Valores encontrados:")

print(
    df_clean["rating"]
    .value_counts()
    .sort_index()
)


# Manter somente ratings válidos de 1 a 5
df_clean = df_clean[
    df_clean["rating"].between(1, 5)
].copy()

print("\nReviews após validação dos ratings:")
print(f"{len(df_clean):,}")


# ============================================================
# 8. DATAS
# ============================================================

print("\n" + "=" * 60)
print("TRATAMENTO DAS DATAS")
print("=" * 60)

colunas_data = [
    "date_added",
    "date_updated",
    "read_at",
    "started_at"
]

for coluna in colunas_data:

    df_clean[coluna] = pd.to_datetime(
        df_clean[coluna],
        errors="coerce",
        utc=True
    )

    print(
        f"{coluna}: "
        f"{df_clean[coluna].notna().sum():,} valores válidos"
    )


# ============================================================
# 9. VARIÁVEIS TEMPORAIS
# ============================================================

df_clean["read_year"] = df_clean["read_at"].dt.year

df_clean["read_month"] = df_clean["read_at"].dt.month

df_clean["read_weekday"] = df_clean["read_at"].dt.day_name()


# ============================================================
# 10. ESTATÍSTICAS DESCRITIVAS
# ============================================================

print("\n" + "=" * 60)
print("ESTATÍSTICAS DESCRITIVAS")
print("=" * 60)

estatisticas = df_clean[
    [
        "rating",
        "n_votes",
        "n_comments",
        "review_length",
        "word_count"
    ]
].describe()

print(estatisticas)


# ============================================================
# 11. DISTRIBUIÇÃO DOS RATINGS
# ============================================================

plt.figure(figsize=(8, 5))

df_clean["rating"].value_counts().sort_index().plot(
    kind="bar"
)

plt.title("Distribuição das avaliações")
plt.xlabel("Rating")
plt.ylabel("Quantidade de reviews")
plt.xticks(rotation=0)

plt.tight_layout()

plt.savefig(
    os.path.join(
        PASTA_GRAFICOS,
        "rating_distribution.png"
    ),
    dpi=150
)

plt.close()


# ============================================================
# 12. DISTRIBUIÇÃO DO TAMANHO DAS RESENHAS
# ============================================================

plt.figure(figsize=(8, 5))

df_clean["word_count"].clip(
    upper=df_clean["word_count"].quantile(0.99)
).plot(
    kind="hist",
    bins=50
)

plt.title("Distribuição do tamanho das resenhas")
plt.xlabel("Número de palavras")
plt.ylabel("Quantidade de reviews")

plt.tight_layout()

plt.savefig(
    os.path.join(
        PASTA_GRAFICOS,
        "review_length_distribution.png"
    ),
    dpi=150
)

plt.close()


# ============================================================
# 13. DISTRIBUIÇÃO DOS VOTOS
# ============================================================

plt.figure(figsize=(8, 5))

df_clean["n_votes"].clip(
    upper=df_clean["n_votes"].quantile(0.99)
).plot(
    kind="hist",
    bins=50
)

plt.title("Distribuição dos votos das resenhas")
plt.xlabel("Número de votos")
plt.ylabel("Quantidade de reviews")

plt.tight_layout()

plt.savefig(
    os.path.join(
        PASTA_GRAFICOS,
        "votes_distribution.png"
    ),
    dpi=150
)

plt.close()


# ============================================================
# 14. DISTRIBUIÇÃO DOS COMENTÁRIOS
# ============================================================

plt.figure(figsize=(8, 5))

df_clean["n_comments"].clip(
    upper=df_clean["n_comments"].quantile(0.99)
).plot(
    kind="hist",
    bins=50
)

plt.title("Distribuição dos comentários")
plt.xlabel("Número de comentários")
plt.ylabel("Quantidade de reviews")

plt.tight_layout()

plt.savefig(
    os.path.join(
        PASTA_GRAFICOS,
        "comments_distribution.png"
    ),
    dpi=150
)

plt.close()


# ============================================================
# 15. RELAÇÕES IMPORTANTES
# ============================================================

print("\n" + "=" * 60)
print("RELAÇÕES ENTRE VARIÁVEIS")
print("=" * 60)

correlacoes = df_clean[
    [
        "rating",
        "n_votes",
        "n_comments",
        "review_length",
        "word_count"
    ]
].corr()

print(correlacoes)


# ============================================================
# 16. RATING MÉDIO POR TAMANHO DA RESENHA
# ============================================================

df_clean["word_count_group"] = pd.cut(
    df_clean["word_count"],
    bins=[
        0,
        50,
        100,
        250,
        500,
        1000,
        float("inf")
    ],
    labels=[
        "0-50",
        "51-100",
        "101-250",
        "251-500",
        "501-1000",
        "1000+"
    ]
)

rating_por_tamanho = (
    df_clean
    .groupby("word_count_group", observed=True)["rating"]
    .agg(["mean", "count"])
)

print("\nRating por tamanho da resenha:")
print(rating_por_tamanho)


# ============================================================
# 17. USUÁRIOS
# ============================================================

print("\n" + "=" * 60)
print("ANÁLISE DOS USUÁRIOS")
print("=" * 60)

usuarios = df_clean.groupby("user_id").agg(
    reviews=("review_id", "count"),
    rating_medio=("rating", "mean"),
    tamanho_medio_resenha=("word_count", "mean"),
    votos_medios=("n_votes", "mean"),
    comentarios_medios=("n_comments", "mean")
)

print("\nQuantidade de usuários:")
print(f"{len(usuarios):,}")

print("\nUsuários com mais reviews:")

print(
    usuarios
    .sort_values("reviews", ascending=False)
    .head(10)
)


# ============================================================
# 18. SALVAR DATASET LIMPO
# ============================================================

print("\n" + "=" * 60)
print("SALVANDO DATASET")
print("=" * 60)

df_clean.to_parquet(
    ARQUIVO_SAIDA,
    index=False
)

print(f"Dataset salvo em:")
print(ARQUIVO_SAIDA)


# ============================================================
# 19. GERAR RELATÓRIO
# ============================================================

with open(
    ARQUIVO_RELATORIO,
    "w",
    encoding="utf-8"
) as arquivo:

    arquivo.write(
        "RELATÓRIO DE ANÁLISE EXPLORATÓRIA - GOODREADS\n"
    )

    arquivo.write("=" * 60 + "\n\n")

    arquivo.write(
        f"Reviews originais: {len(df):,}\n"
    )

    arquivo.write(
        f"Reviews após limpeza: {len(df_clean):,}\n"
    )

    arquivo.write(
        f"Usuários: {df_clean['user_id'].nunique():,}\n"
    )

    arquivo.write(
        f"Livros: {df_clean['book_id'].nunique():,}\n"
    )

    arquivo.write(
        f"Reviews duplicadas removidas: "
        f"{duplicatas_antes:,}\n"
    )

    arquivo.write(
        f"Reviews sem texto removidas: "
        f"{sem_texto:,}\n\n"
    )

    arquivo.write("ESTATÍSTICAS DESCRITIVAS\n")
    arquivo.write("-" * 60 + "\n")

    arquivo.write(
        estatisticas.to_string()
    )

    arquivo.write("\n\nCORRELAÇÕES\n")
    arquivo.write("-" * 60 + "\n")

    arquivo.write(
        correlacoes.to_string()
    )

    arquivo.write("\n\nRATING POR TAMANHO DA RESENHA\n")
    arquivo.write("-" * 60 + "\n")

    arquivo.write(
        rating_por_tamanho.to_string()
    )


# ============================================================
# FINAL
# ============================================================

print("\n" + "=" * 60)
print("PARTE 2 CONCLUÍDA")
print("=" * 60)

print(f"\nDataset limpo:")
print(ARQUIVO_SAIDA)

print(f"\nGráficos:")
print(PASTA_GRAFICOS)

print(f"\nRelatório:")
print(ARQUIVO_RELATORIO)

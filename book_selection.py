from pathlib import Path
import pandas as pd

# Diretório onde está o book_selection.py
BASE_DIR = Path(__file__).resolve().parent

ARQUIVO = (
    BASE_DIR
    / "processed"
    / "goodreads_reviews_clean.parquet"
)

SAIDA = (
    BASE_DIR
    / "processed"
    / "livros_para_enriquecimento.csv"
)

N_LIVROS = 500

# Verifica se o arquivo existe
if not ARQUIVO.exists():
    raise FileNotFoundError(
        f"Arquivo não encontrado:\n{ARQUIVO}"
    )

# Lê a base
df = pd.read_parquet(ARQUIVO)

print(f"Reviews: {len(df):,}")
print(f"Livros únicos: {df['book_id'].nunique():,}")

# Estatísticas por livro
livros = (
    df.groupby("book_id")
    .agg(
        n_reviews=("review_id", "count"),
        rating_medio=("rating", "mean"),
        rating_mediano=("rating", "median"),
        n_usuarios=("user_id", "nunique"),
        n_votes=("n_votes", "sum"),
        n_comments=("n_comments", "sum"),
        word_count_medio=("word_count", "mean")
    )
    .reset_index()
)

# Seleciona os 500 livros com mais reviews
livros = livros.sort_values(
    "n_reviews",
    ascending=False
)

livros_amostra = livros.head(N_LIVROS)

# Salva
livros_amostra.to_csv(
    SAIDA,
    index=False,
    encoding="utf-8"
)

print("\n========== RESULTADO ==========")
print(f"Livros selecionados: {len(livros_amostra):,}")
print(f"Arquivo salvo em:\n{SAIDA}")

print("\nPrimeiros livros:")
print(livros_amostra.head(10).to_string(index=False))

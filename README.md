# Book Review Data Analysis

Projeto de Ciência de Dados (CEFET-MG) para analisar o comportamento de leitores a partir de 10.000 resenhas do Goodreads, enriquecidas com metadados da Open Library e do Google Books.

## Estado atual

| Indicador | Resultado |
| --- | ---: |
| Reviews Goodreads | 10.000 |
| Livros únicos | 8.916 |
| Livros com match Open Library | 7.654 (85,85%) |
| Livros com match Google Books | 4.424 (49,62%) |
| Livros com confiança `MUITO_ALTO`, `ALTO` ou `MEDIO` | 7.031 (78,86%) |
| Reviews com confiança adequada | 8.033 |
| Reviews com ambas as APIs e confiança adequada | 4.293 |

Os números de reviews são maiores que os de livros porque um mesmo livro pode ter várias resenhas.

### Bases recomendadas

- Análise comportamental geral: `processed/goodreads_reviews_validated_100k.parquet`, filtrando `match_status` em `MUITO_ALTO`, `ALTO` e `MEDIO`.
- Comparação entre as APIs: além do filtro de confiança, exigir `openlibrary_match == True` e `google_match == True`.
- Auditoria: `processed/matches_para_revisao.csv` e `processed/relatorio_qualidade_matches.txt`.

```python
import pandas as pd

df = pd.read_parquet("processed/goodreads_reviews_validated_100k.parquet")
df = df[
    df["openlibrary_match"]
    & df["google_match"]
    & df["match_status"].isin(["MUITO_ALTO", "ALTO", "MEDIO"])
]
```

## Pipeline

```text
01_amostra_reviews.py
    -> goodreads_reviews_100k.parquet
02_cruzar_reviews.py
    -> associação inicial entre reviews e livros
03_cruzar_openlibrary.py
    -> goodreads_books_openlibrary_100k.parquet
04_cruzar_google_books.py
    -> goodreads_books_google_books_100k.parquet
05_validar_matches.py
    -> goodreads_books_validated_100k.parquet
    -> goodreads_reviews_validated_100k.parquet
```

O validador cruza as duas APIs por `book_id`, compara títulos, autores, anos e ISBNs, e prioriza um ISBN compartilhado quando há divergência de título por tradução ou edição. Matches fracos ou conflitantes ficam registrados para revisão manual.

## Arquivos principais

```text
processed/
├── goodreads_books_100k.parquet                 # livros Goodreads da amostra
├── goodreads_reviews_100k.parquet               # reviews Goodreads da amostra
├── goodreads_books_openlibrary_100k.parquet     # resultado Open Library
├── goodreads_books_google_books_100k.parquet    # resultado Google Books
├── goodreads_books_validated_100k.parquet       # livros com confiança calculada
├── goodreads_reviews_validated_100k.parquet     # base final para análise
├── openlibrary_cache.json                        # cache ativo da Open Library
├── google_books_cache_batches.json              # cache ativo do Google Books
├── matches_para_revisao.csv                     # matches baixos ou rejeitados
└── relatorio_qualidade_matches.*                # métricas da validação
```

Os caches são mantidos para evitar novas chamadas às APIs. Os arquivos intermediários e o gráfico de cobertura foram removidos por não serem necessários para a análise nem para a execução atual.

## Execução

```bash
python3 -m pip install -r requirements.txt
python3 01_amostra_reviews.py
python3 02_cruzar_reviews.py
python3 03_cruzar_openlibrary.py
python3 04_cruzar_google_books.py
python3 05_validar_matches.py
```

Para a etapa do Google Books, configure `GOOGLE_BOOKS_API_KEY` no ambiente ou em `.env`. As etapas externas reaproveitam os caches persistidos em `processed/`.

## Possibilidades de análise

- Relação entre a nota (`rating`) e o sentimento do texto da resenha.
- Efeito de tamanho, gênero e popularidade do livro sobre a nota.
- Relação entre extensão da resenha, votos de utilidade e comentários.
- Comparação entre avaliações e metadados disponíveis nas duas plataformas.
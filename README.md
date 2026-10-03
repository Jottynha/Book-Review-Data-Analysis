# Book Review Data Analysis

Projeto de Ciência de Dados (CEFET-MG) para analisar o comportamento de leitores
a partir de uma amostra de resenhas do Goodreads, enriquecida com metadados da
Open Library e do Google Books.

## Objetivo e escopo

O objetivo é construir uma base confiável para análise exploratória: preservar as
informações comportamentais do Goodreads, acrescentar características dos livros
e documentar a qualidade dos cruzamentos. O resultado não representa todos os
reviews do dataset original e não garante que todo match externo esteja correto.

O dataset original tem aproximadamente 15 milhões de resenhas. A amostra de
10.000 resenhas representa cerca de 0,067% desse volume. Esse tamanho permite
executar o fluxo completo em ambiente local, reduz custo e chamadas às APIs,
produz uma base manejável para EDA e torna o processamento reproduzível.

A seleção usa reservoir sampling em uma única passagem pelo arquivo compactado,
com `seed=42`. Assim, cada registro tem a mesma oportunidade de entrar na
amostra sem carregar os aproximadamente 15 milhões de registros na memória. A
amostra é adequada para exploração e validação do pipeline; conclusões sobre a
população inteira devem declarar essa limitação.

## Estado atual e encerramento do cruzamento

O cruzamento foi concluído para a amostra definida:

| Indicador | Resultado |
| --- | ---: |
| Reviews Goodreads | 10.000 |
| Livros únicos | 8.916 |
| Livros com match Open Library | 7.654 (85,85%) |
| Livros com match Google Books | 4.424 (49,62%) |
| Livros com ambas as APIs | 3.852 |
| Livros com confiança `MUITO_ALTO`, `ALTO` ou `MEDIO` | 7.031 (78,86%) |
| Reviews com confiança adequada | 8.033 |
| Reviews com ambas as APIs e confiança adequada | 4.293 |

Reviews são mais numerosos que livros porque um mesmo livro pode receber várias
resenhas. A diferença de cobertura entre as APIs não indica, sozinha, que uma
fonte seja melhor: os catálogos, identificadores, edições e políticas de
preenchimento são diferentes.

Esta fase de cruzamento está fechada. A análise principal seguirá somente com
reviews cujo `match_status` seja `MUITO_ALTO`, `ALTO` ou `MEDIO`. A base completa
será mantida para auditoria e análise de sensibilidade, mas não será usada como
referência dos resultados principais porque contém matches baixos, rejeitados e
livros sem match.

```python
import pandas as pd

df = pd.read_parquet("processed/goodreads_reviews_validated_100k.parquet")
df_confiavel = df[
    df["match_status"].isin(["MUITO_ALTO", "ALTO", "MEDIO"])
].copy()
```

Para comparar as duas APIs, além do filtro de confiança, exigir
`openlibrary_match == True` e `google_match == True`.

## Justificativa do enriquecimento

O Goodreads é a fonte principal das resenhas e avaliações, mas seus metadados
não são suficientes para todas as análises. O enriquecimento foi feito no nível
de livro, usando os `book_id` encontrados na amostra, para evitar consultas de
livros que não participam da análise e manter o vínculo com cada review.

- **Open Library:** prioriza ISBN-13 e ISBN-10 em lote; sem resultado, tenta
  título + autor e depois título.
- **Google Books:** funciona como fonte independente de conferência e de
  atributos editoriais; prioriza ISBN e encerra a busca quando encontra um
  resultado adequado.

As consultas usam cache persistente, retentativas para erros de rede e rate limit,
salvamento incremental e retomada. A ausência de match significa apenas que a
fonte ou a estratégia não retornou um registro utilizável.

## Validação dos matches

O validador compara, quando disponíveis:

- ISBN-13 e ISBN-10, com maior peso para os identificadores;
- similaridade de título após normalização de acentos e pontuação;
- interseção de autores;
- ano de publicação, com tolerância de até três anos para diferenças de edição;
- concordância entre Open Library e Google Books.

Cada livro recebe `match_status` em `MUITO_ALTO`, `ALTO`, `MEDIO`, `BAIXO`,
`REJEITAR` ou `SEM_MATCH`. Os casos fracos ou conflitantes ficam em
`processed/matches_para_revisao.csv`.

## Pipeline

```text
01_amostra_reviews.py
    -> goodreads_reviews_100k.parquet
    -> goodreads_books_100k.parquet
02_cruzar_reviews.py
    -> associação entre reviews e livros por book_id
03_cruzar_openlibrary.py
    -> goodreads_books_openlibrary_100k.parquet
04_cruzar_google_books.py
    -> goodreads_books_google_books_100k.parquet
05_validar_matches.py
    -> goodreads_books_validated_100k.parquet
    -> goodreads_reviews_validated_100k.parquet
    -> relatório de qualidade e fila de revisão
06_eda_amostra.py
    -> processed/eda_amostra/
    -> processed/eda_amostra_confiante/
```

O `02_cruzar_reviews.py` usa `left join` por `book_id` para manter as resenhas
mesmo quando algum metadado não estiver disponível. As etapas externas trabalham
somente com os livros únicos da amostra.

> **Nota:** o sufixo `100k` é histórico e foi mantido para não quebrar o pipeline.
> A execução documentada contém 10.000 reviews, não 100.000.

## EDA inicial da amostra

O script `06_eda_amostra.py` gera relatório Markdown, tabelas CSV e gráficos PNG
com perfil da amostra, usuários, notas, tamanho das reviews, votos, comentários,
livros mais resenhados, idiomas, páginas, formatos e gêneros aproximados.

Por padrão, a análise principal usa somente matches confiáveis:

```bash
python3 06_eda_amostra.py --only-confident
```

Esse comando grava em `processed/eda_amostra_confiante/`. Para gerar também o
recorte completo, usado como comparação e auditoria, execute:

```bash
python3 06_eda_amostra.py
```

As `popular_shelves` são rótulos sociais e podem se sobrepor. O script classifica
cada shelf como `genre_proxy`, `status_leitura`, `formato` ou `outro`. Apenas
`genre_proxy` entra em `top_generos_proxy.csv`; portanto, o resultado é um
indicador exploratório, não uma classificação editorial definitiva.

Principais artefatos da EDA:

- `resumo_geral.csv`: indicadores gerais;
- `distribuicao_notas.csv`: frequência e percentual das notas;
- `top_usuarios.csv` e `top_livros.csv`: concentrações da amostra;
- `top_shelves.csv`: shelves com sua classificação;
- `top_generos_proxy.csv`: somente shelves usadas como proxy de gênero;
- `shelves_por_tipo.csv`: distribuição das categorias de shelf;
- `relatorio_eda.md` e os gráficos `01_` a `05_`.

As relações observadas na EDA são descritivas e não demonstram causalidade.

## Arquivos principais

```text
processed/
├── goodreads_books_validated_100k.parquet
├── goodreads_reviews_validated_100k.parquet
├── openlibrary_cache.json
├── google_books_cache_batches.json
├── matches_para_revisao.csv
├── relatorio_qualidade_matches.*
├── eda_amostra/
└── eda_amostra_confiante/
```

## Execução completa

```bash
python3 -m pip install -r requirements.txt
python3 01_amostra_reviews.py
python3 02_cruzar_reviews.py
python3 03_cruzar_openlibrary.py
python3 04_cruzar_google_books.py
python3 05_validar_matches.py
python3 06_eda_amostra.py --only-confident
```

Para o Google Books, configure `GOOGLE_BOOKS_API_KEY` no ambiente ou em `.env`.
As etapas externas reaproveitam os caches persistidos em `processed/`.

## Próximas análises

- Relação entre nota e sentimento do texto da resenha.
- Efeito de tamanho, gênero e popularidade do livro sobre a nota.
- Relação entre extensão da resenha, votos de utilidade e comentários.
- Comparação entre usuários ocasionais e usuários mais ativos.

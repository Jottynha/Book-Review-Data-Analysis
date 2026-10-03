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
└── relatorio_qualidade_matches.*                # métricas da validação
    # Book Review Data Analysis

    Projeto de Ciência de Dados (CEFET-MG) para analisar o comportamento de leitores
    a partir de uma amostra de resenhas do Goodreads, enriquecida com metadados da
    Open Library e do Google Books.

    ## Objetivo e escopo

    O objetivo desta etapa é construir uma base confiável para a análise exploratória
    de resenhas: preservar as informações comportamentais do Goodreads, acrescentar
    características dos livros e documentar a qualidade dos cruzamentos. O resultado
    não é uma tentativa de copiar todo o dataset original nem uma garantia de que
    todo match externo está correto.

    O dataset original tem aproximadamente 15 milhões de resenhas. A amostra de
    10.000 resenhas representa cerca de 0,067% desse volume. Esse tamanho foi adotado
    por quatro razões:

    - permite executar o fluxo completo em ambiente local, incluindo leitura,
        transformação, chamadas externas e validação;
    - reduz substancialmente o custo, o tempo e a exposição a limites de requisição
        das APIs;
    - é suficiente para testar a metodologia e produzir uma base manejável para EDA,
        preservando a unidade de análise `review`;
    - torna possível repetir o processamento e auditar as decisões com os mesmos
        dados.

    A seleção usa reservoir sampling em uma única passagem pelo arquivo compactado,
    com `seed=42`. Assim, cada registro tem a mesma oportunidade de entrar na
    amostra sem exigir que os aproximadamente 15 milhões de registros sejam
    carregados simultaneamente na memória. A amostra é adequada para exploração e
    validação do pipeline; conclusões sobre a população inteira devem declarar essa
    limitação e, quando necessário, ser acompanhadas de uma estratégia estatística
    de representatividade.

    ## Estado atual e encerramento do cruzamento

    O cruzamento foi concluído para a amostra definida. Os resultados registrados no
    relatório de qualidade são:

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
    resenhas. A diferença entre as coberturas das APIs não indica, sozinha, que uma
    fonte seja melhor: as bases têm catálogos, identificadores, edições e políticas
    de preenchimento diferentes.

    Com isso, esta fase está fechada. A próxima etapa pode começar usando a base
    validada para EDA, análise de sentimento e investigação das relações entre
    avaliações, texto da resenha e características dos livros. Novos matches devem
    ser tratados como uma nova rodada metodológica, e não misturados silenciosamente
    com estes resultados.

    ### Bases recomendadas

    - Análise comportamental geral: `processed/goodreads_reviews_validated_100k.parquet`,
        filtrando `match_status` em `MUITO_ALTO`, `ALTO` e `MEDIO`.
    - Comparação entre as APIs: além do filtro de confiança, exigir
        `openlibrary_match == True` e `google_match == True`.
    - Auditoria e casos ambíguos: `processed/matches_para_revisao.csv` e
        `processed/relatorio_qualidade_matches.txt`.

    ```python
    import pandas as pd

    df = pd.read_parquet("processed/goodreads_reviews_validated_100k.parquet")
    df = df[
            df["openlibrary_match"]
            & df["google_match"]
            & df["match_status"].isin(["MUITO_ALTO", "ALTO", "MEDIO"])
    ]
    ```

    ## Justificativa do enriquecimento

    O Goodreads é a fonte principal das resenhas e das avaliações, mas os metadados
    dos livros não são suficientes para todas as análises pretendidas. O
    enriquecimento foi feito no nível de livro, usando os `book_id` encontrados na
    amostra, para evitar consultar APIs para livros que não participam da análise.
    Isso reduz chamadas e mantém o vínculo entre cada review e seu livro.

    As fontes têm papéis complementares:

    - **Open Library:** primeira fonte externa consultada, priorizando ISBN-13 e
        ISBN-10 em lote; para itens sem resultado, tenta título + autor e depois título.
    - **Google Books:** segunda fonte, útil como fonte independente de conferência e
        de atributos editoriais; prioriza busca por ISBN e encerra a consulta assim que
        encontra um resultado adequado.

    As consultas são protegidas por cache persistente, retentativas para erros de
    rede e rate limit, salvamento incremental e possibilidade de retomada. O cache
    evita repetir chamadas, reduz custo e cota consumida, além de tornar os
    resultados reproduzíveis dentro do estado registrado. A ausência de match não é
    tratada como ausência do livro no mundo: significa apenas que a estratégia ou a
    fonte não retornou um registro utilizável.

    ## Validação dos matches

    O cruzamento externo não aceita um resultado apenas porque uma API respondeu. O
    validador compara, quando disponíveis:

    - ISBN-13 e ISBN-10, dando maior peso aos identificadores;
    - similaridade de título após normalização de acentos e pontuação;
    - interseção de autores;
    - ano de publicação, com tolerância de até três anos para diferenças de edição;
    - concordância entre Open Library e Google Books.

    Essa combinação é necessária porque títulos podem variar por tradução, subtítulo,
    edição ou formatação. Um ISBN compartilhado pode confirmar a identidade mesmo
    quando os títulos divergem; por outro lado, título semelhante isolado não é
    evidência suficiente para aceitar automaticamente um match.

    Cada livro recebe `match_status` em `MUITO_ALTO`, `ALTO`, `MEDIO`, `BAIXO`,
    `REJEITAR` ou `SEM_MATCH`. Para a EDA, recomenda-se usar os três primeiros
    status. Os casos fracos ou conflitantes permanecem em
    `processed/matches_para_revisao.csv`, preservando a possibilidade de auditoria
    manual sem contaminar a base principal.

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
    ```

    O `02_cruzar_reviews.py` usa `left join` por `book_id` para manter as resenhas
    mesmo quando algum metadado de livro não estiver disponível. As etapas externas
    trabalham somente com os livros únicos da amostra. O validador final cruza os
    resultados das duas APIs e propaga os atributos de livro de volta para cada
    review.

    > **Nota sobre os nomes dos arquivos:** o sufixo `100k` é histórico e foi mantido
    > para não quebrar o pipeline. A execução documentada nesta versão contém 10.000
    > reviews, não 100.000.

    ## Arquivos principais

    ```text
    processed/
    ├── goodreads_books_100k.parquet                 # livros Goodreads da amostra
    ├── goodreads_reviews_100k.parquet               # reviews Goodreads da amostra
    ├── goodreads_books_openlibrary_100k.parquet     # resultado Open Library
    ├── goodreads_books_google_books_100k.parquet    # resultado Google Books
    ├── goodreads_books_validated_100k.parquet       # livros com confiança calculada
    ├── goodreads_reviews_validated_100k.parquet     # base final para análise
    ├── openlibrary_cache.json                        # cache da Open Library
    ├── google_books_cache_batches.json              # cache do Google Books
    ├── matches_para_revisao.csv                     # matches baixos ou rejeitados
    └── relatorio_qualidade_matches.*                # métricas da validação
    ```

    ## Execução

    ```bash
    python3 -m pip install -r requirements.txt
    python3 01_amostra_reviews.py
    python3 02_cruzar_reviews.py
    python3 03_cruzar_openlibrary.py
    python3 04_cruzar_google_books.py
    python3 05_validar_matches.py
    ```

    Para a etapa do Google Books, configure `GOOGLE_BOOKS_API_KEY` no ambiente ou em
    `.env`. As etapas externas reaproveitam os caches persistidos em `processed/`.

    ## EDA inicial da amostra

    O script `06_eda_amostra.py` inaugura a etapa de análise exploratória. Ele lê as
    bases validadas e gera em `processed/eda_amostra/` um relatório Markdown, tabelas
    CSV e gráficos PNG sobre:

    - perfil da amostra, livros únicos e concentração de reviews por livro;
    - usuários únicos e concentração de reviews por usuário;
    - distribuição das notas, incluindo a proporção de `rating == 0`;
    - tamanho das reviews, votos de utilidade e comentários;
    - livros mais resenhados e relação entre volume de reviews e nota média;
    - idiomas, páginas, formato e proporção de ebooks;
    - `popular_shelves` e gêneros aproximados.

    Execute:

    ```bash
    python3 06_eda_amostra.py
    ```

    Por padrão, a EDA usa todas as reviews da amostra para descrever a seleção
    observada. Para uma leitura mais conservadora, restrita aos livros com
    `match_status` `MUITO_ALTO`, `ALTO` ou `MEDIO`, execute:

    ```bash
    python3 06_eda_amostra.py --only-confident
    ```

    O segundo comando grava em `processed/eda_amostra_confiante/`, sem sobrescrever
    os resultados da amostra completa.

    As `popular_shelves` são rótulos sociais e podem misturar gênero, status de
    leitura, formato e outros interesses. Por isso, `top_generos_proxy.csv` é um
    indicador exploratório, não uma classificação editorial definitiva. As relações
    observadas na EDA são descritivas e não demonstram causalidade.

    ## Próxima etapa: análises

    - Relação entre a nota (`rating`) e o sentimento do texto da resenha.
    - Efeito de tamanho, gênero e popularidade do livro sobre a nota.
    - Relação entre extensão da resenha, votos de utilidade e comentários.
    - Comparação entre avaliações e metadados disponíveis nas duas plataformas.
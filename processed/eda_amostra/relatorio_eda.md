# EDA inicial da amostra

Filtro aplicado: todas as reviews da amostra.

## Perfil da base

- Reviews analisadas: 10,000
- Livros únicos analisados: 8,916
- Usuários únicos: 8,981
- Média de reviews por livro: 1.12
- Mediana de reviews por livro: 1
- Média de reviews por usuário: 1.11
- Mediana de reviews por usuário: 1

## Notas e comportamento

- Nota média: 3.76
- Mediana da nota: 4.0
- Percentual sem nota (`rating == 0`): 3.45%
- Média de caracteres por review: 700.0
- Mediana de caracteres por review: 342
- Média de votos: 1.14
- Média de comentários: 0.26

## Livros e metadados

- Livros com ano de publicação: 82.69%
- Livros com número de páginas: 84.17%
- Livros marcados como ebook: 22.31%
- Número mediano de páginas: 309
- Nota média Goodreads dos livros: 3.92

## Gêneros e shelves

As `popular_shelves` são usadas como proxy exploratório de gênero. Elas são
rótulos sociais dos usuários, podem se sobrepor e também incluem status de
leitura; por isso não devem ser interpretadas como uma classificação editorial
exclusiva. Os status e formatos mais óbvios foram removidos da tabela de proxy.

## Artefatos

- `resumo_geral.csv`: indicadores gerais da amostra.
- `distribuicao_notas.csv`: frequência e percentual de cada nota.
- `top_usuarios.csv`: usuários com mais reviews na amostra.
- `top_livros.csv`: livros com mais reviews e nota média.
- `top_shelves.csv` e `top_generos_proxy.csv`: rótulos mais frequentes.
- `01_` a `05_*.png`: gráficos para inspeção inicial.

## Perguntas para a próxima análise

- A nota está associada ao tamanho da review ou aos votos recebidos?
- A distribuição de notas muda entre livros com diferentes volumes de reviews?
- Usuários mais ativos avaliam de forma diferente dos usuários com uma única review?
- Quais gêneros/shelves concentram mais reviews e quais têm maior nota média?

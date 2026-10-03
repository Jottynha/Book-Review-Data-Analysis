# EDA inicial da amostra

Filtro aplicado: somente matches confiáveis.

## Perfil da base

- Reviews analisadas: 8,033
- Livros únicos analisados: 7,031
- Usuários únicos: 7,350
- Média de reviews por livro: 1.14
- Mediana de reviews por livro: 1
- Média de reviews por usuário: 1.09
- Mediana de reviews por usuário: 1

## Notas e comportamento

- Nota média: 3.73
- Mediana da nota: 4.0
- Percentual sem nota (`rating == 0`): 3.49%
- Média de caracteres por review: 686.7
- Mediana de caracteres por review: 334
- Média de votos: 1.10
- Média de comentários: 0.24

## Livros e metadados

- Livros com ano de publicação: 82.69%
- Livros com número de páginas: 84.17%
- Livros marcados como ebook: 22.31%
- Número mediano de páginas: 309
- Nota média Goodreads dos livros: 3.92

## Gêneros e shelves

As `popular_shelves` são rótulos sociais e podem se sobrepor. Nesta EDA,
cada shelf é classificada como `genre_proxy`, `status_leitura`, `formato`
ou `outro`. Apenas `genre_proxy` é usado na tabela de gêneros; a classificação
é exploratória e não equivale a uma taxonomia editorial definitiva.

- Associações livro-shelf classificadas como proxy de gênero: 538,835
- Associações livro-shelf classificadas como status de leitura: 148,726
- Associações livro-shelf classificadas como formato: 61,766

## Artefatos

- `resumo_geral.csv`: indicadores gerais da amostra.
- `distribuicao_notas.csv`: frequência e percentual de cada nota.
- `top_usuarios.csv`: usuários com mais reviews na amostra.
- `top_livros.csv`: livros com mais reviews e nota média.
- `top_shelves.csv`: todos os rótulos, com seu tipo de classificação.
- `top_generos_proxy.csv`: somente rótulos classificados como gênero.
- `shelves_por_tipo.csv`: quantidade de shelves por categoria.
- `01_` a `05_*.png`: gráficos para inspeção inicial.

## Perguntas para a próxima análise

- A nota está associada ao tamanho da review ou aos votos recebidos?
- A distribuição de notas muda entre livros com diferentes volumes de reviews?
- Usuários mais ativos avaliam de forma diferente dos usuários com uma única review?
- Quais gêneros/shelves concentram mais reviews e quais têm maior nota média?

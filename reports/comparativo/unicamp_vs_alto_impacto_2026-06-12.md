# Análise comparativa: Unicamp/IC vs. top-tier
Data: 2026-06-12
Conjunto de dados: `network.graphml` local

## 1. Motivação
- Esta análise compara a posição estrutural dos artigos vinculados à Unicamp/IC com artigos top-tier e artigos das instituições importantes selecionadas.
- Ela aprofunda a hipótese do relatório parcial: artigos top-tier tendem a ocupar a cauda de maior grau/impacto, enquanto a Unicamp aparece em faixa intermediária.
- Foram usadas distribuições, e não apenas médias, porque métricas de citação e centralidade têm cauda longa.

## 2. Metodologia
- Unicamp/IC foi identificada por `AFFILIATED_WITH` à instituição OpenAlex `I181391015`.
- `Top-tier ETL` foi identificado pela coluna `is_top_tier`, derivada apenas dos IDs encontrados em `data/responses_1/_top_cited_cs`.
- `Instituições importantes` foi identificado pela coluna `is_important_institution`, derivada dos IDs encontrados em `data/responses_1/by_institution`.
- O subdiretório `data/responses_1/by_institution/unicamp` foi excluído de `is_important_institution` para não misturar o conjunto de comparação com a própria Unicamp.
- Artigos que pertencem simultaneamente a Unicamp/IC e top-tier foram mantidos no grupo Unicamp para evitar sobreposição.
- As métricas comparadas foram in-degree, PageRank, taxa de citação por idade e citações brutas.
- Em vez de teste paramétrico, reporta-se distância KS empírica entre distribuições; valores maiores indicam separação mais forte.

## 3. Resultados
Resumo das distribuições:

| Grupo | Métrica | n | Média | Mediana | P90 | P99 |
| --- | --- | --- | --- | --- | --- | --- |
| Unicamp/IC | in_degree | 1.885 | 11.44 | 4 | 22 | 116 |
| Unicamp/IC | pagerank | 1.885 | 1.954e-05 | 1.111e-05 | 2.982e-05 | 0.0001177 |
| Unicamp/IC | citation_rate_by_age | 1.885 | 4.046 | 1.905 | 7.462 | 32.88 |
| Unicamp/IC | cited_by_count | 1.885 | 51.81 | 22 | 92 | 422 |
| Top-tier ETL | in_degree | 298 | 171.1 | 98 | 334 | 1212 |
| Top-tier ETL | pagerank | 298 | 0.0003592 | 0.0001847 | 0.0008871 | 0.002264 |
| Top-tier ETL | citation_rate_by_age | 298 | 1030 | 556.8 | 2062 | 6292 |
| Top-tier ETL | cited_by_count | 298 | 1.747e+04 | 1.162e+04 | 3.146e+04 | 9.586e+04 |
| Instituições importantes | in_degree | 7.142 | 8.054 | 2 | 20 | 84 |
| Instituições importantes | pagerank | 7.142 | 2.249e-05 | 7.909e-06 | 3.35e-05 | 0.0002668 |
| Instituições importantes | citation_rate_by_age | 7.142 | 23.39 | 6.3 | 51 | 292.1 |
| Instituições importantes | cited_by_count | 7.142 | 318.2 | 73 | 740 | 4262 |
| Demais artigos | in_degree | 39.871 | 6.407 | 2 | 15 | 70 |
| Demais artigos | pagerank | 39.871 | 1.744e-05 | 7.724e-06 | 2.582e-05 | 0.0001735 |
| Demais artigos | citation_rate_by_age | 39.871 | 14.7 | 4.167 | 30.62 | 170.7 |
| Demais artigos | cited_by_count | 39.871 | 219 | 50 | 424 | 3068 |

Distância KS empírica:

| Métrica | Unicamp vs top-tier ETL | Unicamp vs instituições importantes | Unicamp vs demais |
| --- | --- | --- | --- |
| in_degree | 0.7913 | 0.1594 | 0.183 |
| pagerank | 0.794 | 0.2261 | 0.252 |
| citation_rate_by_age | 0.9973 | 0.38 | 0.2781 |
| cited_by_count | 0.9989 | 0.3816 | 0.2882 |

Leitura dos resultados:

- A separação entre Unicamp e top-tier é muito forte em citações brutas (KS=0.999) e taxa normalizada por idade (KS=0.997). Isso mostra que o grupo top-tier não é apenas mais antigo ou mais numeroso: ele ocupa uma cauda de impacto distinta.
- A diferença de PageRank entre Unicamp e top-tier (KS=0.794) é menor que a de citações, mas ainda alta. Esse é um fato não trivial: parte da distância entre os grupos permanece mesmo quando o impacto é propagado pela estrutura de quem cita quem.
- As instituições importantes ficam mais próximas da Unicamp que o top-tier em todas as métricas KS. Isso sugere que a comparação institucional é um problema diferente da comparação contra artigos extremos: ela mede posição relativa em uma elite institucional ampla, não apenas proximidade da cauda de maior impacto.

## 4. Problemas encontrados
- As marcações `is_top_tier` e `is_important_institution` dependem da disponibilidade local do symlink `data/responses_1`; nesta execução ele apontou para `/home/debian/projeto_ruben/responses_1/`.
- A versão atual não aplica Mann-Whitney/KS com p-valor; ela reporta a distância KS como tamanho de efeito exploratório.
- Artigos de instituições importantes que também são top-tier entram no grupo top-tier, para manter os grupos disjuntos.

## 5. Importância e interpretação
- A comparação separa impacto bruto, impacto propagado e impacto normalizado por idade, evitando conclusões baseadas apenas em contagem de citações.
- Se a distância KS de PageRank for alta, isso indica que a diferença não é só volume de citações, mas posição estrutural na rede.
- Esta análise deve ser repetida quando `A_inter` também for materializado em cache.

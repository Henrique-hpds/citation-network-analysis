# Análise global: GraphML final
Data: 2026-06-12
Conjunto de dados: `network.graphml` local (49196 artigos, 385529 arestas `CITES`)

## 1. Motivação
- Esta análise inicia a camada de relatórios do agente definida em `AGENTS.md`, usando o GraphML final como fonte reprodutível.
- Ela responde às perguntas centrais da proposta: estrutura do grafo, presença da Unicamp/IC, artigos estruturalmente centrais e comunidades dominantes.
- Foram escolhidas métricas globais, PageRank, HITS, Louvain e taxa de citação por idade porque elas aprofundam a análise parcial sem recalcular distâncias já cobertas pelo script `distances.py`.

## 2. Metodologia
- Schema local confirmado a partir do GraphML: nós {':Article': 49196, ':Institution': 9266, ':Venue': 3845, ':Subfield': 202}; arestas {'AFFILIATED_WITH': 94010, 'HAS_SUBFIELD': 109179, 'CITES': 385529, 'PUBLISHED_IN': 36152}.
- O grafo estrutural usado nas métricas contém apenas `Article -[:CITES]-> Article`.
- PageRank foi calculado no grafo direcionado com `alpha=0.85`.
- HITS foi tentado com `max_iter=500` e `tol=1e-8`.
- Louvain foi calculado no maior WCC convertido para grafo não direcionado, com `seed=42`, usando `networkx.algorithms.community.louvain_communities`.
- `is_top_tier` foi derivado dos OpenAlex IDs encontrados em `data/responses_1/_top_cited_cs`.
- `is_important_institution` foi derivado dos OpenAlex IDs encontrados em `data/responses_1/by_institution`.
- O subdiretório `data/responses_1/by_institution/unicamp` foi excluído de `is_important_institution` para preservar a comparação com a própria Unicamp.
- Caches gerados: `metrics/article_metrics.csv` e `metrics/cluster_metrics.csv`.

## 3. Resultados
| Métrica | Valor |
| --- | --- |
| Artigos | 49.196 |
| Citações CITES | 385.529 |
| Artigos Unicamp/IC | 1.885 |
| IDs top-tier no ETL | 460 |
| Artigos is_top_tier no GraphML | 298 |
| IDs de instituições importantes no ETL | 195.328 |
| Artigos is_important_institution no GraphML | 7.569 |
| Artigos alto impacto >= 500 citações | 4.731 |
| Artigos alto impacto >= 100.000 citações | 3 |
| WCCs | 32 |
| Maior WCC | 49.012 |
| SCCs | 48.298 |
| Maior SCC | 34 |
| Comunidades Louvain no maior WCC | 38 |
| Modularidade Louvain | 0.7841 |

Fontes de `is_top_tier`:

| Diretório | IDs adicionados |
| --- | --- |
| data/responses_1/_top_cited_cs | 460 |

Fontes de `is_important_institution`:

| Diretório | IDs adicionados |
| --- | --- |
| data/responses_1/by_institution | 195.328 |

Top-10 artigos por PageRank:

| Rank | OpenAlex | Ano | Citações | PageRank | Título |
| --- | --- | --- | --- | --- | --- |
| 1 | W2163605009 | 2017 | 75.671 | 0.003955 | ImageNet classification with deep convolutional neural networks |
| 2 | W2147800946 | 1989 | 11.763 | 0.003348 | Backpropagation Applied to Handwritten Zip Code Recognition |
| 3 | W2103504761 | 1983 | 6.014 | 0.002797 | The Laplacian Pyramid as a Compact Image Code |
| 4 | W1970026646 | 1974 | 3.055 | 0.002624 | The String-to-String Correction Problem |
| 5 | W2133155955 | 2005 | 1.075 | 0.00259 | Scale-space filtering: A new approach to multi-scale description |
| 6 | W2104820473 | 1992 | 851 | 0.00258 | Supporting real-time applications in an Integrated Services Packet Network |
| 7 | W2130259898 | 1987 | 2.313 | 0.002501 | Low-dimensional procedure for the characterization of human faces |
| 8 | W2142276208 | 1996 | 5.342 | 0.002457 | A new, fast, and efficient image codec based on set partitioning in hierarchica... |
| 9 | W1973976434 | 1986 | 801 | 0.002418 | Uniqueness of the Gaussian Kernel for Scale-Space Filtering |
| 10 | W4211007335 | 1965 | 65.474 | 0.002412 | Fuzzy sets |

Top-10 comunidades por influência composta (`z(citações externas recebidas)`, `z(PageRank agregado)`, `z(nº de comunidades citantes)` com pesos iguais):

| community_id | Tamanho | Unicamp | Recebidas externas | PR soma | Alcance | Influência | Representante |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 5.739 | 61 | 10.918 | 0.1168 | 35 | 2.842 | ImageNet classification with deep convolutional neural networks |
| 1 | 4.122 | 177 | 8.878 | 0.09475 | 32 | 2.201 | Low-dimensional procedure for the characterization of human faces |
| 4 | 3.062 | 122 | 7.691 | 0.06045 | 32 | 1.629 | A training algorithm for optimal margin classifiers |
| 5 | 2.931 | 130 | 4.418 | 0.06657 | 32 | 1.264 | Fuzzy sets |
| 2 | 3.151 | 87 | 2.945 | 0.0676 | 28 | 0.9435 | OpenFlow |
| 3 | 3.065 | 105 | 2.764 | 0.05277 | 32 | 0.8765 | The Google file system |
| 9 | 2.082 | 28 | 1.965 | 0.052 | 30 | 0.6922 | The Laplacian Pyramid as a Compact Image Code |
| 10 | 1.637 | 119 | 2.480 | 0.03943 | 32 | 0.6777 | Scale-space filtering: A new approach to multi-scale description |
| 13 | 1.529 | 30 | 1.719 | 0.03385 | 32 | 0.5086 | The String-to-String Correction Problem |
| 6 | 2.479 | 47 | 997 | 0.04522 | 30 | 0.4809 | The Algebraic Eigenvalue Problem |

Fatos estruturais interpretáveis:

- O grafo é globalmente coeso, mas quase acíclico: o maior WCC contém 99.63% dos artigos, enquanto há 48,298 SCCs para 49,196 artigos. Isso sustenta a leitura de citações como fluxo temporal de conhecimento, com poucos ciclos residuais.
- A distribuição de impacto é fortemente concentrada: os 492 artigos no top 1% de in-degree concentram 24.6% das citações internas `CITES`, e o top 1% por PageRank concentra 29.2% do PageRank total.
- A marca `is_top_tier` é agora restrita aos top-cited por ano: 298 dos 460 IDs desse corpus aparecem no GraphML. Isso evita confundir prestígio institucional com seleção por impacto extremo.
- A mediana de PageRank dos top-tier (0.000185) é maior que a da Unicamp (1.11e-05) e a das instituições importantes sem top-tier (8e-06), indicando que a diferença não é apenas de volume institucional, mas de posição propagada na rede.

Figuras:
- `reports/figs/citation_rate_by_age_hist.png`: distribuição da taxa de citação normalizada por idade.
- `reports/figs/top_clusters_unicamp.png`: maiores comunidades e presença absoluta da Unicamp/IC.

## 4. Problemas encontrados
- A inspeção dinâmica do Neo4j (`CALL db.schema.visualization()`) não foi executada nesta primeira rodada; a confirmação foi feita pelo `network.graphml` local.
- Os scripts antigos não persistem `community_id`, `wcc_id` ou `scc_id` por artigo; por isso, a análise gerou novos caches em `metrics/`.
- O relatório antigo de comunidades em `analysis/reports/community_report.txt` registra 87.901 nós particionados, acima dos 49.196 artigos do GraphML final; por consistência, Louvain foi recalculado localmente no maior WCC do GraphML.
- Não há propriedade/label materializada para `A_Uni` e `A_inter` no GraphML. A Unicamp foi identificada por `AFFILIATED_WITH` para `I181391015`; `is_top_tier` e `is_important_institution` foram reconstruídos a partir dos diretórios de ETL indicados.
- HITS convergiu nesta execução, mas deve continuar sendo monitorado porque o grafo é quase acíclico.

## 5. Importância e interpretação
- O maior WCC concentra quase todo o grafo, então análises de comunidades e influência são estruturalmente significativas para o objetivo do projeto.
- A fragmentação em SCCs confirma o comportamento quase-DAG esperado para citações acadêmicas.
- As comunidades com alta influência composta são bons alvos para relatórios específicos, pois combinam citações externas, centralidade agregada e alcance entre áreas.
- A ausência de labels dos conjuntos-semente ainda limita comparações diretas com `A_inter`; o próximo passo técnico é materializar também os intermediários dos caminhos mínimos no cache.

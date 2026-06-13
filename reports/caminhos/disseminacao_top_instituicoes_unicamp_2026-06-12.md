# Análise caminhos: top-tier/instituições -> Unicamp
Data: 2026-06-12
Conjunto de dados: `data/output/paths_top_to_unicamp.json`, `data/output/paths_institutions_to_unicamp.json`, `metrics/article_metrics.csv`

## 1. Motivação
- O checklist final exige distribuição completa de distâncias, baseline e identificação de artigos-ponte em caminhos mínimos ou caminhos reconstruídos.
- Esta análise atende essa lacuna usando as saídas do ETL de caminhos, cruzadas com os caches de PageRank, clusters e flags de conjunto.
- A métrica central é frequência em caminhos: ela aproxima uma betweenness restrita aos pares de interesse (`A_fonte -> A_Uni`) e revela transmissores de conhecimento mais diretamente ligados ao objetivo do projeto.

## 2. Metodologia
- Foram reutilizados os caminhos de `data/output`, produzidos por `data/etl/3_find_path.py`.
- Parâmetros inferidos dos comandos do repositório: `max-depth=15`, `min-citations=5`, `top-k=50`; quando ausentes no JSON, estes parâmetros são tratados como provenance externa.
- Comprimento do caminho = número de arestas (`len(path)-1`).
- Artigo-ponte = nó intermediário `path[1:-1]`; endpoints não contam como ponte.
- Baseline: caminhos de instituições importantes para Unicamp são usados como referência para caminhos top-tier para Unicamp. Isso compara a cauda extrema de impacto contra uma elite institucional ampla.
- Arestas seguem a direção de citação do ETL (`fonte -> ... -> Unicamp` no caminho reconstruído). A interpretação como disseminação deve respeitar a limitação da busca podada.

## 3. Resultados
Resumo das distribuições:

| Conjunto | Caminhos | Pares fonte-alvo | Fonte no cache | Alvo no cache | Alvo Unicamp | Min | Média | Mediana | P90 | P99 | Max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| top_tier_to_unicamp | 37.168 | 3.316 | 19163 (51.6%) | 37085 (99.8%) | 36882 (99.2%) | 1 | 7.92 | 8 | 10 | 10 | 10 |
| institutions_to_unicamp | 71.424 | 71.424 | 4947 (6.9%) | 71144 (99.6%) | 70691 (99.0%) | 0 | 4.65 | 4 | 7 | 10 | 10 |

Comparação de distribuições:

| Comparação | KS empírico | Interpretação |
| --- | --- | --- |
| top-tier -> Unicamp vs instituições -> Unicamp | 0.695 | valores maiores indicam distribuições de comprimento mais separadas |

Figura:
- `figs/distribuicao_caminhos_2026-06-12.png`: distribuição normalizada dos comprimentos dos caminhos para os dois conjuntos.

Fatos não triviais:
- Caminhos top-tier e caminhos institucionais não medem a mesma coisa: o primeiro isola proximidade à cauda extrema de impacto; o segundo mede conectividade a uma elite institucional ampla.
- A mediana top-tier é 8 arestas, enquanto a mediana institucional é 4 arestas. A distância entre esses valores orienta se a Unicamp está mais diretamente conectada a artigos extremos ou a instituições selecionadas em geral.
- O KS empírico de 0.695 quantifica a separação entre as duas distribuições sem assumir normalidade, importante porque comprimentos de caminho são discretos e assimétricos.


### top_tier_to_unicamp

Artigos-ponte mais recorrentes:

| OpenAlex | Freq. | % caminhos | Cluster | PageRank | Unicamp | Top-tier | Título |
| --- | --- | --- | --- | --- | --- | --- | --- |
| W2970724283 | 2.098 | 5.6% | fora |  |  |  | fora do cache do GraphML |
| W2148043549 | 1.871 | 5.0% | 14 | 6.421416426224068e-05 | False | False | The NP-completeness column: An ongoing guide |
| W2783430431 | 1.666 | 4.5% | fora |  |  |  | fora do cache do GraphML |
| W2024332685 | 1.506 | 4.1% | 13 | 6.162361851594137e-06 | False | False | On Live-Dead Analysis for Global Data Flow Problems |
| W4392773210 | 1.408 | 3.8% | fora |  |  |  | fora do cache do GraphML |
| W2004618348 | 1.393 | 3.7% | 13 | 0.00011644976079537747 | False | False | Parallelism in random access machines |
| W2131929623 | 1.388 | 3.7% | 14 | 0.00021265156516163245 | False | False | Exploiting virtual synchrony in distributed systems |
| W4385763767 | 1.043 | 2.8% | fora |  |  |  | fora do cache do GraphML |
| W3184127157 | 1.020 | 2.7% | 0 | 1.180648397631745e-05 | False | False | Learning Graph Structures with Transformer for Multivariate Time Series Anomaly Detec... |
| W2124199440 | 838 | 2.3% | 1 | 1.875371476129518e-05 | False | False | An introduction to spatial database systems |
| W2584004798 | 802 | 2.2% | fora |  |  |  | fora do cache do GraphML |
| W2012329067 | 802 | 2.2% | 25 | 0.00021770871965492755 | False | False | The ellipsoid method and its consequences in combinatorial optimization |

Clusters mais frequentes como intermediários:

| Cluster | Ocorrências | Caminhos únicos | % caminhos | Tamanho | Unicamp | Influência | Representante |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 14 | 8.443 | 6.981 | 18.8% | 1383 | 71 | 0.342 | The Art of Computer Programming. Volume 2: Seminumerical Algorithms. |
| 0 | 8.265 | 6.394 | 17.2% | 5739 | 61 | 2.842 | ImageNet classification with deep convolutional neural networks |
| 13 | 7.535 | 7.097 | 19.1% | 1529 | 30 | 0.509 | The String-to-String Correction Problem |
| 1 | 6.349 | 5.369 | 14.4% | 4122 | 177 | 2.201 | Low-dimensional procedure for the characterization of human faces |
| 3 | 5.791 | 5.039 | 13.6% | 3065 | 105 | 0.876 | The Google file system |
| 9 | 5.223 | 4.078 | 11.0% | 2082 | 28 | 0.692 | The Laplacian Pyramid as a Compact Image Code |
| 8 | 4.235 | 2.766 | 7.4% | 2211 | 82 | 0.053 | Quantum computational networks |
| 10 | 2.487 | 1.949 | 5.2% | 1637 | 119 | 0.678 | Scale-space filtering: A new approach to multi-scale description |
| 12 | 1.848 | 1.479 | 4.0% | 1579 | 104 | -0.023 | SELECT—a formal system for testing and debugging programs by symbolic... |
| 4 | 1.694 | 1.528 | 4.1% | 3062 | 122 | 1.629 | A training algorithm for optimal margin classifiers |

Pares de cluster origem/destino mais comuns nos caminhos caracterizáveis:

| Par | Caminhos | Origem | Destino |
| --- | --- | --- | --- |
| 0 -> 13 | 2.786 | ImageNet classification with deep convolutional... | The String-to-String Correction Problem |
| 0 -> 1 | 2.444 | ImageNet classification with deep convolutional... | Low-dimensional procedure for the characterizat... |
| 3 -> 14 | 1.838 | The Google file system | The Art of Computer Programming. Volume 2: Semi... |
| 0 -> 14 | 1.256 | ImageNet classification with deep convolutional... | The Art of Computer Programming. Volume 2: Semi... |
| 8 -> 13 | 1.198 | Quantum computational networks | The String-to-String Correction Problem |
| 3 -> 13 | 1.047 | The Google file system | The String-to-String Correction Problem |
| 0 -> 9 | 884 | ImageNet classification with deep convolutional... | The Laplacian Pyramid as a Compact Image Code |
| 1 -> 13 | 828 | Low-dimensional procedure for the characterizat... | The String-to-String Correction Problem |
| 0 -> 10 | 710 | ImageNet classification with deep convolutional... | Scale-space filtering: A new approach to multi-... |
| 7 -> 13 | 606 | A computational procedure for determining energ... | The String-to-String Correction Problem |

Leitura:
- 25.3% das ocorrências intermediárias foram mapeadas para algum `community_id`; o restante envolve nós presentes nos caminhos brutos, mas ausentes do GraphML/cache final.
- A concentração dos intermediários em poucos clusters indica que a disseminação não ocorre por todo o grafo de forma uniforme; ela passa por comunidades específicas que funcionam como corredores estruturais.

### institutions_to_unicamp

Artigos-ponte mais recorrentes:

| OpenAlex | Freq. | % caminhos | Cluster | PageRank | Unicamp | Top-tier | Título |
| --- | --- | --- | --- | --- | --- | --- | --- |
| W2148043549 | 5.535 | 7.7% | 14 | 6.421416426224068e-05 | False | False | The NP-completeness column: An ongoing guide |
| W2071939274 | 2.543 | 3.6% | 13 | 6.162361851594137e-06 | False | False | Parallel Algorithms in Graph Theory: Planarity Testing |
| W2024332685 | 2.380 | 3.3% | 13 | 6.162361851594137e-06 | False | False | On Live-Dead Analysis for Global Data Flow Problems |
| W2004618348 | 2.360 | 3.3% | 13 | 0.00011644976079537747 | False | False | Parallelism in random access machines |
| W2045466646 | 2.275 | 3.2% | fora |  |  |  | fora do cache do GraphML |
| W2088300760 | 1.863 | 2.6% | 13 | 7.792797082934815e-05 | False | False | A characterization of the power of vector machines |
| W2135105491 | 1.743 | 2.4% | 3 | 4.27955425374232e-05 | False | False | Computational problems related to the design of normal form relational schemas |
| W4251893211 | 1.422 | 2.0% | 13 | 8.613362486234331e-06 | False | False | Alternation |
| W2162436812 | 1.318 | 1.8% | fora |  |  |  | fora do cache do GraphML |
| W2047092246 | 1.315 | 1.8% | fora |  |  |  | fora do cache do GraphML |
| W2012329067 | 1.271 | 1.8% | 25 | 0.00021770871965492755 | False | False | The ellipsoid method and its consequences in combinatorial optimization |
| W2119694598 | 1.264 | 1.8% | 3 | 1.528222757952216e-05 | False | False | Load Balancing with Neural Network |

Clusters mais frequentes como intermediários:

| Cluster | Ocorrências | Caminhos únicos | % caminhos | Tamanho | Unicamp | Influência | Representante |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 13 | 16.444 | 15.723 | 22.0% | 1529 | 30 | 0.509 | The String-to-String Correction Problem |
| 14 | 14.165 | 12.618 | 17.7% | 1383 | 71 | 0.342 | The Art of Computer Programming. Volume 2: Seminumerical Algorithms. |
| 1 | 8.044 | 7.312 | 10.2% | 4122 | 177 | 2.201 | Low-dimensional procedure for the characterization of human faces |
| 3 | 6.484 | 6.172 | 8.6% | 3065 | 105 | 0.876 | The Google file system |
| 0 | 4.734 | 3.693 | 5.2% | 5739 | 61 | 2.842 | ImageNet classification with deep convolutional neural networks |
| 9 | 4.362 | 3.740 | 5.2% | 2082 | 28 | 0.692 | The Laplacian Pyramid as a Compact Image Code |
| 8 | 3.888 | 3.179 | 4.5% | 2211 | 82 | 0.053 | Quantum computational networks |
| 10 | 3.580 | 3.035 | 4.2% | 1637 | 119 | 0.678 | Scale-space filtering: A new approach to multi-scale description |
| 20 | 3.264 | 3.030 | 4.2% | 712 | 69 | -0.345 | A formulae-as-type notion of control |
| 25 | 2.533 | 2.455 | 3.4% | 428 | 34 | -0.405 | A Minimax Theorem for Directed Graphs |

Pares de cluster origem/destino mais comuns nos caminhos caracterizáveis:

| Par | Caminhos | Origem | Destino |
| --- | --- | --- | --- |
| 14 -> 13 | 4.154 | The Art of Computer Programming. Volume 2: Semi... | The String-to-String Correction Problem |
| 0 -> 1 | 1.851 | ImageNet classification with deep convolutional... | Low-dimensional procedure for the characterizat... |
| 8 -> 13 | 814 | Quantum computational networks | The String-to-String Correction Problem |
| 15 -> 13 | 692 | Multiple emitter location and signal parameter ... | The String-to-String Correction Problem |
| 5 -> 13 | 559 | Fuzzy sets | The String-to-String Correction Problem |
| 1 -> 13 | 485 | Low-dimensional procedure for the characterizat... | The String-to-String Correction Problem |
| 16 -> 13 | 388 | On the Complexity of Finite Sequences | The String-to-String Correction Problem |
| 1 -> 10 | 362 | Low-dimensional procedure for the characterizat... | Scale-space filtering: A new approach to multi-... |
| 0 -> 10 | 349 | ImageNet classification with deep convolutional... | Scale-space filtering: A new approach to multi-... |
| 10 -> 13 | 338 | Scale-space filtering: A new approach to multi-... | The String-to-String Correction Problem |

Leitura:
- 33.6% das ocorrências intermediárias foram mapeadas para algum `community_id`; o restante envolve nós presentes nos caminhos brutos, mas ausentes do GraphML/cache final.
- A concentração dos intermediários em poucos clusters indica que a disseminação não ocorre por todo o grafo de forma uniforme; ela passa por comunidades específicas que funcionam como corredores estruturais.


## 4. Problemas encontrados
- Os arquivos de caminho não guardam todos os parâmetros de execução; `max-depth`, `min-citations` e `top-k` foram inferidos de `data/comandos.md`.
- Muitos nós intermediários aparecem nos caminhos brutos, mas não no GraphML final, então não recebem `community_id`, PageRank ou título. O relatório separa essas ocorrências e não as interpreta tematicamente.
- Os caminhos vêm de busca podada por citações/top-k; portanto, são caminhos relevantes dentro do procedimento de ETL, não uma enumeração exaustiva de todos os menores caminhos no grafo completo OpenAlex.

## 5. Importância e interpretação
- Artigos-ponte recorrentes são candidatos a transmissores de conhecimento entre fontes externas e Unicamp.
- Clusters intermediários recorrentes indicam quais áreas conectam a produção Unicamp a top-tier/instituições, complementando a análise de comunidades.
- A comparação com baseline institucional evita interpretar uma distância curta ao top-tier como algo trivial: ela precisa ser lida contra outro conjunto de referência.
- Próximo passo: selecionar 2-3 artigos-ponte do cache `metrics/path_bridge_metrics.csv` para análise de artigo específico.

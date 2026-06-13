# Análise caminhos: A_inter e disseminação
Data: 2026-06-12
Conjunto de dados: `data/output/paths_*.json`, `metrics/article_metrics.csv`, `network.graphml`

## 1. Motivação
- A auditoria da iteração anterior apontou uma lacuna estrutural: a conclusão sobre disseminação de conhecimento ainda estava mais bem escrita do que metricamente sustentada.
- Esta análise responde diretamente a essa lacuna materializando `A_inter` no cache, medindo centralidade de ponte no subgrafo relevante aos caminhos e identificando de forma sistemática os artigos e clusters intermediários mais recorrentes.
- Também complementa a rodada anterior com percentis completos de distância e um heatmap explícito de fluxo intercomunitário.

## 2. Metodologia
- `A_inter bruto` foi definido como a união dos nós intermediários `path[1:-1]` dos arquivos `paths_top_to_unicamp.json` e `paths_institutions_to_unicamp.json`.
- Como grande parte desses nós não aparece no GraphML final, foi materializado um conjunto operacional `A_inter_graphml = A_inter bruto ∩ nós do GraphML`.
- O subgrafo de análise contém todos os nós de caminhos presentes no GraphML, somados a `A_Uni` e `A_TT` (`13.239` nós, `82.642` arestas).
- Métricas calculadas:
  - `approx_betweenness_path_subgraph`: betweenness aproximada com `k=64` e `seed=42`;
  - `eigenvector_path_subgraph`: eigenvector no grafo não-direcionado induzido;
  - `harmonic_to_unicamp_sampled` e `harmonic_to_top_tier_sampled`: alcance harmônico amostrado, com até `128` sementes por conjunto e `seed=42`;
  - `bridge_proximity`: média geométrica das versões normalizadas dos dois alcances harmônicos, para destacar nós simultaneamente próximos de `A_Uni` e `A_TT`.
- O alcance harmônico foi amostrado, e não exato, porque a versão exata sobre todos os nós do subgrafo se mostrou cara demais para esta iteração.

## 3. Resultados
Materialização de `A_inter`:

| Indicador | Valor |
| --- | --- |
| A_inter bruto top-tier | 46.191 |
| A_inter bruto instituicoes | 51.808 |
| A_inter bruto uniao | 79.022 |
| A_inter materializado no GraphML | 8.525 |
| Nos de caminhos presentes no GraphML | 12.146 |
| Sobreposicao A_inter_graphml ∩ A_Uni | 27 |
| Sobreposicao A_inter_graphml ∩ A_TT | 77 |
| Sobreposicao A_inter_graphml ∩ A_Inst | 1.931 |

Leitura inicial:
- `A_inter` existe em grande quantidade nos caminhos brutos, mas só uma fração vira objeto analisável no corpus final: `A_inter_graphml` tem 8525 nós.
- Mesmo assim, o conjunto materializado já é substantivo o bastante para análise comparativa e cobre os intermediários mais recorrentes do lado visível do grafo.

Distribuições completas de comprimento de caminho:

| Conjunto | n | min | p25 | mediana | p75 | p90 | p99 | max | media |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| top-tier -> Unicamp | 37.168 | 1 | 7 | 8 | 9 | 10 | 10 | 10 | 7.92 |
| instituicoes -> Unicamp | 71.424 | 0 | 4 | 4 | 6 | 7 | 10 | 10 | 4.65 |
| instituicoes -> Unicamp (sem zeros) | 71.042 | 1 | 4 | 4 | 6 | 7 | 10 | 10 | 4.67 |

Figura:
- `figs/caminhos_cdf_2026-06-12.png`: CDF dos comprimentos de caminho para top-tier e instituições.

Leitura:
- A diferença observada antes nas medianas permanece quando olhamos a distribuição inteira: `top-tier -> Unicamp` continua mais longo em quase todos os percentis.
- Os 382 caminhos institucionais de tamanho zero não explicam a conclusão; sem eles, a mediana institucional continua 4.

Tabela-resumo por conjunto:

| Conjunto | n | Mediana PR | Mediana authority | Mediana betweenness* | P90 betweenness* | Mediana eigenvector* | Mediana harm.->Uni* | Mediana harm.->TT* | Mediana bridge_proximity* |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| A_inter_graphml | 8.525 | 1.3e-05 | 4.77e-07 | 0 | 1.74e-05 | 3.02e-05 | 0.175 | 0.262 | 0.598 |
| A_Uni | 1.885 | 1.11e-05 | 2.23e-08 | 0 | 0 | 2.08e-06 | 0.156 | 0.224 | 0.521 |
| A_TT | 298 | 0.000185 | 9.47e-05 | 0 | 8.97e-05 | 0.00128 | 0.205 | 0.335 | 0.73 |
| A_Inst | 7.569 | 8.05e-06 | 4.34e-08 | 0 | 7.76e-06 | 3.44e-05 | 0.175 | 0.261 | 0.596 |

(* metricas calculadas no subgrafo relevante aos caminhos.)

Leitura:
- A mediana de betweenness fica zerada para varios conjuntos, então ela sozinha esconde a cauda realmente intermediadora. O percentil 90 separa melhor `A_inter_graphml` de `A_Uni`, mostrando que a funcao de ponte se concentra numa fração do conjunto, e nao em todos os seus nos.
- `A_TT` continua dominante em centralidade estrutural clássica, mas isso não significa que ele próprio desempenhe o papel intermediário mais frequente nos caminhos até a Unicamp.
- O contraste entre `A_inter_graphml` e `A_Uni` é especialmente útil: ele separa artigos finais do destino institucional de artigos que servem como canal de passagem.

Artigos intermediários mais recorrentes no corpus materializado:

| OpenAlex | Freq top-tier | Freq inst. | Freq total | Cluster | Betweenness* | Bridge proximity* | Título |
| --- | --- | --- | --- | --- | --- | --- | --- |
| W2148043549 | 1.871 | 5.535 | 7.406 | 14 | 0.000316 | 0.646 | The NP-completeness column: An ongoing guide |
| W2024332685 | 1.506 | 2.380 | 3.886 | 13 | 0 | 0.426 | On Live-Dead Analysis for Global Data Flow Problems |
| W2004618348 | 1.393 | 2.360 | 3.753 | 13 | 4.76e-05 | 0.594 | Parallelism in random access machines |
| W2071939274 | 520 | 2.543 | 3.063 | 13 | 0 | 0.486 | Parallel Algorithms in Graph Theory: Planarity Testing |
| W2131929623 | 1.388 | 1.152 | 2.540 | 14 | 6.14e-05 | 0.525 | Exploiting virtual synchrony in distributed systems |
| W2135105491 | 788 | 1.743 | 2.531 | 3 | 0 | 0.441 | Computational problems related to the design of normal form relational schemas |
| W2088300760 | 410 | 1.863 | 2.273 | 13 | 4.58e-06 | 0.527 | A characterization of the power of vector machines |
| W2012329067 | 802 | 1.271 | 2.073 | 25 | 0.000118 | 0.572 | The ellipsoid method and its consequences in combinatorial optimization |
| W4251893211 | 368 | 1.422 | 1.790 | 13 | 0 | 0.438 | Alternation |
| W2124199440 | 838 | 902 | 1.740 | 1 | 5.19e-05 | 0.665 | An introduction to spatial database systems |
| W2119694598 | 356 | 1.264 | 1.620 | 3 | 0 | 0.541 | Load Balancing with Neural Network |
| W2027501230 | 579 | 928 | 1.507 | 13 | 2.7e-05 | 0.521 | On uniform circuit complexity |

Artigos intermediários mais centrais por betweenness no subgrafo de caminhos:

| OpenAlex | Cluster | Freq total | Betweenness* | Harm.->Uni* | Harm.->TT* | Título |
| --- | --- | --- | --- | --- | --- | --- |
| W2040340473 | 2 | 6 | 0.0023 | 0.223 | 0.338 | Software-Defined Networking: A Comprehensive Survey |
| W2962814013 | 3 | 7 | 0.00152 | 0.221 | 0.355 | Convergence of Edge Computing and Deep Learning: A Comprehensive Survey |
| W2117539524 | 0 | 15 | 0.00129 | 0.242 | 0.454 | ImageNet Large Scale Visual Recognition Challenge |
| W2615459164 | 3 | 8 | 0.00103 | 0.195 | 0.29 | On Multi-Access Edge Computing: A Survey of the Emerging 5G Network Edge Cloud Architec... |
| W2102605133 | 0 | 1 | 0.00102 | 0.227 | 0.436 | Rich Feature Hierarchies for Accurate Object Detection and Semantic Segmentation |
| W2002791369 | 0 | 14 | 0.000906 | 0.193 | 0.312 | Content-based retrieval of human actions from realistic video databases |
| W3103243695 | 2 | 1 | 0.000867 | 0.201 | 0.301 | Survey on Network Virtualization Hypervisors for Software Defined Networking |
| W2148037789 | 9 | 437 | 0.000743 | 0.189 | 0.274 | Multiple description coding: compression meets the network |
| W2046382188 | 0 | 16 | 0.000708 | 0.206 | 0.351 | CPMC: Automatic Object Segmentation Using Constrained Parametric Min-Cuts |
| W2594560857 | 3 | 11 | 0.000597 | 0.206 | 0.312 | Mobile Edge Computing: A Survey on Architecture and Computation Offloading |
| W1932198206 | 0 | 7 | 0.000593 | 0.213 | 0.387 | Deep neural networks are easily fooled: High confidence predictions for unrecognizable ... |
| W2153663612 | 9 | 40 | 0.000557 | 0.202 | 0.341 | Image Denoising Via Sparse and Redundant Representations Over Learned Dictionaries |

Leitura:
- A frequência em caminhos e a betweenness aproximada não são redundantes. Alguns artigos aparecem muitas vezes por estarem em um corredor muito usado; outros têm betweenness alta por conectar partes menos substituíveis do subgrafo.
- Isso corrige uma fragilidade da rodada anterior, onde havia só um artigo-ponte citado isoladamente. Agora há uma identificação sistemática de top-N.

Clusters com maior presença de `A_inter_graphml`:

| Cluster | Nos A_inter | % de A_inter | Tamanho do cluster | Unicamp | Influencia | Representante |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | 1.490 | 17.5% | 5739 | 61 | 2.841792877478408 | ImageNet classification with deep convolutional neural networks |
| 1 | 873 | 10.2% | 4122 | 177 | 2.200851041592891 | Low-dimensional procedure for the characterization of human faces |
| 3 | 615 | 7.2% | 3065 | 105 | 0.876487834869056 | The Google file system |
| 4 | 531 | 6.2% | 3062 | 122 | 1.628681702795295 | A training algorithm for optimal margin classifiers |
| 8 | 518 | 6.1% | 2211 | 82 | 0.052834620518682936 | Quantum computational networks |
| 2 | 470 | 5.5% | 3151 | 87 | 0.943538205186242 | OpenFlow |
| 5 | 421 | 4.9% | 2931 | 130 | 1.264101946448883 | Fuzzy sets |
| 14 | 377 | 4.4% | 1383 | 71 | 0.34205539813302116 | The Art of Computer Programming. Volume 2: Seminumerical Algorithms. |
| 9 | 372 | 4.4% | 2082 | 28 | 0.6922409219392337 | The Laplacian Pyramid as a Compact Image Code |
| 12 | 323 | 3.8% | 1579 | 104 | -0.023405402974720452 | SELECT—a formal system for testing and debugging programs by symbolic e... |

Figura:
- `figs/fluxo_intercomunidades_a_inter_2026-06-12.png`: fluxo de citações entre os 10 clusters com maior presença de `A_inter_graphml`.

Leitura:
- Os intermediários não se espalham uniformemente pelo grafo: eles se concentram em poucos clusters já relevantes nas análises anteriores, especialmente `0`, `1`, `14` e outros blocos grandes de visão computacional, sistemas e teoria.
- O cluster `13` aparece menos pelo volume bruto de nós intermediários e mais pela recorrência de artigos-ponte muito frequentes, o que é um lembrete útil de que contagem de nós e intensidade de uso dos corredores não são a mesma coisa.
- Isso fortalece a leitura de que a disseminação observada no corpus passa por corredores comunitários específicos, e não por uma malha homogênea.

## 4. Problemas encontrados
- `A_inter bruto` é muito maior que `A_inter_graphml`; a maior parte dos intermediários dos caminhos não está no GraphML final.
- Por isso, esta análise fala do subconjunto visível de `A_inter` no corpus final, não do universo completo de intermediários do ETL.
- `harmonic_to_*` foi aproximado por amostragem de sementes; ele é útil para ranking relativo, mas não deve ser tratado como estimativa exata de closeness/harmonic centrality.
- O heatmap de fluxo intercomunitário depende dos clusters materializados no GraphML final e, portanto, herda a sensibilidade do Louvain à resolução.

## 5. Importância e interpretação
- Esta iteração reduz a maior lacuna apontada pela auditoria: a discussão de disseminação agora repousa sobre um conjunto explícito de intermediários, com cache próprio e métricas de ponte mais diretamente conectadas ao objetivo do projeto.
- O resultado principal é que `A_inter_graphml` não é apenas "quem apareceu em algum caminho": ele forma um conjunto estruturalmente distinguível, mais próximo simultaneamente de `A_Uni` e `A_TT` e concentrado em poucos clusters-chave.
- A conclusão continua merecendo cautela por causa da cobertura parcial do GraphML, mas agora ela está muito mais próxima de uma análise sustentada por evidência estrutural do que de uma inferência qualitativa apoiada só em exemplos.

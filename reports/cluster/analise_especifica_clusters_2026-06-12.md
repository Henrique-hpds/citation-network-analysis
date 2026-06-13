# Análise específica de clusters selecionados
Data: 2026-06-12
Conjunto de dados: `network.graphml` local + `metrics/article_metrics.csv` + `metrics/cluster_metrics.csv`

## 1. Motivação
- Esta análise aprofunda a detecção de comunidades seguindo o escopo do `AGENTS.md`: densidade, condutância, fluxo intercomunidades, representatividade temática, participação da Unicamp e influência composta.
- Os clusters foram escolhidos automaticamente para cobrir papéis estruturais diferentes, não apenas os maiores: influência global, presença Unicamp, presença top-tier, presença de instituições importantes, alta fração Unicamp e alta condutância.
- O objetivo é transformar métricas em fatos explicáveis sobre disseminação de conhecimento no grafo.

## 2. Metodologia
- Comunidades: Louvain recalculado previamente no maior WCC não direcionado e cacheado em `metrics/article_metrics.csv`.
- Fluxo intercomunidades: contagem dirigida de arestas `CITES` entre pares de `community_id`; a direção preserva `artigo origem cita artigo destino`.
- Enriquecimento: razão entre contagem observada no cluster e contagem esperada se o conjunto estivesse distribuído proporcionalmente ao tamanho do cluster.
- Interpretação: top-tier (`is_top_tier`) representa `_top_cited_cs`; instituições importantes (`is_important_institution`) representa `by_institution`, excluindo Unicamp.

## 3. Resultados
Clusters selecionados:

| Cluster | Motivo | Tamanho | Unicamp | % Uni | Top-tier | Enr. TT | Inst. imp. | Enr. inst. | Influência | Representante |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | maior influência composta; maior quantidade de artigos top-tier; maior quantidade de artigos de instituições importantes | 5.739 | 61 | 1.06% | 84 | 2.42x | 1.248 | 1.41x | 2.842 | ImageNet classification with deep convolutional neural ne... |
| 1 | maior presença absoluta da Unicamp/IC | 4.122 | 177 | 4.29% | 32 | 1.28x | 761 | 1.20x | 2.201 | Low-dimensional procedure for the characterization of hum... |
| 29 | maior fração Unicamp/IC entre clusters com ao menos 100 artigos | 191 | 21 | 10.99% | 0 | 0.00x | 34 | 1.16x | -0.869 | On sufficiency of the Kuhn-Tucker conditions |
| 32 | maior condutância de saída entre clusters grandes | 124 | 6 | 4.84% | 0 | 0.00x | 26 | 1.36x | -0.636 | Detecting and reading text in natural scenes |

Figura:
- `figs/clusters_especificos_2026-06-12.png`: compara a composição percentual dos clusters selecionados por Unicamp, top-tier e instituições importantes.


### Cluster 0: maior influência composta; maior quantidade de artigos top-tier; maior quantidade de artigos de instituições importantes

**Perfil estrutural.**
| Métrica | Valor |
| --- | --- |
| Tamanho | 5.739 |
| Rank por tamanho | 1 |
| Arestas internas | 59287 |
| Densidade interna | 0.0018 |
| Condutância de saída | 0.1496 |
| Influência composta | 2.842 |
| Ano médio | 2017.4 |
| Representante | ImageNet classification with deep convolutional neural networks |

**Composição e enriquecimento.**
| Conjunto | Artigos | % do cluster | Enriquecimento vs grafo |
| --- | --- | --- | --- |
| Unicamp/IC | 61 | 1.06% | 0.28x |
| Top-tier | 84 | 1.46% | 2.42x |
| Instituições importantes | 1.248 | 21.75% | 1.41x |
| Alto impacto >=500 citações | 1.096 | 19.10% | - |

**Caracterização temática.**
| Dimensão | Top valores |
| --- | --- |
| Subfields | Computer Vision and Pattern Recognition (4222); Artificial Intelligence (3213); Signal Processing (512); Biomedical Engineering (342); Media Technology (304) |
| Instituições | Chinese Academy of Sciences (243); Tsinghua University (226); University of Chinese Academy of Sciences (169); Google (United States) (134); Carnegie Mellon University (133) |
| Veículos | IEEE Transactions on Pattern Analysis and Machine Intelligence (255); Proceedings of the AAAI Conference on Artificial Intelligence (171); IEEE Transactions on Image Processing (163); IEEE Access (133); Pattern Recognition (126) |

**Fluxo intercomunidades.**

Principais origens que citam este cluster:

| Cluster origem | Citações | Tamanho | Unicamp | Representante |
| --- | --- | --- | --- | --- |
| 1 | 3.240 | 4122 | 177 | Low-dimensional procedure for the characterization of human fac... |
| 17 | 1.557 | 1024 | 28 | <title>Live face detection based on the analysis of Fourier spe... |
| 4 | 968 | 3062 | 122 | A training algorithm for optimal margin classifiers |
| 21 | 962 | 685 | 29 | Random sample consensus |
| 3 | 859 | 3065 | 105 | The Google file system |

Principais destinos citados por este cluster:

| Cluster destino | Citações | Tamanho | Unicamp | Representante |
| --- | --- | --- | --- | --- |
| 1 | 4.198 | 4122 | 177 | Low-dimensional procedure for the characterization of human fac... |
| 4 | 1.909 | 3062 | 122 | A training algorithm for optimal margin classifiers |
| 10 | 808 | 1637 | 119 | Scale-space filtering: A new approach to multi-scale description |
| 5 | 572 | 2931 | 130 | Fuzzy sets |
| 9 | 521 | 2082 | 28 | The Laplacian Pyramid as a Compact Image Code |

**Artigos representativos por PageRank.**
| Rank | OpenAlex | Ano | Citações | Unicamp | Top-tier | Título |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | W2163605009 | 2017 | 75671 | False | True | ImageNet classification with deep convolutional neural networks |
| 2 | W2147800946 | 1989 | 11763 | False | True | Backpropagation Applied to Handwritten Zip Code Recognition |
| 3 | W1498436455 | 1986 | 30286 | False | True | Learning representations by back-propagating errors |
| 4 | W169539560 | 1989 | 844 | False | False | Generalization and network design strategies |
| 5 | W2117671523 | 1989 | 2637 | False | False | Phoneme recognition using time-delay neural networks |

**Artigos Unicamp mais centrais no cluster.**
| Rank | OpenAlex | Ano | Citações | PageRank | Título |
| --- | --- | --- | --- | --- | --- |
| 1 | W2119880843 | 2012 | 1410 | 0.000181 | Toward Open Set Recognition |
| 2 | W2100332042 | 2011 | 213 | 6.48e-05 | Meta-Recognition: The Theory and Practice of Recognition Score Analysis |
| 3 | W3097185012 | 2020 | 196 | 4.45e-05 | Fighting Hate Speech, Silencing Drag Queens? Artificial Intelligence in Content Moder... |
| 4 | W1984669326 | 2010 | 140 | 3.73e-05 | Violence Detection in Video Using Spatio-Temporal Features |
| 5 | W2248269543 | 2016 | 273 | 3.69e-05 | Nearest neighbors distance ratio open-set classifier |

**Leitura não trivial.**
- Este cluster tem enriquecimento top-tier de 2.42x e enriquecimento Unicamp de 0.28x. A comparação separa dois fenômenos diferentes: proximidade da cauda extrema de impacto e presença institucional.
- A condutância de saída (0.150) indica quanto o cluster conversa com outras comunidades. Valores altos sugerem papel de circulação/difusão; valores baixos sugerem bloco mais autocontido.
- O par de fluxos de entrada e saída mostra quem alimenta o cluster e para onde ele referencia conhecimento, evitando interpretar comunidade apenas como tema isolado.

### Cluster 1: maior presença absoluta da Unicamp/IC

**Perfil estrutural.**
| Métrica | Valor |
| --- | --- |
| Tamanho | 4.122 |
| Rank por tamanho | 2 |
| Arestas internas | 24053 |
| Densidade interna | 0.001416 |
| Condutância de saída | 0.2489 |
| Influência composta | 2.201 |
| Ano médio | 2008.3 |
| Representante | Low-dimensional procedure for the characterization of human faces |

**Composição e enriquecimento.**
| Conjunto | Artigos | % do cluster | Enriquecimento vs grafo |
| --- | --- | --- | --- |
| Unicamp/IC | 177 | 4.29% | 1.12x |
| Top-tier | 32 | 0.78% | 1.28x |
| Instituições importantes | 761 | 18.46% | 1.20x |
| Alto impacto >=500 citações | 440 | 10.67% | - |

**Caracterização temática.**
| Dimensão | Top valores |
| --- | --- |
| Subfields | Computer Vision and Pattern Recognition (3063); Artificial Intelligence (1249); Signal Processing (829); Media Technology (368); Information Systems (343) |
| Instituições | Universidade Estadual de Campinas (UNICAMP) (177); Carnegie Mellon University (89); Universidade de São Paulo (85); Microsoft (United States) (80); Microsoft Research Asia (China) (77) |
| Veículos | Pattern Recognition (153); IEEE Transactions on Pattern Analysis and Machine Intelligence (153); IEEE Transactions on Visualization and Computer Graphics (144); IEEE Transactions on Image Processing (87); International Journal of Computer Vision (73) |

**Fluxo intercomunidades.**

Principais origens que citam este cluster:

| Cluster origem | Citações | Tamanho | Unicamp | Representante |
| --- | --- | --- | --- | --- |
| 0 | 4.198 | 5739 | 61 | ImageNet classification with deep convolutional neural networks |
| 4 | 863 | 3062 | 122 | A training algorithm for optimal margin classifiers |
| 17 | 720 | 1024 | 28 | <title>Live face detection based on the analysis of Fourier spe... |
| 10 | 719 | 1637 | 119 | Scale-space filtering: A new approach to multi-scale description |
| 21 | 391 | 685 | 29 | Random sample consensus |

Principais destinos citados por este cluster:

| Cluster destino | Citações | Tamanho | Unicamp | Representante |
| --- | --- | --- | --- | --- |
| 0 | 3.240 | 5739 | 61 | ImageNet classification with deep convolutional neural networks |
| 4 | 1.351 | 3062 | 122 | A training algorithm for optimal margin classifiers |
| 10 | 724 | 1637 | 119 | Scale-space filtering: A new approach to multi-scale description |
| 5 | 460 | 2931 | 130 | Fuzzy sets |
| 9 | 373 | 2082 | 28 | The Laplacian Pyramid as a Compact Image Code |

**Artigos representativos por PageRank.**
| Rank | OpenAlex | Ano | Citações | Unicamp | Top-tier | Título |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | W2130259898 | 1987 | 2313 | False | False | Low-dimensional procedure for the characterization of human faces |
| 2 | W2138451337 | 1991 | 13744 | False | True | Eigenfaces for Recognition |
| 3 | W2044465660 | 1973 | 22329 | False | False | Textural Features for Image Classification |
| 4 | W2325227998 | 1983 | 5980 | False | True | Introduction to modern information retrieval |
| 5 | W2008196645 | 1974 | 2014 | False | False | Quad trees a data structure for retrieval on composite keys |

**Artigos Unicamp mais centrais no cluster.**
| Rank | OpenAlex | Ano | Citações | PageRank | Título |
| --- | --- | --- | --- | --- | --- |
| 1 | W2105206277 | 1999 | 139 | 9.79e-05 | Estimating crowd density with Minkowski fractal dimension |
| 2 | W2003863309 | 1998 | 248 | 8.99e-05 | Towards historical R-trees |
| 3 | W2512304460 | 2016 | 282 | 6.38e-05 | Visualizing the Hidden Activity of Artificial Neural Networks |
| 4 | W2031610515 | 2005 | 165 | 5.67e-05 | Tracking soccer players aiming their kinematical motion analysis |
| 5 | W2039388884 | 2002 | 303 | 5.08e-05 | A compact and efficient image retrieval approach based on border/interior pixel class... |

**Leitura não trivial.**
- Este cluster tem enriquecimento top-tier de 1.28x e enriquecimento Unicamp de 1.12x. A comparação separa dois fenômenos diferentes: proximidade da cauda extrema de impacto e presença institucional.
- A condutância de saída (0.249) indica quanto o cluster conversa com outras comunidades. Valores altos sugerem papel de circulação/difusão; valores baixos sugerem bloco mais autocontido.
- O par de fluxos de entrada e saída mostra quem alimenta o cluster e para onde ele referencia conhecimento, evitando interpretar comunidade apenas como tema isolado.

### Cluster 29: maior fração Unicamp/IC entre clusters com ao menos 100 artigos

**Perfil estrutural.**
| Métrica | Valor |
| --- | --- |
| Tamanho | 191 |
| Rank por tamanho | 30 |
| Arestas internas | 728 |
| Densidade interna | 0.02006 |
| Condutância de saída | 0.02804 |
| Influência composta | -0.869 |
| Ano médio | 2001.4 |
| Representante | On sufficiency of the Kuhn-Tucker conditions |

**Composição e enriquecimento.**
| Conjunto | Artigos | % do cluster | Enriquecimento vs grafo |
| --- | --- | --- | --- |
| Unicamp/IC | 21 | 10.99% | 2.87x |
| Top-tier | 0 | 0.00% | 0.00x |
| Instituições importantes | 34 | 17.80% | 1.16x |
| Alto impacto >=500 citações | 8 | 4.19% | - |

**Caracterização temática.**
| Dimensão | Top valores |
| --- | --- |
| Subfields | Computational Theory and Mathematics (180); Numerical Analysis (150); Control and Systems Engineering (49); Geometry and Topology (26); Computational Mechanics (23) |
| Instituições | Universidade Estadual de Campinas (UNICAMP) (21); La Trobe University (8); Universidad de Sevilla (8); Universidade Estadual Paulista (Unesp) (8); University of Würzburg (7) |
| Veículos | Journal of Optimization Theory and Applications (26); Journal of Mathematical Analysis and Applications (18); Mathematical Programming (17); Optimization (12); SIAM Journal on Optimization (10) |

**Fluxo intercomunidades.**

Principais origens que citam este cluster:

| Cluster origem | Citações | Tamanho | Unicamp | Representante |
| --- | --- | --- | --- | --- |
| 6 | 14 | 2479 | 47 | The Algebraic Eigenvalue Problem |
| 5 | 8 | 2931 | 130 | Fuzzy sets |
| 10 | 5 | 1637 | 119 | Scale-space filtering: A new approach to multi-scale description |
| 4 | 4 | 3062 | 122 | A training algorithm for optimal margin classifiers |
| 0 | 2 | 5739 | 61 | ImageNet classification with deep convolutional neural networks |

Principais destinos citados por este cluster:

| Cluster destino | Citações | Tamanho | Unicamp | Representante |
| --- | --- | --- | --- | --- |
| 6 | 12 | 2479 | 47 | The Algebraic Eigenvalue Problem |
| 5 | 5 | 2931 | 130 | Fuzzy sets |
| 2 | 1 | 3151 | 87 | OpenFlow |
| 3 | 1 | 3065 | 105 | The Google file system |
| 0 | 1 | 5739 | 61 | ImageNet classification with deep convolutional neural networks |

**Artigos representativos por PageRank.**
| Rank | OpenAlex | Ano | Citações | Unicamp | Top-tier | Título |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | W2000983337 | 1981 | 1198 | False | False | On sufficiency of the Kuhn-Tucker conditions |
| 2 | W2111948593 | 1967 | 623 | False | False | The Fritz John necessary optimality conditions in the presence of equality and in... |
| 3 | W2145994306 | 1981 | 338 | False | False | Invex functions and constrained local minima |
| 4 | W4246495354 | 1999 | 247 | False | False | A smoothing method for mathematical programs with equilibrium constraints |
| 5 | W2093246563 | 1990 | 1811 | False | False | Finite-dimensional variational inequality and nonlinear complementarity problems:... |

**Artigos Unicamp mais centrais no cluster.**
| Rank | OpenAlex | Ano | Citações | PageRank | Título |
| --- | --- | --- | --- | --- | --- |
| 1 | W2018812345 | 2005 | 144 | 4.96e-05 | On the Relation between Constant Positive Linear Dependence Condition and Quasinormal... |
| 2 | W1531652711 | 1999 | 23 | 2.1e-05 | Optimally Conditions for Pareto Nonsmooth Nonconvex Programming in Banach Spaces |
| 3 | W2014238350 | 2004 | 23 | 1.71e-05 | Preinvex functions and weak efficient solutions for some vectorial optimization probl... |
| 4 | W1521048074 | 2003 | 77 | 1.64e-05 | A Practical Optimality Condition Without Constraint Qualifications for Nonlinear Prog... |
| 5 | W1998065341 | 2007 | 38 | 1.6e-05 | An inexact-restoration method for nonlinear bilevel programming problems |

**Leitura não trivial.**
- Este cluster tem enriquecimento top-tier de 0.00x e enriquecimento Unicamp de 2.87x. A comparação separa dois fenômenos diferentes: proximidade da cauda extrema de impacto e presença institucional.
- A condutância de saída (0.028) indica quanto o cluster conversa com outras comunidades. Valores altos sugerem papel de circulação/difusão; valores baixos sugerem bloco mais autocontido.
- O par de fluxos de entrada e saída mostra quem alimenta o cluster e para onde ele referencia conhecimento, evitando interpretar comunidade apenas como tema isolado.

### Cluster 32: maior condutância de saída entre clusters grandes

**Perfil estrutural.**
| Métrica | Valor |
| --- | --- |
| Tamanho | 124 |
| Rank por tamanho | 33 |
| Arestas internas | 876 |
| Densidade interna | 0.05744 |
| Condutância de saída | 0.3582 |
| Influência composta | -0.636 |
| Ano médio | 2014.3 |
| Representante | Detecting and reading text in natural scenes |

**Composição e enriquecimento.**
| Conjunto | Artigos | % do cluster | Enriquecimento vs grafo |
| --- | --- | --- | --- |
| Unicamp/IC | 6 | 4.84% | 1.26x |
| Top-tier | 0 | 0.00% | 0.00x |
| Instituições importantes | 26 | 20.97% | 1.36x |
| Alto impacto >=500 citações | 19 | 15.32% | - |

**Caracterização temática.**
| Dimensão | Top valores |
| --- | --- |
| Subfields | Computer Vision and Pattern Recognition (117); Media Technology (49); Artificial Intelligence (20); Computer Networks and Communications (6); Information Systems (4) |
| Instituições | Huazhong University of Science and Technology (12); University of Malaya (11); Nanjing University (10); Chinese Academy of Sciences (9); University of Science and Technology of China (6) |
| Veículos | Pattern Recognition (16); IEEE Transactions on Image Processing (12); Proceedings of the AAAI Conference on Artificial Intelligence (5); International Journal on Document Analysis and Recognition (IJDAR) (4); IEEE Transactions on Intelligent Transportation Systems (4) |

**Fluxo intercomunidades.**

Principais origens que citam este cluster:

| Cluster origem | Citações | Tamanho | Unicamp | Representante |
| --- | --- | --- | --- | --- |
| 0 | 90 | 5739 | 61 | ImageNet classification with deep convolutional neural networks |
| 1 | 14 | 4122 | 177 | Low-dimensional procedure for the characterization of human fac... |
| 21 | 6 | 685 | 29 | Random sample consensus |
| 9 | 3 | 2082 | 28 | The Laplacian Pyramid as a Compact Image Code |
| 13 | 3 | 1529 | 30 | The String-to-String Correction Problem |

Principais destinos citados por este cluster:

| Cluster destino | Citações | Tamanho | Unicamp | Representante |
| --- | --- | --- | --- | --- |
| 0 | 284 | 5739 | 61 | ImageNet classification with deep convolutional neural networks |
| 1 | 76 | 4122 | 177 | Low-dimensional procedure for the characterization of human fac... |
| 10 | 55 | 1637 | 119 | Scale-space filtering: A new approach to multi-scale description |
| 4 | 34 | 3062 | 122 | A training algorithm for optimal margin classifiers |
| 11 | 8 | 1589 | 62 | <i>The Fractal Geometry of Nature</i> |

**Artigos representativos por PageRank.**
| Rank | OpenAlex | Ano | Citações | Unicamp | Top-tier | Título |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | W2131673214 | 2004 | 633 | False | False | Detecting and reading text in natural scenes |
| 2 | W2165569569 | 2005 | 285 | False | False | ICDAR 2005 text locating competition results |
| 3 | W1998042868 | 2011 | 1107 | False | False | End-to-end scene text recognition |
| 4 | W2027883219 | 2004 | 842 | False | False | Text information extraction in images and video: a survey |
| 5 | W2142159465 | 2010 | 1514 | False | False | Detecting text in natural scenes with stroke width transform |

**Artigos Unicamp mais centrais no cluster.**
| Rank | OpenAlex | Ano | Citações | PageRank | Título |
| --- | --- | --- | --- | --- | --- |
| 1 | W2337261827 | 2016 | 15 | 2.19e-05 | Crowdsourced integrity verification of election results |
| 2 | W2001297828 | 2012 | 87 | 1.98e-05 | T-HOG: An effective gradient-based descriptor for single line text regions |
| 3 | W2165707896 | 2013 | 59 | 1.56e-05 | SnooperText: A text detection system for automatic indexing of urban scenes |
| 4 | W2952117232 | 2019 | 20 | 1.43e-05 | The return of software vulnerabilities in the Brazilian voting machine |
| 5 | W3111479991 | 2020 | 16 | 9.31e-06 | Pelee-Text++: A Tiny Neural Network for Scene Text Detection |

**Leitura não trivial.**
- Este cluster tem enriquecimento top-tier de 0.00x e enriquecimento Unicamp de 1.26x. A comparação separa dois fenômenos diferentes: proximidade da cauda extrema de impacto e presença institucional.
- A condutância de saída (0.358) indica quanto o cluster conversa com outras comunidades. Valores altos sugerem papel de circulação/difusão; valores baixos sugerem bloco mais autocontido.
- O par de fluxos de entrada e saída mostra quem alimenta o cluster e para onde ele referencia conhecimento, evitando interpretar comunidade apenas como tema isolado.


## 4. Problemas encontrados
- A análise usa os `community_id` cacheados; se o algoritmo de Louvain for reexecutado com outro seed ou outra versão de grafo, os IDs podem mudar.
- A caracterização por `Subfield` ainda usa frequência bruta; subáreas genéricas podem aparecer em muitos clusters. Um refinamento natural é TF-IDF por comunidade.
- Betweenness restrita a caminhos `A_fonte -> A_Uni` ainda não foi integrada; por isso, o papel de ponte em caminhos mínimos é inferido por fluxo/condutância, não por contagem direta em caminhos.

## 5. Importância e interpretação
- Clusters com top-tier enriquecido mostram onde o grafo concentra a cauda extrema de impacto.
- Clusters com Unicamp enriquecida mostram onde a instituição está mais inserida tematicamente.
- Quando os dois enriquecimentos aparecem juntos, há indício de proximidade entre produção Unicamp e comunidades de alto impacto; quando divergem, o cluster ajuda a separar impacto extremo de presença institucional.
- Fluxos intercomunidades indicam possíveis rotas de disseminação: clusters muito citados por vários outros funcionam como reservatórios de conhecimento; clusters com alta saída funcionam como consumidores/integradores de literatura.

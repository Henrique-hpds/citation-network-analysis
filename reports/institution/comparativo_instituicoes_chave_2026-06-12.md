# Análise instituição: Unicamp e instituições de referência
Data: 2026-06-12
Conjunto de dados: `network.graphml` local + `metrics/article_metrics.csv`

## 1. Motivação
- O checklist final pede pelo menos um estudo de caso institucional além da Unicamp.
- Esta análise compara a Unicamp com instituições internacionais e nacionais que aparecem fortemente no grafo: MIT, Stanford, Carnegie Mellon, USP e Tsinghua.
- As métricas escolhidas seguem o `AGENTS.md`: impacto agregado/per capita, PageRank médio/mediano, amplitude temática, distribuição em clusters e fluxo bilateral de citações com a Unicamp.

## 2. Metodologia
- Artigos foram selecionados por `AFFILIATED_WITH` no GraphML.
- Impacto bruto usa `cited_by_count` do OpenAlex; impacto interno usa in-degree no subgrafo `CITES`.
- Fluxo bilateral preserva a direção de citação: `instituição -> Unicamp` significa artigo da instituição citando artigo Unicamp; `Unicamp -> instituição` significa artigo Unicamp citando artigo da instituição.
- Como artigos podem ter múltiplas afiliações, os conjuntos institucionais não são disjuntos.

## 3. Resultados
Resumo comparativo:

| Instituição | Artigos | Citações/artigo | In-degree/artigo | Mediana PR | Clusters | Cluster dominante | Top-tier | Cita Uni | Citada pela Uni |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Universidade Estadual de Campinas (UNICAMP) | 1.885 | 51.8 | 11.44 | 1.11e-05 | 38 | 1 (9.4%) | 0 | 2.236 | 2.236 |
| Massachusetts Institute of Technology | 467 | 1040.3 | 17.30 | 1.19e-05 | 33 | 0 (12.6%) | 14 | 40 | 415 |
| Stanford University | 568 | 1041.6 | 18.82 | 1.22e-05 | 32 | 0 (13.7%) | 13 | 47 | 496 |
| Carnegie Mellon University | 604 | 576.2 | 10.67 | 9.58e-06 | 29 | 0 (22.0%) | 5 | 85 | 471 |
| Universidade de São Paulo | 395 | 53.6 | 4.45 | 7.44e-06 | 29 | 1 (21.5%) | 0 | 424 | 288 |
| Tsinghua University | 575 | 249.3 | 5.63 | 7.13e-06 | 28 | 0 (39.3%) | 1 | 175 | 174 |

Figura:
- `figs/instituicao_cluster_heatmap_2026-06-12.png`: heatmap instituição x cluster para os 10 clusters mais presentes entre as instituições selecionadas.

Fatos não triviais:
- A Unicamp tem mais artigos no grafo (1885) que cada instituição de referência selecionada, mas isso não implica maior impacto per capita; Stanford University tem 1041.6 citações OpenAlex por artigo.
- O fluxo bilateral mais intenso com a Unicamp, entre as instituições analisadas, é de Universidade de São Paulo (424 citações para Unicamp, 288 citações recebidas da Unicamp no subgrafo). Isso mede proximidade estrutural direta, não apenas reputação.
- A distribuição por clusters distingue amplitude temática de concentração: uma instituição pode ter muitos artigos, mas concentrados em poucos clusters, enquanto outra ocupa menos artigos e mais comunidades.


### Universidade Estadual de Campinas (UNICAMP)

| Métrica | Valor |
| --- | --- |
| Artigos no GraphML | 1.885 |
| Citações OpenAlex por artigo | 51.8 |
| In-degree interno por artigo | 11.44 |
| Mediana PageRank | 1.11e-05 |
| Clusters com presença | 38 |
| Cluster dominante | 1 (9.4% dos artigos) |
| Representante do cluster dominante | Low-dimensional procedure for the characterization of human faces |
| Subfields distintos | 129 |
| Cita artigos Unicamp | 2.236 |
| É citado por artigos Unicamp | 2.236 |

Subfields dominantes: Artificial Intelligence (797); Computer Networks and Communications (480); Computer Vision and Pattern Recognition (472); Computational Theory and Mathematics (371); Information Systems (254).

Artigos mais centrais por PageRank:

| Rank | OpenAlex | Ano | Citações | Cluster | Top-tier | Título |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | W2142276208 | 1996 | 5342 | 9 | False | A new, fast, and efficient image codec based on set partitioning in hierarchical ... |
| 2 | W2040340473 | 2014 | 4829 | 2 | False | Software-Defined Networking: A Comprehensive Survey |
| 3 | W2913642042 | 2002 | 1515 | 5 | False | Uncertain rule-based fuzzy logic systems: introduction and new directions |
| 4 | W2082511574 | 2013 | 1437 | 26 | False | V-REP: A versatile and scalable robot simulation framework |
| 5 | W2138674095 | 1996 | 616 | 9 | False | An image multiresolution representation for lossless and lossy compression |

### Massachusetts Institute of Technology

| Métrica | Valor |
| --- | --- |
| Artigos no GraphML | 467 |
| Citações OpenAlex por artigo | 1040.3 |
| In-degree interno por artigo | 17.30 |
| Mediana PageRank | 1.19e-05 |
| Clusters com presença | 33 |
| Cluster dominante | 0 (12.6% dos artigos) |
| Representante do cluster dominante | ImageNet classification with deep convolutional neural networks |
| Subfields distintos | 72 |
| Cita artigos Unicamp | 40 |
| É citado por artigos Unicamp | 415 |

Subfields dominantes: Artificial Intelligence (192); Computer Vision and Pattern Recognition (143); Computer Networks and Communications (119); Computational Theory and Mathematics (77); Signal Processing (68).

Artigos mais centrais por PageRank:

| Rank | OpenAlex | Ano | Citações | Cluster | Top-tier | Título |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | W1970026646 | 1974 | 3055 | 13 | False | The String-to-String Correction Problem |
| 2 | W2176566884 | 1993 | 3727 | 19 | False | A generalized processor sharing approach to flow control in integrated services n... |
| 3 | W2138451337 | 1991 | 13744 | 1 | True | Eigenfaces for Recognition |
| 4 | W2010365467 | 2000 | 2423 | 2 | False | The click modular router |
| 5 | W2101927907 | 1987 | 7853 | 5 | True | An introduction to computing with neural nets |

### Stanford University

| Métrica | Valor |
| --- | --- |
| Artigos no GraphML | 568 |
| Citações OpenAlex por artigo | 1041.6 |
| In-degree interno por artigo | 18.82 |
| Mediana PageRank | 1.22e-05 |
| Clusters com presença | 32 |
| Cluster dominante | 0 (13.7% dos artigos) |
| Representante do cluster dominante | ImageNet classification with deep convolutional neural networks |
| Subfields distintos | 70 |
| Cita artigos Unicamp | 47 |
| É citado por artigos Unicamp | 496 |

Subfields dominantes: Artificial Intelligence (215); Computer Vision and Pattern Recognition (170); Computer Networks and Communications (157); Computational Theory and Mathematics (118); Signal Processing (75).

Artigos mais centrais por PageRank:

| Rank | OpenAlex | Ano | Citações | Cluster | Top-tier | Título |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | W1981663184 | 1970 | 3891 | 14 | False | The Art of Computer Programming. Volume 2: Seminumerical Algorithms. |
| 2 | W2147118406 | 2008 | 8381 | 2 | True | OpenFlow |
| 3 | W2156186849 | 1976 | 14389 | 14 | False | New directions in cryptography |
| 4 | W2022758041 | 2008 | 1435 | 2 | False | NOX |
| 5 | W2120900812 | 2007 | 760 | 2 | False | Ethane |

### Carnegie Mellon University

| Métrica | Valor |
| --- | --- |
| Artigos no GraphML | 604 |
| Citações OpenAlex por artigo | 576.2 |
| In-degree interno por artigo | 10.67 |
| Mediana PageRank | 9.58e-06 |
| Clusters com presença | 29 |
| Cluster dominante | 0 (22.0% dos artigos) |
| Representante do cluster dominante | ImageNet classification with deep convolutional neural networks |
| Subfields distintos | 67 |
| Cita artigos Unicamp | 85 |
| É citado por artigos Unicamp | 471 |

Subfields dominantes: Artificial Intelligence (296); Computer Vision and Pattern Recognition (214); Computer Networks and Communications (180); Information Systems (103); Signal Processing (73).

Artigos mais centrais por PageRank:

| Rank | OpenAlex | Ano | Citações | Cluster | Top-tier | Título |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | W1498436455 | 1986 | 30286 | 0 | True | Learning representations by back-propagating errors |
| 2 | W2117671523 | 1989 | 2637 | 0 | False | Phoneme recognition using time-delay neural networks |
| 3 | W2164284397 | 2005 | 656 | 2 | False | A clean slate 4D approach to network control and management |
| 4 | W1807098818 | 2006 | 307 | 2 | False | SANE: a protection architecture for enterprise networks |
| 5 | W2135099885 | 2009 | 3653 | 3 | False | The Case for VM-Based Cloudlets in Mobile Computing |

### Universidade de São Paulo

| Métrica | Valor |
| --- | --- |
| Artigos no GraphML | 395 |
| Citações OpenAlex por artigo | 53.6 |
| In-degree interno por artigo | 4.45 |
| Mediana PageRank | 7.44e-06 |
| Clusters com presença | 29 |
| Cluster dominante | 1 (21.5% dos artigos) |
| Representante do cluster dominante | Low-dimensional procedure for the characterization of human faces |
| Subfields distintos | 92 |
| Cita artigos Unicamp | 424 |
| É citado por artigos Unicamp | 288 |

Subfields dominantes: Artificial Intelligence (179); Computer Vision and Pattern Recognition (122); Computational Theory and Mathematics (70); Computer Networks and Communications (66); Information Systems (46).

Artigos mais centrais por PageRank:

| Rank | OpenAlex | Ano | Citações | Cluster | Top-tier | Título |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | W2052154033 | 1985 | 101 | 14 | False | Streets of Byzantium: Network Architectures for Fast Reliable Broadcasts |
| 2 | W2004143918 | 2010 | 463 | 8 | False | Non-Markovian dynamics of quantum discord |
| 3 | W2510806995 | 2016 | 161 | 6 | False | Worst-case evaluation complexity for unconstrained nonlinear optimization using h... |
| 4 | W2136706845 | 1995 | 429 | 10 | False | Dynamic programming for detecting, tracking, and matching deformable contours |
| 5 | W3097185012 | 2020 | 196 | 0 | False | Fighting Hate Speech, Silencing Drag Queens? Artificial Intelligence in Content M... |

### Tsinghua University

| Métrica | Valor |
| --- | --- |
| Artigos no GraphML | 575 |
| Citações OpenAlex por artigo | 249.3 |
| In-degree interno por artigo | 5.63 |
| Mediana PageRank | 7.13e-06 |
| Clusters com presença | 28 |
| Cluster dominante | 0 (39.3% dos artigos) |
| Representante do cluster dominante | ImageNet classification with deep convolutional neural networks |
| Subfields distintos | 68 |
| Cita artigos Unicamp | 175 |
| É citado por artigos Unicamp | 174 |

Subfields dominantes: Computer Vision and Pattern Recognition (303); Artificial Intelligence (231); Computer Networks and Communications (126); Signal Processing (74); Information Systems (63).

Artigos mais centrais por PageRank:

| Rank | OpenAlex | Ano | Citações | Cluster | Top-tier | Título |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | W2126210439 | 2009 | 1359 | 2 | False | BCube |
| 2 | W2143065961 | 2008 | 988 | 2 | False | Dcell |
| 3 | W4256524787 | 2008 | 549 | 2 | False | Dcell |
| 4 | W3015608194 | 2020 | 4452 | 7 | False | Structure of Mpro from SARS-CoV-2 and discovery of its inhibitors |
| 5 | W2204750386 | 2015 | 4527 | 0 | False | Scalable Person Re-identification: A Benchmark |


## 4. Problemas encontrados
- Afiliações OpenAlex podem refletir coautorias múltiplas; portanto, a soma de artigos por instituição excede o número de artigos únicos.
- `cited_by_count` é externo ao subgrafo e mede impacto amplo; `in_degree` mede apenas citações internas no GraphML. As duas métricas não são substitutas.
- Fluxos bilaterais são baixos para algumas instituições porque o grafo foi construído a partir de caminhos e filtros, não como universo completo de todas as citações institucionais.

## 5. Importância e interpretação
- A comparação mostra que o impacto da Unicamp precisa ser lido em duas escalas: volume de presença no grafo e impacto/permeabilidade estrutural por artigo.
- Instituições com alto impacto per capita ajudam a calibrar a distância entre produção local e centros internacionais de alta centralidade.
- O fluxo bilateral com Unicamp aponta candidatos para estudos de caso de colaboração/influência indireta, especialmente quando coincide com clusters onde a Unicamp já é presente.

# Análise artigo: estudos de caso
Data: 2026-06-12
Conjunto de dados: `metrics/article_metrics.csv`, `metrics/path_bridge_metrics.csv`, `network.graphml`

## 1. Motivação
- O checklist final exige 2-3 estudos de caso de artigos específicos analisados em profundidade.
- Estes três casos cobrem papéis complementares: um artigo-ponte recorrente nos caminhos até a Unicamp, um artigo central da própria Unicamp e um artigo top-tier dominante em PageRank.

## 2. Metodologia
- Os artigos foram escolhidos a partir dos caches já produzidos: `path_bridge_metrics.csv` e `article_metrics.csv`.
- A análise combina métricas de artigo (`in_degree`, `PageRank`, `authority`, `hub`, taxa normalizada por idade) com contexto local (vizinhança, cluster, afiliação, subfield e participação em caminhos).
- O objetivo não é esgotar cada artigo, mas explicar por que ele importa estruturalmente.

## 3. Resultados

## W2148043549

Motivação do estudo de caso: artigo-ponte recorrente nos caminhos top-tier -> Unicamp.

| Métrica | Valor |
| --- | --- |
| Título | The NP-completeness column: An ongoing guide |
| Ano | 1986 |
| Citações OpenAlex | 803 |
| In-degree | 14 |
| Out-degree | 31 |
| PageRank | 6.42e-05 |
| Authority | 3.06e-08 |
| Hub | 1.9e-07 |
| Taxa de citação por idade | 20.07 |
| Cluster | 14 |
| Cluster representante |  |
| Unicamp | False |
| Top-tier | False |
| Instituição importante | False |

Contexto:
- Instituições: AT&T (United States).
- Subfields: Artificial Intelligence, Computational Theory and Mathematics, Hardware and Architecture.
- Veículo: Journal of Algorithms.
- Participação em caminhos: {'top_tier_to_unicamp': 1, 'institutions_to_unicamp': 1}.

Vizinhança recebida mais central (quem o cita):

| OpenAlex | Ano | Cluster | PageRank | Título |
| --- | --- | --- | --- | --- |
| W1660562555 | 1997 | 14 | 0.000471 | Handbook of applied cryptography |
| W1991755800 | 1989 | 25 | 3.18e-05 | Linear time algorithms for NP-hard problems restricted to partial k-trees |
| W2081069741 | 2000 | 25 | 2.04e-05 | Finding Skew Partitions Efficiently |
| W2064796716 | 1991 | 25 | 1.35e-05 | Easy problems for tree-decomposable graphs |
| W1972004026 | 1995 | 3 | 1.3e-05 | Identifying the Minimal Transversals of a Hypergraph and Related Problems |

Vizinhança referenciada mais central (quem ele cita):

| OpenAlex | Ano | Cluster | PageRank | Título |
| --- | --- | --- | --- | --- |
| W2156186849 | 1976 | 14 | 0.00192 | New directions in cryptography |
| W1996360405 | 1983 | 14 | 0.000976 | A method for obtaining digital signatures and public-key cryptosystems |
| W4232836212 | 1978 | 14 | 0.000703 | A method for obtaining digital signatures and public-key cryptosystems |
| W2108834246 | 1985 | 14 | 0.000282 | A public key cryptosystem and a signature scheme based on discrete logarithms |
| W2012329067 | 1981 | 25 | 0.000218 | The ellipsoid method and its consequences in combinatorial optimization |

Leitura:
- Este artigo ocupa um papel diferente dos demais estudos de caso: artigo-ponte recorrente nos caminhos top-tier -> Unicamp. A utilidade do caso é mostrar que impacto extremo, centralidade institucional e recorrência em caminhos não são a mesma coisa.
- O contraste entre `in_degree`, `PageRank` e participação em caminhos ajuda a evitar leituras simplistas: um artigo pode ser muito citado sem necessariamente servir de ponte, ou pode servir de ponte sem estar no topo do impacto bruto.

## W2040340473

Motivação do estudo de caso: artigo da Unicamp com PageRank elevado.

| Métrica | Valor |
| --- | --- |
| Título | Software-Defined Networking: A Comprehensive Survey |
| Ano | 2014 |
| Citações OpenAlex | 4829 |
| In-degree | 1349 |
| Out-degree | 361 |
| PageRank | 0.00148 |
| Authority | 0.000169 |
| Hub | 1.97e-06 |
| Taxa de citação por idade | 402.42 |
| Cluster | 2 |
| Cluster representante |  |
| Unicamp | True |
| Top-tier | False |
| Instituição importante | False |

Contexto:
- Instituições: Gesellschaft für wissenschaftliche Datenverarbeitung mbH Göttingen, Queen Mary University of London, Universidade Estadual de Campinas (UNICAMP), University of Lisbon, University of Luxembourg.
- Subfields: Computer Networks and Communications, Electrical and Electronic Engineering.
- Veículo: Proceedings of the IEEE.
- Participação em caminhos: {'top_tier_to_unicamp': 1}.

Vizinhança recebida mais central (quem o cita):

| OpenAlex | Ano | Cluster | PageRank | Título |
| --- | --- | --- | --- | --- |
| W2260783129 | 2015 | 2 | 0.000153 | Network Function Virtualization: State-of-the-Art and Research Challenges |
| W2594560857 | 2017 | 3 | 0.000107 | Mobile Edge Computing: A Survey on Architecture and Computation Offloading |
| W2218937857 | 2015 | 2 | 9.92e-05 | Mininet-WiFi: Emulating software-defined wireless networks |
| W2275015310 | 2015 | 2 | 5.41e-05 | A Survey of Security in Software Defined Networks |
| W2112700013 | 2015 | 2 | 5.13e-05 | Security in Software Defined Networks: A Survey |

Vizinhança referenciada mais central (quem ele cita):

| OpenAlex | Ano | Cluster | PageRank | Título |
| --- | --- | --- | --- | --- |
| W2147118406 | 2008 | 2 | 0.00213 | OpenFlow |
| W2022758041 | 2008 | 2 | 0.00158 | NOX |
| W2120900812 | 2007 | 2 | 0.00129 | Ethane |
| W2164284397 | 2005 | 2 | 0.00102 | A clean slate 4D approach to network control and management |
| W2144553078 | 2001 | 2 | 0.00097 | Resilient overlay networks |

Leitura:
- Este artigo ocupa um papel diferente dos demais estudos de caso: artigo da Unicamp com PageRank elevado. A utilidade do caso é mostrar que impacto extremo, centralidade institucional e recorrência em caminhos não são a mesma coisa.
- O contraste entre `in_degree`, `PageRank` e participação em caminhos ajuda a evitar leituras simplistas: um artigo pode ser muito citado sem necessariamente servir de ponte, ou pode servir de ponte sem estar no topo do impacto bruto.

## W2163605009

Motivação do estudo de caso: artigo top-tier com maior PageRank no grafo.

| Métrica | Valor |
| --- | --- |
| Título | ImageNet classification with deep convolutional neural networks |
| Ano | 2017 |
| Citações OpenAlex | 75671 |
| In-degree | 3913 |
| Out-degree | 16 |
| PageRank | 0.00395 |
| Authority | 0.0508 |
| Hub | 9.65e-05 |
| Taxa de citação por idade | 8407.89 |
| Cluster | 0 |
| Cluster representante |  |
| Unicamp | False |
| Top-tier | True |
| Instituição importante | False |

Contexto:
- Instituições: Google (United States), OpenAI (United States), University of Toronto.
- Subfields: Artificial Intelligence, Computer Vision and Pattern Recognition.
- Veículo: Communications of the ACM.
- Participação em caminhos: não apareceu entre os principais artigos-ponte cacheados.

Vizinhança recebida mais central (quem o cita):

| OpenAlex | Ano | Cluster | PageRank | Título |
| --- | --- | --- | --- | --- |
| W2194775991 | 2016 | 0 | 0.00126 | Deep Residual Learning for Image Recognition |
| W2102605133 | 2014 | 0 | 0.0012 | Rich Feature Hierarchies for Accurate Object Detection and Semantic Segmentation |
| W2097117768 | 2015 | 0 | 0.0011 | Going deeper with convolutions |
| W2117539524 | 2015 | 0 | 0.000692 | ImageNet Large Scale Visual Recognition Challenge |
| W2155893237 | 2014 | 0 | 0.000683 | Caffe |

Vizinhança referenciada mais central (quem ele cita):

| OpenAlex | Ano | Cluster | PageRank | Título |
| --- | --- | --- | --- | --- |
| W2108598243 | 2009 | 0 | 0.00135 | ImageNet: A large-scale hierarchical image database |
| W2154579312 | 1989 | 0 | 0.00134 | Handwritten Digit Recognition with a Back-Propagation Network |
| W2097117768 | 2015 | 0 | 0.0011 | Going deeper with convolutions |
| W1576445103 | 2007 | 0 | 0.000757 | Caltech-256 Object Category Dataset |
| W2911964244 | 2001 | 11 | 0.00068 | Random Forests |

Leitura:
- Este artigo ocupa um papel diferente dos demais estudos de caso: artigo top-tier com maior PageRank no grafo. A utilidade do caso é mostrar que impacto extremo, centralidade institucional e recorrência em caminhos não são a mesma coisa.
- O contraste entre `in_degree`, `PageRank` e participação em caminhos ajuda a evitar leituras simplistas: um artigo pode ser muito citado sem necessariamente servir de ponte, ou pode servir de ponte sem estar no topo do impacto bruto.


## 4. Problemas encontrados
- A vizinhança usada aqui é de 1 salto para manter a leitura compacta; uma análise posterior pode expandir para 2 saltos.
- A participação em caminhos depende do cache `path_bridge_metrics.csv`, que por sua vez depende dos caminhos podados do ETL.

## 5. Importância e interpretação
- Os três estudos de caso fecham a lacuna entre análises agregadas e exemplos concretos.
- Eles ajudam a mostrar que a rede contém artigos com funções distintas: reservatórios de impacto, transmissores de conhecimento e âncoras institucionais.

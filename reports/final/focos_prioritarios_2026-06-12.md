# Análise final: pontos prioritários de foco para a próxima iteração
Data: 2026-06-12
Conjunto de dados: `metrics/article_metrics.csv`, `metrics/cluster_metrics.csv`, `metrics/path_bridge_metrics.csv`, `metrics/a_inter_metrics.csv`, `metrics/institution_metrics.csv`

## 1. Motivação
- A auditoria deixou claro que o problema agora não é apenas "calcular mais métricas", mas escolher **bons alvos de aprofundamento**.
- Este relatório não tenta encerrar a análise. Ele organiza os **focos mais promissores** para a próxima iteração, com exemplos concretos e argumentos sobre por que cada um deles pode gerar achados substantivos.
- A postura aqui é deliberadamente seletiva e cética: um bom foco não é só algo que parece interessante, mas algo que combina sinal estrutural, interpretabilidade e relevância para os objetivos do projeto.

## 2. Metodologia
- Os focos abaixo foram priorizados com base em três critérios:
  1. **Sinal estrutural**: o fenômeno aparece em mais de uma métrica, ou em mais de um relatório/cache.
  2. **Interpretabilidade**: há exemplos reais de artigos, clusters ou instituições que permitem traduzir o número em narrativa analítica.
  3. **Relevância para o projeto**: o foco ajuda a responder impacto da Unicamp, estrutura de comunidades ou disseminação de conhecimento.
- Como contrapeso, cada foco também traz sua **principal limitação**. A ideia é evitar priorizar apenas o que parece chamativo.

## 3. Resultados

### 3.1 Resumo executivo dos focos

| Prioridade | Foco | Evidência concreta | Por que vale aprofundar | Risco principal |
| --- | --- | --- | --- | --- |
| 1 | `A_inter_graphml` e corredores de disseminação | `A_inter_graphml = 8.525` nós; artigos-ponte recorrentes como `W2148043549`; percentis de caminho `8` vs `4` | Toca o objetivo central do projeto: papel de intermediários entre elite e Unicamp | Cobertura parcial do GraphML frente aos caminhos brutos |
| 2 | Contraste cluster `0` vs cluster `1` | cluster `0`: 84 top-tier, influência `2,842`; cluster `1`: 177 artigos Unicamp, influência `2,201` | É a melhor comparação comunidade-comunidade entre elite extrema e presença forte da Unicamp | Partição depende do Louvain em `resolution=1.0` |
| 3 | USP como estudo institucional de acoplamento com a Unicamp | fluxo `USP -> Unicamp = 424`, `Unicamp -> USP = 288` | Mostra proximidade estrutural real, não só prestígio abstrato | Fluxos dependem do corpus filtrado, não do OpenAlex completo |
| 4 | Artigos da Unicamp que parecem pontes reais, não só artigos fortes | `W2040340473` tem maior betweenness entre Unicamp intermediários; `W2131884039` aparece 15 vezes nos caminhos | Permite sair da leitura “Unicamp só está longe da elite” para “onde ela intermedeia circulação” | Betweenness foi aproximada no subgrafo de caminhos |
| 5 | Clusters `13` e `14` como corredores recorrentes | vários artigos-ponte mais frequentes estão em `13`/`14`, mesmo sem esses clusters liderarem em volume bruto de `A_inter` | Bom foco para explicar caminhos longos e intermediação teórica/sistêmica | Parte dos nós mais frequentes dos caminhos fica fora do GraphML |

### 3.2 Foco 1: `A_inter_graphml` e corredores de disseminação

Este é o foco mais forte da próxima iteração.

Evidência:
- `A_inter` bruto nos caminhos soma `79.022` nós únicos, mas só `8.525` aparecem no GraphML final como `A_inter_graphml`.
- Mesmo nessa versão visível e incompleta, `A_inter_graphml` já é grande o suficiente para análise própria.
- A diferença entre caminhos `top-tier -> Unicamp` e `instituições -> Unicamp` é robusta em toda a distribuição:
  - top-tier: `p25=7`, `mediana=8`, `p90=10`;
  - instituições: `p25=4`, `mediana=4`, `p90=7`.
- O percentil 90 de betweenness no subgrafo de caminhos separa `A_inter_graphml` de `A_Uni`:
  - `A_inter_graphml`: `1,74e-05`;
  - `A_Uni`: `0`.

Exemplos verdadeiros:
- `W2148043549` — *The NP-completeness column: An ongoing guide*:
  - frequência total em caminhos: `7.406`;
  - cluster `14`;
  - aparece tanto em caminhos top-tier quanto institucionais.
- `W2040340473` — *Software-Defined Networking: A Comprehensive Survey*:
  - artigo da Unicamp;
  - maior betweenness no subgrafo de caminhos entre os artigos observados (`0,002299...`);
  - mostra que alguns artigos da Unicamp não são apenas destino, mas também corredor.

Argumento:
- Este foco é o mais alinhado com a pergunta “como o conhecimento chega à Unicamp?”.
- Além disso, ele permite separar três papéis diferentes que estavam meio misturados nas iterações anteriores:
  - artigo de elite;
  - artigo de destino final da Unicamp;
  - artigo intermediário que efetivamente costura trajetórias.

Limitação:
- A maior parte do `A_inter` bruto segue fora do GraphML. Portanto, qualquer conclusão aqui deve ser apresentada como “sobre o subconjunto visível no corpus final”.

### 3.3 Foco 2: contraste cluster `0` vs cluster `1`

Se eu tivesse de escolher um par de comunidades para análise comparativa aprofundada, seria este.

Evidência:
- cluster `0`:
  - `5.739` nós;
  - `84` artigos top-tier;
  - influência composta `2,842`;
  - `1.490` nós de `A_inter_graphml`.
- cluster `1`:
  - `4.122` nós;
  - `177` artigos Unicamp;
  - influência composta `2,201`;
  - `873` nós de `A_inter_graphml`.

Exemplos verdadeiros:
- representante do cluster `0`: *ImageNet classification with deep convolutional neural networks*.
- representante do cluster `1`: *Low-dimensional procedure for the characterization of human faces*.
- artigo Unicamp central no cluster `0`: `W2119880843` — *Toward Open Set Recognition*.

Argumento:
- O cluster `0` parece ser o melhor retrato do polo de impacto extremo.
- O cluster `1` parece ser o melhor retrato de uma comunidade onde a Unicamp realmente tem massa crítica e conectividade.
- Comparar esses dois clusters pode gerar um resultado mais rico do que estudar “o cluster da Unicamp” isoladamente: a comparação explicita o que muda entre presença institucional, elite e circulação.

Limitação:
- Isso ainda depende da partição de Louvain em `resolution=1.0`, que já sabemos não ser a única possível.

### 3.4 Foco 3: USP como estudo institucional de acoplamento com a Unicamp

Este foco vale porque ele contraria uma expectativa superficial.

Evidência:
- USP:
  - `395` artigos;
  - `53,6` citações por artigo;
  - cluster dominante `1` com `21,5%` dos artigos.
- Fluxo bilateral com a Unicamp:
  - `USP -> Unicamp = 424`;
  - `Unicamp -> USP = 288`.
- Para comparação, instituições muito mais prestigiadas internacionalmente aparecem com acoplamento direto menor na ida:
  - Stanford `47 -> Unicamp`;
  - MIT `40 -> Unicamp`.

Exemplos verdadeiros:
- A USP compartilha com a Unicamp o cluster dominante `1`, o que já sugere proximidade temática/estrutural.
- Ao mesmo tempo, ela não tem impacto per-article muito acima da Unicamp. Então o caso não é “USP como elite extrema”, mas “USP como parceiro estrutural plausível”.

Argumento:
- Este é um ótimo estudo de caso para diferenciar “prestígio global” de “proximidade no corpus”.
- Também pode produzir um capítulo institucional mais explicável para o relatório: por que a USP aparece tão colada à Unicamp no grafo, e em quais comunidades isso acontece.

Limitação:
- Como o corpus foi construído por caminhos e filtros, esses fluxos não devem ser tratados como medida absoluta da relação institucional real.

### 3.5 Foco 4: artigos da Unicamp que parecem pontes reais

Nem todo artigo forte da Unicamp é um bom artigo-ponte. Esse foco vale justamente por separar essas duas coisas.

Exemplos verdadeiros:
- `W2040340473` — *Software-Defined Networking: A Comprehensive Survey*:
  - PageRank muito alto para a Unicamp (`0,001477...`);
  - `4.829` citações OpenAlex;
  - maior betweenness no subgrafo de caminhos entre os candidatos observados.
- `W2131884039` — *A relevance feedback method based on genetic programming...*:
  - artigo da Unicamp;
  - frequência total em caminhos: `15`;
  - betweenness `0,000231...`;
  - cluster `1`.
- `W2115284428` — *The image foresting transform: theory, algorithms, and applications*:
  - betweenness `0,000375...`;
  - cluster `10`;
  - mostra que o papel de ponte da Unicamp não está concentrado em um único tema.

Argumento:
- Este foco ajuda a evitar uma leitura muito agregada da Unicamp.
- Em vez de dizer apenas “a Unicamp está distante da elite”, ele permite perguntar: **quais artigos da Unicamp funcionam como pontos de passagem, e em quais áreas?**

Limitação:
- O ranking depende do subgrafo de caminhos, não do grafo inteiro, e usa betweenness aproximada.

### 3.6 Foco 5: clusters `13` e `14` como corredores recorrentes

Este é talvez o foco mais subestimado das iterações anteriores.

Evidência:
- Os clusters `13` e `14` não lideram `A_inter` por volume bruto, mas concentram muitos dos artigos-ponte mais recorrentes:
  - cluster `13`: `W2024332685`, `W2004618348`, `W2071939274`, `W2088300760`, `W4251893211`, `W2027501230`;
  - cluster `14`: `W2148043549`, `W2131929623`.
- Isso sugere que eles funcionam mais como **corredores de uso intenso** do que como grandes reservatórios de intermediários.

Exemplos verdadeiros:
- `W2024332685` — *On Live-Dead Analysis for Global Data Flow Problems* (`3.886` ocorrências).
- `W2004618348` — *Parallelism in random access machines* (`3.753` ocorrências).
- `W2148043549` — *The NP-completeness column: An ongoing guide* (`7.406` ocorrências).

Argumento:
- Esse foco pode render um resultado bonito e não trivial: a disseminação para a Unicamp não parece passar só pelos maiores clusters de IA/visão, mas também por um corredor forte de teoria e sistemas clássicos.
- Isso também ajuda a explicar por que alguns caminhos ficam longos: eles atravessam áreas que funcionam como infraestrutura conceitual, não necessariamente como centros contemporâneos de elite.

Limitação:
- Parte importante dos caminhos mais frequentes segue fora do GraphML; então esses clusters explicam o corredor visível, não o corredor completo.

### 3.7 Falsos bons focos: coisas que parecem fortes, mas ainda são frágeis

Nem tudo que aparece “enriquecido” merece prioridade.

Casos a tratar com cautela:
- **cluster `32`**:
  - maior fração de `A_inter` por tamanho (`36,3%`);
  - mas só `45` nós de `A_inter`, influência `-0,636`.
  - Pode ser interessante depois, mas ainda parece pequeno demais para virar foco principal.
- **cluster `28`**:
  - maior enriquecimento relativo de top-tier (`2,50x`);
  - mas são apenas `3` artigos top-tier em `198` nós.
  - Bom exemplo de número que chama atenção, mas pode ser ruído de baixa contagem.
- **clusters muito pequenos com alta razão Unicamp/intermediários**:
  - podem render histórias sedutoras, mas ainda são vulneráveis a instabilidade estatística e à própria sensibilidade do Louvain.

Argumento:
- Vale mais investir nos focos onde sinal, escala e interpretabilidade andam juntos do que perseguir outliers pequenos cedo demais.

## 4. Problemas encontrados
- Os focos desta lista dependem do corpus visível no GraphML final; isso afeta especialmente qualquer prioridade ligada a `A_inter`.
- Algumas métricas de ponte foram calculadas no subgrafo de caminhos, não no grafo inteiro.
- Este relatório não substitui a necessidade de baseline aleatório, tabela-resumo consolidada e fechamento documental do relatório final.

## 5. Importância e interpretação
- O ganho desta rodada não é “provar uma conclusão nova”, mas reduzir a dispersão da próxima iteração.
- Se a equipe quiser maximizar retorno analítico por esforço, os melhores investimentos agora parecem ser:
  1. `A_inter_graphml` e os artigos-ponte;
  2. contraste cluster `0` vs `1`;
  3. USP como estudo institucional;
  4. artigos específicos da Unicamp com papel de ponte;
  5. clusters `13` e `14` como corredores recorrentes.
- Em outras palavras: o próximo avanço não precisa vir de medir tudo, e sim de aprofundar os lugares onde o grafo já começou a contar uma história consistente.

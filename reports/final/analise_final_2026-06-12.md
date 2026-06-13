# Análise final: impacto, comunidades e disseminação no grafo de citações
Data: 2026-06-12
Conjunto de dados: `network.graphml` local + caches em `metrics/` + caminhos em `data/output/`

## 1. Motivação
- Este relatório final sintetiza as análises feitas para responder à pergunta central do projeto: como a produção associada à Unicamp/IC se posiciona em relação a artigos de impacto extremo, instituições relevantes e comunidades estruturais do grafo.
- A síntese é deliberadamente cética: quando um resultado depende fortemente de definição operacional, filtragem do ETL ou cobertura incompleta do GraphML, isso é tratado como limitação substantiva, não apenas detalhe técnico.
- Em particular, `top-tier` aqui **não** significa prestígio de venue; significa o conjunto operacional derivado de `data/responses_1/_top_cited_cs`.

## 2. Metodologia
- Fonte estrutural principal: subgrafo `Article -[:CITES]-> Article` do `network.graphml`, com 49.196 artigos e 385.529 arestas `CITES`.
- Conjuntos usados:
  - `A_Uni`: artigos com afiliação à Unicamp (`I181391015`).
  - `Top-tier ETL`: artigos marcados por `is_top_tier`, derivados apenas de `_top_cited_cs`.
  - `Instituições importantes`: artigos marcados por `is_important_institution`, derivados de `by_institution`, excluindo o diretório `by_institution/unicamp`.
- Métricas efetivamente usadas nesta etapa: componentes fracas/fortes, PageRank, HITS, Louvain, distribuições de in-degree, `cited_by_count`, taxa de citação por idade, composição e fluxo intercomunidades, distâncias/caminhos fornecidos pelo ETL.
- Testes de robustez realizados:
  - sensibilidade do Louvain à resolução (`0.5`, `1.0`, `1.5`, `2.0`);
  - sensibilidade da definição de elite, trocando `Top-tier ETL` por `>=500 citações` e `Top 1% por citações`.
- Nem todas as métricas previstas no `AGENTS.md` foram executadas. Em especial, betweenness global exata, harmonic/closeness por componente e eigenvector não entraram nesta rodada final.

## 3. Resultados

### 3.1 Estrutura global do grafo
- O resultado mais sólido desta etapa é estrutural: o grafo tem um único componente fraco dominante e é quase acíclico.
- No GraphML final, o maior WCC contém 49.012 dos 49.196 artigos (99,63%), enquanto há 48.298 SCCs e a maior SCC tem apenas 34 nós.
- Interpretação: a maior parte do corpus participa de um mesmo espaço de circulação de conhecimento, mas essa circulação tem direção temporal muito marcada. Em termos práticos, isso torna plausível usar o grafo para discutir disseminação, mas pouco plausível interpretar ciclos como fenômeno central.

### 3.2 Desigualdade de impacto
- A concentração de impacto é extrema. O top 1% por PageRank concentra 29,2% do PageRank total; o top 1% por in-degree concentra 24,6% das citações internas `CITES`.
- Esse padrão é coerente com todas as definições de elite testadas:
  - `Top-tier ETL`: mediana de 11.615 citações e 0,000185 de PageRank;
  - `Top 1% por citações`: mediana de 8.438 citações e 0,000146 de PageRank;
  - `>=500 citações`: mediana de 1.084 citações e 0,0000335 de PageRank.
- Conclusão: a rede é fortemente hierárquica, e essa hierarquia não desaparece quando trocamos a definição do grupo de elite.

### 3.3 Unicamp versus elite e instituições relevantes
- A Unicamp aparece com forte presença volumétrica, mas não com impacto per-article comparável ao grupo de elite.
- O conjunto `A_Uni` contém 1.885 artigos, mais do que cada uma das instituições de referência analisadas isoladamente. Ainda assim, sua mediana é baixa frente à elite:
  - Unicamp: mediana `in_degree=4`, `PageRank=1,11e-05`, `cited_by_count=22`, taxa/idade `=1,90`;
  - Top-tier ETL: mediana `in_degree=98`, `PageRank=1,85e-04`, `cited_by_count=11.615`, taxa/idade `=556,8`.
- Os testes estatísticos reforçam que isso não é ruído amostral:
  - para `cited_by_count`, Unicamp vs. top-tier: `KS=0,999`, `p≈5,35e-227`; `Mann-Whitney p≈6,2e-170`;
  - para `citation_rate_by_age`, Unicamp vs. top-tier: `KS=0,997`, `p≈2,81e-226`; `Mann-Whitney p≈1,2e-169`.
- O contraste com `Instituições importantes` é menor, mas continua relevante. Isso sugere que a distância da Unicamp para a elite extrema é muito maior que a distância para uma elite institucional mais ampla.
- Resultado importante e fácil de exagerar: a Unicamp **não está ausente** do grafo; ela está espalhada por 39 comunidades. O que ela não mostra, neste corpus, é presença forte na cauda mais extrema de impacto.

### 3.4 Comunidades: o que parece real e o que parece contingente
- O Louvain com `resolution=1.0` produziu 38 comunidades no maior WCC, com modularidade `0,7841`.
- O ponto cético aqui importa: a maior modularidade apareceu em `resolution=0.5` (`0,8346`), não em `1.0`. Portanto, os `community_id` usados nos relatórios devem ser lidos como uma partição útil para interpretação, não como “a” decomposição verdadeira do grafo.
- Ainda assim, algumas regularidades sobrevivem à sensibilidade:
  - a comunidade `0` concentra a maior influência composta e o maior número de artigos de elite;
  - a comunidade `1` concentra a maior presença absoluta da Unicamp;
  - a comunidade `29` tem a maior fração relativa da Unicamp entre clusters com pelo menos 100 artigos, mas baixa influência global.
- Isso permite uma leitura mais refinada:
  - **cluster 0**: polo de alto impacto e forte concentração de elite, sobretudo em visão computacional/IA; a Unicamp está presente, mas sub-representada;
  - **cluster 1**: comunidade importante para a Unicamp e ainda conectada à elite, com forte circulação intercomunitária;
  - **cluster 29**: nicho relativamente mais “doméstico” para a Unicamp, com maior densidade e menor condutância, mas pouca influência externa.
- O achado não trivial aqui é que presença da Unicamp e centralidade global não coincidem automaticamente. A Unicamp ocupa melhor alguns bolsões temáticos do que o núcleo mais concentrado de elite.

### 3.5 Instituições: volume não é o mesmo que impacto
- A comparação institucional reforça a mesma história em outra escala.
- A Unicamp tem 1.885 artigos no GraphML, contra 568 de Stanford e 467 do MIT. Mas em citações OpenAlex por artigo:
  - Unicamp: `51,8`;
  - Stanford: `1041,6`;
  - MIT: `1040,3`.
- Isso elimina uma leitura confortável, mas frágil: a de que forte presença volumétrica equivaleria a forte centralidade relativa.
- Ao mesmo tempo, a USP aparece como o fluxo bilateral mais intenso com a Unicamp (`424 -> Uni`, `288 <- Uni`), o que sugere proximidade estrutural direta mais forte que a observada com instituições internacionais de maior prestígio.
- Interpretação cuidadosa: esse resultado fala de conectividade dentro deste corpus filtrado, não de “maior influência real” da USP em sentido amplo.

### 3.6 Caminhos e disseminação: resultado promissor, mas o mais frágil
- A análise de caminhos aponta uma diferença clara entre dois tipos de proximidade:
  - `top-tier -> Unicamp`: mediana de 8 arestas;
  - `instituições -> Unicamp`: mediana de 4 arestas.
- Mesmo retirando os 382 caminhos de tamanho zero do baseline institucional (0,53% do total), a mediana institucional permanece 4. Logo, a conclusão não depende desse artefato.
- A interpretação substantiva é plausível: a Unicamp está estruturalmente mais próxima de uma elite institucional ampla do que da cauda extrema de artigos mais citados.
- Mas este é também o resultado que mais pede cautela:
  - apenas 51,6% das fontes top-tier aparecem no cache do GraphML;
  - apenas 6,9% das fontes institucionais aparecem no cache do GraphML;
  - só 25,3% das ocorrências intermediárias dos caminhos top-tier e 33,6% das institucionais puderam ser mapeadas de volta para o GraphML.
- Isso significa que os caminhos são bons como evidência de tendência, mas ruins como inventário temático completo dos corredores de disseminação.
- Mesmo com essa limitação, alguns intermediários reaparecem de forma consistente, como `W2148043549` (“The NP-completeness column: An ongoing guide”), o que sugere que certos artigos funcionam como pontes recorrentes entre regiões bem distintas do corpus.

## 4. Problemas encontrados
- O esquema foi confirmado pelo `network.graphml`, não por inspeção viva do Neo4j. Isso é suficiente para a análise local, mas não garante equivalência perfeita com o banco operacional.
- O corpus não é “OpenAlex completo”; ele é um recorte construído por ETL e por caminhos. Isso afeta sobretudo leituras de distância, fluxo institucional e cobertura temática.
- `Top-tier` é uma convenção operacional do ETL, não uma verdade ontológica sobre prestígio científico.
- Há sobreposição entre conjuntos:
  - `Top-tier ∩ Unicamp = 0`;
  - `Instituições importantes ∩ Unicamp = 358`;
  - `Top-tier ∩ Instituições importantes = 69`.
  Essa sobreposição não invalida a análise, mas mostra que os conjuntos não são blocos sociais totalmente separados.
- As análises de caminhos são as mais limitadas pela cobertura: grande parte dos intermediários não está no GraphML final e, portanto, não recebe cluster, título confiável nem centralidade.
- A partição em comunidades é sensível à resolução do Louvain. Ela é útil como lente interpretativa, mas não deve ser tratada como fato natural fixo.

## 5. Importância e interpretação
- A conclusão principal que resiste melhor ao escrutínio é esta: **a Unicamp está estruturalmente presente em muitas regiões do grafo, mas não ocupa, neste corpus, a mesma faixa de centralidade e impacto per-article do grupo de elite extrema**.
- Essa conclusão é sustentada por várias métricas ao mesmo tempo: `cited_by_count`, in-degree, PageRank, taxa de citação por idade, distribuição por comunidades e testes estatísticos.
- Uma segunda conclusão robusta é que **a produção associada à Unicamp parece mais próxima de uma elite institucional ampla do que da cauda mais extrema de artigos top-cited**. Isso aparece nas distribuições comparativas e nos caminhos.
- A conclusão mais interessante, mas menos segura, é a de disseminação por “corredores” comunitários específicos. Ela é sugestiva e coerente com o restante da estrutura, mas depende demais de caminhos podados e de cobertura incompleta do GraphML para ser tratada como evidência definitiva.
- Se eu tivesse de resumir a leitura final em uma frase: o grafo mostra uma Unicamp distribuída e conectada, com nichos temáticos fortes e boa inserção em comunidades relevantes, mas ainda distante do núcleo mais concentrado de impacto extremo.

## 6. Próximos passos sugeridos
- Reexecutar a etapa de caminhos com cobertura mais próxima do corpus final do GraphML, para reduzir a fração de intermediários “fora do cache”.
- Materializar explicitamente `A_inter` no cache para fechar a lacuna entre análise de comunidades e análise de disseminação.
- Rodar uma segunda análise de robustez variando não só Louvain e definição de elite, mas também o limiar de citações usado no ETL de caminhos.
- Consolidar estas evidências em um relatório final do projeto organizado por objetivo, não por script.

# Metodologia analítica extraída dos exemplos

Data: 2026-06-12

Os PDFs em `entregas/examples/` mostram um padrão de análise que deve orientar os relatórios deste projeto: métricas só são úteis quando explicam um fato estrutural não trivial do grafo.

## Princípios

- Explicar a função da métrica antes do número: grau identifica hubs locais; betweenness identifica pontes; PageRank mede impacto propagado; modularidade indica separação comunitária.
- Comparar contra uma expectativa: rede aleatória, mediana do grafo, grupos de referência, componente gigante, top 1%, ou distribuição de outro conjunto.
- Interpretar extremos: top-k e bottom-k ajudam a transformar uma métrica abstrata em exemplos concretos.
- Usar métricas complementares em conjunto: grau alto sem betweenness pode indicar popularidade local; betweenness alta com baixa clusterização sugere ponte intercomunitária.
- Documentar alternativas descartadas: quando uma métrica ou filtro é inadequado, explicar o motivo e o risco analítico.
- Produzir uma conclusão causal com cautela: dizer o que a topologia sugere, não o que ela prova.

## Aplicação ao grafo de citações

- `is_top_tier` deve representar apenas artigos top-cited do corpus `_top_cited_cs`.
- `is_important_institution` deve representar artigos dos diretórios `by_institution`, separado de top-tier.
- Comparações Unicamp vs. top-tier medem distância em relação à cauda extrema de impacto.
- Comparações Unicamp vs. instituições importantes medem posição relativa frente a uma elite institucional ampla.
- Comunidades devem ser descritas por coesão interna, fluxo externo, representante estrutural, participação da Unicamp e interpretação temática.
- Caminhos mínimos devem ser usados para identificar transmissores de conhecimento, não apenas distâncias médias.

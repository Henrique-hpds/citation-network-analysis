# Scripts de análise de comunidades

## Ordem

1. Gerar a partição Louvain oficial:

```bash
.venv/bin/python ./louvain_partition_robustness.py
```

2. Atribuir `article_set` aos artigos e persistir no Neo4j:

```bash
.venv/bin/python ./assign_article_sets.py
```

3. Calcular métricas internas e externas das comunidades:

```bash
.venv/bin/python ./community_internal_impact.py
.venv/bin/python ./community_external_impact.py
```

4. Calcular centralidades agregadas por comunidade.

Este passo calcula betweenness exata e pode demorar bastante:

```bash
.venv/bin/python ./community_aggregate_centrality.py
```

5. Calcular influência composta:

```bash
.venv/bin/python ./community_influence_score.py
```

6. Calcular subcampo dominante e presença da Unicamp:

```bash
.venv/bin/python ./community_theme_counts.py
.venv/bin/python ./unicamp_community_presence.py
```

7. Rodar modelos nulos:

```bash
.venv/bin/python ./community_null_model_significance.py
```

8. Gerar tabela final de posição intra-cluster da Unicamp:

```bash
.venv/bin/python ./unicamp_intracluster_report_table.py
```

9. Gerar as figuras usadas no relatório:

```bash
.venv/bin/python ./community_impact_figures.py
```

10. Gerar o grafo agregado de comunidades:

```bash
.venv/bin/python ./community_graph_plain.py --min-citation-percent 3
```

Exemplos úteis:

```bash
.venv/bin/python ./community_graph_plain.py --min-citation-percent 3 --color-metric fraction
.venv/bin/python ./community_graph_plain.py --min-citation-percent 3 --min-node-distance 0.75
```

## Scripts adicionais

`louvain_hypothesis_test.py` roda a análise antiga de hipótese Louvain diretamente a partir do Neo4j:

```bash
.venv/bin/python ./louvain_hypothesis_test.py
```

`community_aggregate_centrality copy.py` é uma cópia antiga do cálculo de centralidade agregada. Se precisar rodá-la:

```bash
.venv/bin/python "./community_aggregate_centrality copy.py"
```

## Módulo auxiliar

`helper_functions.py` não deve ser executado diretamente. Ele contém funções compartilhadas de carregamento de grafo, leitura de partição, PageRank, k-core, escrita de CSV e acesso ao Neo4j.


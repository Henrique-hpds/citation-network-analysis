# Índice de relatórios

| Data | Tipo | Identificador | Relatório | Resumo |
| --- | --- | --- | --- | --- |
| 2026-06-12 | global | GraphML final | [visão geral](global/visao_geral_2026-06-12.md) | Métricas estruturais, PageRank, Louvain e problemas de schema/caches. |
| 2026-06-12 | comparativo | Unicamp vs top-tier/instituições | [comparativo](comparativo/unicamp_vs_alto_impacto_2026-06-12.md) | Distribuições usando `is_top_tier` e `is_important_institution` derivados do ETL. |
| 2026-06-12 | cluster | 1 | [cluster 1](cluster/1_2026-06-12.md) | Comunidade com maior presença absoluta da Unicamp/IC. |
| 2026-06-12 | cluster | clusters selecionados | [análise específica](cluster/analise_especifica_clusters_2026-06-12.md) | Análise interpretável de clusters por influência, Unicamp, top-tier, instituições e fluxo. |
| 2026-06-12 | caminhos | top-tier/instituições -> Unicamp | [disseminação](caminhos/disseminacao_top_instituicoes_unicamp_2026-06-12.md) | Distribuição de caminhos, baseline institucional e artigos/clusters-ponte. |
| 2026-06-12 | institution | Unicamp e referências | [instituições](institution/comparativo_instituicoes_chave_2026-06-12.md) | Impacto institucional, clusters e fluxo bilateral com a Unicamp. |
| 2026-06-12 | comparativo | testes estatísticos | [testes](comparativo/testes_estatisticos_2026-06-12.md) | KS e Mann-Whitney com p-valor para Unicamp, top-tier e instituições. |
| 2026-06-12 | article | estudos de caso | [artigos](article/estudos_de_caso_artigos_2026-06-12.md) | Três artigos analisados em profundidade: ponte, Unicamp central e top-tier. |
| 2026-06-12 | cluster | sensibilidade Louvain | [resolução](cluster/sensibilidade_louvain_2026-06-12.md) | Comparação de modularidade e número de comunidades para múltiplas resoluções. |
| 2026-06-12 | comparativo | robustez de conjunto | [robustez](comparativo/robustez_conjuntos_elite_2026-06-12.md) | Sensibilidade das conclusões à definição de artigos de elite/alto impacto. |
| 2026-06-12 | final | síntese cética | [análise final](final/analise_final_2026-06-12.md) | Síntese final com graus de confiança, resultados robustos e limites substantivos. |
| 2026-06-12 | final | focos prioritários | [focos](final/focos_prioritarios_2026-06-12.md) | Mapa argumentado dos melhores alvos para aprofundar a próxima iteração. |
| 2026-06-12 | caminhos | A_inter e disseminacao | [A_inter](caminhos/intermediarios_disseminacao_2026-06-12.md) | Materializacao de intermediarios, metricas de ponte e fluxo entre clusters. |

## Pendências
- `auditoria_2026-06-12.md`: ainda falta baseline de grafo aleatório para contextualizar distâncias/densidades.
- `auditoria_2026-06-12.md`: ainda faltam estudos de caso mais profundos para pelo menos uma instituição e mais clusters/artigos além dos já analisados.
- `auditoria_2026-06-12.md`: ainda falta consolidar uma tabela-resumo única por conjunto (`A_TT`, `A_Uni`, `A_Inst`, `A_inter`) e por cluster relevante.
- `auditoria_2026-06-12.md`: ainda faltam itens documentais do relatório final (resumo, trabalhos relacionados, referências, apêndices, mapeamento objetivo-a-objetivo).

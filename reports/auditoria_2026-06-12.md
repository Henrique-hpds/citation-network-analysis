# Auditoria de checklist — `analise_final_2026-06-12.md`

> Objetivo deste documento: aplicar `FINAL_REPORT_CHECKLIST.md` retroativamente
> ao relatório `analise_final_2026-06-12.md`, item a item, para deixar
> explícito **quão longe** esse relatório está de poder ser chamado de
> "análise final", e que tipo de trabalho falta para fechar cada lacuna.
>
> Convenção de status: `✅ atendido` / `⚠️ parcial` / `❌ pendente` /
> `➖ não aplicável (justificado)`.
>
> Importante: o relatório auditado é **bom como síntese intermediária** — é
> honesto, cético e bem escrito. O problema não é a qualidade da prosa, é o
> título/enquadramento ("análise final") frente ao que o checklist exige.

## 1. Estrutura geral do documento

| Item | Status | Evidência / motivo |
|---|---|---|
| Título, autores, resumo | ⚠️ parcial | Tem título e data; não tem autores nem um resumo no estilo "pergunta → método → conclusão principal". |
| Introdução com objetivos revisados | ⚠️ parcial | A seção "Motivação" situa o relatório, mas não retoma, um a um, os objetivos específicos da proposta para dizer o que mudou/foi refinado. |
| Trabalhos relacionados | ❌ pendente | Não há nenhuma seção contextualizando as métricas (PageRank, HITS, Louvain) frente à literatura. |
| Metodologia | ⚠️ parcial | Seção 2 existe e é clara sobre conjuntos e fontes, mas não documenta parâmetros de PageRank/HITS (damping factor, nº de iterações) nem seeds do Louvain. |
| Resultados organizados por objetivo | ⚠️ parcial | A organização é temática (estrutura, desigualdade, Unicamp vs. elite, comunidades, instituições, caminhos) e cobre boa parte dos objetivos, mas não há mapeamento explícito "objetivo X da proposta → seção Y deste relatório". |
| Discussão | ✅ atendido | Seção 5 cumpre esse papel de forma sólida. |
| Limitações | ✅ atendido | Seção 4 é específica e bem fundamentada (cobertura do GraphML, definição operacional de "top-tier", sobreposição de conjuntos). |
| Conclusão e trabalhos futuros | ⚠️ parcial | Seção 6 lista próximos passos, mas não há uma conclusão separada que sintetize "o que este relatório estabelece" de forma fechada (isso está espalhado pela Seção 5). |
| Referências | ❌ pendente | Nenhuma referência bibliográfica. |
| Apêndices | ❌ pendente | Nenhum apêndice (ex.: lista de instituições, tabelas grandes). |

**Subtotal: 2 atendidos / 5 parciais / 3 pendentes (de 10)**

## 2. Resultados — mapeados aos objetivos da proposta

| Item | Status | Evidência / motivo |
|---|---|---|
| Estrutura geral atualizada do grafo | ✅ atendido | Seção 3.1 traz números de WCC/SCC consistentes com o relatório parcial. |
| Detecção de comunidades: número, tamanhos, modularidade | ⚠️ parcial | Número (38) e modularidade (0,7841) são dados; a **distribuição de tamanhos** das 38 comunidades não é apresentada (nem tabela, nem figura). |
| Sensibilidade do Louvain à resolução | ✅ atendido | 4 valores testados (0,5 / 1,0 / 1,5 / 2,0), com resultado reportado e discutido. |
| Caracterização de cada cluster relevante | ⚠️ parcial | Apenas 3 de 38 clusters (0, 1, 29) são caracterizados, e mesmo esses sem nó representativo nomeado (título do artigo) nem perfil temporal explícito (ano médio/dispersão). |
| Matriz/heatmap de fluxo intercomunidades | ❌ pendente | "forte circulação intercomunitária" é afirmado em prosa, sem matriz, tabela ou figura. |
| Distribuição completa das distâncias mínimas, com percentis | ⚠️ parcial | Apenas medianas são reportadas (8 e 4 arestas); sem p10/p25/p75/p90, sem histograma. |
| Comparação das distâncias com baseline (grafo aleatório) | ❌ pendente | Nenhum baseline de grafo aleatório é usado em nenhuma métrica do relatório. |
| Identificação de artigos-ponte | ⚠️ parcial | Apenas 1 exemplo (`W2148043549`) é citado; não há uma identificação sistemática (top-N por frequência em caminhos mínimos). |
| Comparação Unicamp vs. top-tier vs. instituições (distribuições completas) | ⚠️ parcial | Medianas + testes KS/Mann-Whitney são reportados (bom), mas sem percentis/IQR/visualização das distribuições inteiras. |
| Testes estatísticos com estatística + p-valor | ✅ atendido | KS e Mann-Whitney reportados com valores para `cited_by_count` e `citation_rate_by_age`. |
| Estudos de caso (clusters, artigos, instituição) | ⚠️ parcial | Clusters: 3 (razoável). Artigos específicos: apenas 1 (`W2148043549`), checklist pede 2-3. Instituição: Stanford/MIT/USP aparecem em comparação numérica, mas nenhuma é tratada como estudo de caso em profundidade. |
| Visualização geral do grafo/subgrafo | ❌ pendente | Não há nenhuma figura referenciada no relatório. |

**Subtotal: 3 atendidos / 6 parciais / 3 pendentes (de 12)**

## 3. Critérios de qualidade da análise

| Item | Status | Evidência / motivo |
|---|---|---|
| Triangulação (≥2 métricas por conclusão) | ✅ atendido | A conclusão principal (Seção 5) é sustentada por `cited_by_count`, in-degree, PageRank, taxa por idade, comunidades e testes estatísticos. |
| Baseline para todo número absoluto | ⚠️ parcial | Há robustez testada para *definição de elite* (3 definições) e *resolução do Louvain* (4 valores), mas não há baseline para densidade, distância, ou tamanho de componente. |
| Tradução quantitativo → qualitativo (o que o nó/cluster *é*) | ⚠️ parcial | Cluster 0 é descrito tematicamente ("visão computacional/IA") e `W2148043549` tem título; mas clusters 1 e 29, e a maioria dos demais números, não têm essa tradução. |
| Achados inesperados discutidos | ✅ atendido | Ex.: modularidade máxima em resolução 0,5 (não 1,0); fluxo USP↔Unicamp mais forte que com instituições de maior prestígio. |
| Teste de robustez adicional (além do Louvain) | ✅ atendido | Definição de elite testada com 3 variantes. |
| Limitações de dados discutidas | ✅ atendido | Seção 4, bem específica (cobertura GraphML, sobreposição de conjuntos, operacionalização de "top-tier"). |

**Subtotal: 4 atendidos / 2 parciais / 0 pendentes (de 6)**

## 4. Rigor e reprodutibilidade

| Item | Status | Evidência / motivo |
|---|---|---|
| Repositório de código referenciado | ❌ pendente | Não há link/menção ao repositório nem a instruções de execução. |
| Versão/data de extração dos dados explicitada | ⚠️ parcial | Diz "GraphML local + caches + caminhos", mas sem hash/commit/data de extração. |
| Parâmetros + seeds documentados | ⚠️ parcial | Resoluções do Louvain são documentadas; damping factor do PageRank, parâmetros do HITS e seed do Louvain não são mencionados. |
| Tabela-resumo final consolidando métricas-chave por conjunto/cluster | ❌ pendente | Os números aparecem espalhados em prosa; não há uma tabela única consolidando `A_TT`/`A_Uni`/`A_Inst`/`A_inter` × métricas. |

**Subtotal: 0 atendidos / 2 parciais / 2 pendentes (de 4)**

## 5. Conexão com o plano de trabalho

| Item | Status | Evidência / motivo |
|---|---|---|
| Comparação cronograma planejado vs. executado | ❌ pendente | Não abordado. |
| Para cada objetivo específico da proposta, indicação de status | ❌ pendente | Não há mapeamento objetivo a objetivo. |

**Subtotal: 0 atendidos / 0 parciais / 2 pendentes (de 2)**

---

## Resultado agregado

| Categoria | Itens | % do total (34) |
|---|---|---|
| ✅ Atendido | 9 | 26% |
| ⚠️ Parcial | 15 | 44% |
| ❌ Pendente | 10 | 29% |

**Conclusão da auditoria**: aproximadamente **1 em cada 4 itens** do checklist
está de fato atendido. A maioria está "parcial" — ou seja, o tema foi tocado,
mas não na profundidade exigida (medianas sem distribuição completa, 3 de 38
clusters caracterizados, 1 artigo-ponte citado isoladamente, etc.). Isso é
consistente com uma **boa segunda ou terceira iteração**, não com um
relatório final.

## Lacuna estrutural mais importante: métricas de `A_inter`

O próprio relatório admite (Seção 2) que **betweenness global, closeness/harmonic
por componente, eigenvector e a materialização explícita de `A_inter`** não
entraram nesta rodada. Essas são justamente as métricas que, segundo a
proposta original (Objetivo 3) e o `AGENTS.md` (Seção 3.1), sustentam a
discussão sobre **papel da Unicamp na disseminação de conhecimento** — que é
tratada na Seção 3.6/5 do relatório como a conclusão "mais interessante,
porém menos segura".

Ou seja: a conclusão mais ligada ao objetivo central do projeto é
exatamente a que repousa sobre as métricas que ainda não foram calculadas.
Isso não invalida o que já foi feito, mas significa que **o objetivo central
do projeto continua pendente**, independentemente de quão bem-escritas
estejam as outras seções.

## Plano recomendado para a próxima iteração (em ordem de prioridade)

1. **Materializar `A_inter`** no cache (conforme já listado como próximo
   passo) e calcular betweenness (aproximada, se necessário),
   closeness/harmonic por componente, eigenvector e HITS — ao menos para os
   conjuntos `A_inter`, `A_Uni` e `A_TT`. Sem isso, o objetivo central do
   projeto permanece pendente.
2. **Gerar as visualizações que faltam**: heatmap de fluxo intercomunidades,
   histograma/CDF das distâncias `A_Uni ↔ A_TT` e `A_Uni ↔ A_Inst` com
   percentis, e ao menos uma visualização do subgrafo de uma comunidade
   relevante (ex.: cluster 0 ou 1).
3. **Tabela-resumo consolidada** por conjunto (`A_TT`, `A_Uni`, `A_Inst`,
   `A_inter`) e por cluster relevante, com todas as métricas-chave.
4. **Expandir estudos de caso**: caracterizar mais clusters (pelo menos
   top-5 por tamanho ou por influência composta, não só 0/1/29, cada um com
   nó representativo nomeado e perfil temporal); adicionar 1-2 artigos
   específicos além de `W2148043549`; tratar uma instituição (ex.: USP, dado
   o fluxo bilateral encontrado) como estudo de caso completo.
5. **Baseline de grafo aleatório** para ao menos distância e densidade, para
   dar contexto aos números absolutos já calculados.
6. **Identificação sistemática de artigos-ponte** (tabela top-N por
   frequência em caminhos mínimos, com cluster e título).
7. Só depois de 1–6: fechar a estrutura documental (resumo, introdução com
   objetivos revisados, trabalhos relacionados, referências, apêndices,
   mapeamento objetivo-a-objetivo da proposta, comparação de cronograma).

## Recomendação de nomenclatura

Renomear este documento de `analise_final_2026-06-12.md` para algo como
`iteracao_03_unicamp_vs_elite_e_comunidades.md`, e tratá-lo como insumo para
a próxima iteração — não como o relatório final do projeto. Ver
`AGENTS.md`, Seção 9.1, para a convenção de nomenclatura e a tabela de
autoauditoria que deve acompanhar cada iteração a partir de agora.
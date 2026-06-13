# FINAL_REPORT_CHECKLIST.md — Checklist do Relatório Final (MC859A)

> Este checklist define o que precisa estar presente no relatório final para
> que ele constitua uma **boa análise** (não apenas descritiva). O agente
> definido em `AGENTS.md` deve usar este arquivo como critério de parada:
> a análise está "completa" quando todos os itens abaixo podem ser marcados
> como atendidos, com evidência (tabela, figura, texto) localizável no
> relatório.

## 1. Estrutura geral do documento

- [ ] Título, autores, resumo (o resumo antecipa pergunta de pesquisa, método
      e principal conclusão — não apenas "construímos um grafo").
- [ ] Introdução com objetivos revisados em relação à proposta original,
      indicando explicitamente o que mudou/foi refinado.
- [ ] Seção de trabalhos relacionados (breve), contextualizando as métricas
      escolhidas (PageRank, HITS, Louvain, etc.) frente à literatura de
      cienciometria/redes de citação.
- [ ] Metodologia: dados, construção do grafo (recap resumido do parcial),
      algoritmos e parâmetros usados nesta etapa.
- [ ] Resultados organizados por objetivo (não por script/execução).
- [ ] Discussão (interpretação cruzada dos resultados).
- [ ] Limitações.
- [ ] Conclusão e trabalhos futuros.
- [ ] Referências.
- [ ] Apêndices.

## 2. Resultados — mapeados aos objetivos da proposta

- [ ] Estrutura geral atualizada do grafo (tabelas/figuras de tamanho,
      componentes, distribuição de graus), reapresentadas apenas se algo
      mudou desde o relatório parcial.
- [ ] Detecção de comunidades: número de comunidades, distribuição de
      tamanhos, modularidade obtida.
- [ ] Teste de sensibilidade do parâmetro de resolução do Louvain
      (pelo menos 2-3 valores testados e comparados, com justificativa do
      valor escolhido).
- [ ] Caracterização de cada cluster relevante: rótulo temático
      (`Subfield`/`Venue` dominantes), instituições dominantes, perfil
      temporal, participação da Unicamp/IC, nó representativo.
- [ ] Matriz/heatmap de fluxo de citação intercomunidades.
- [ ] Distribuição completa (não só média) das distâncias mínimas
      `A_Uni` ↔ `A_TT` (e premiados, se identificáveis), com percentis.
- [ ] Comparação dessas distâncias com um baseline (grafo aleatório de
      tamanho/grau similar, ou outro par de conjuntos de referência).
- [ ] Identificação de artigos-ponte (alta frequência em caminhos mínimos) e
      a qual(is) cluster(s) pertencem.
- [ ] Comparação Unicamp vs. top-tier vs. outras instituições: distribuições
      (não só médias) de PageRank, in-degree, betweenness, taxa de citação
      normalizada por idade.
- [ ] Testes estatísticos de diferença entre grupos (ex.: Mann-Whitney/KS),
      com resultado reportado (estatística + p-valor).
- [ ] Pelo menos 1-2 estudos de caso de clusters, 2-3 artigos específicos e
      1 instituição (além da Unicamp), analisados em profundidade.
- [ ] Pelo menos uma visualização geral do grafo ou de um subgrafo/comunidade,
      além dos gráficos de distribuição já presentes no parcial.

## 3. Critérios de qualidade da análise (não apenas descrição)

- [ ] Cada conclusão importante é sustentada por mais de uma métrica
      (triangulação).
- [ ] Todo número absoluto relevante (distância média, densidade, etc.) é
      comparado a algum baseline (grafo aleatório, outro cluster, outra
      instituição, grafo todo).
- [ ] Sempre que um nó/cluster importante é identificado, há explicação do
      *que ele é* (título do artigo, subárea, instituição) — não apenas o
      número/ID.
- [ ] Achados inesperados ou que contrariam a hipótese inicial são discutidos
      explicitamente, não omitidos.
- [ ] Há pelo menos um teste de robustez/sensibilidade de parâmetro além do
      Louvain (ex.: limiar de 10 citações, escolha das 30 instituições).
- [ ] Limitações de dados são discutidas (cobertura do OpenAlex, viés do
      filtro de citações, possíveis lacunas em `Subfield`/`Institution`).

## 4. Rigor e reprodutibilidade

- [ ] Repositório de código atualizado e referenciado, com instruções de
      execução.
- [ ] Versão/data de extração dos dados explicitada.
- [ ] Parâmetros de todos os algoritmos documentados, incluindo *seeds* para
      algoritmos não-determinísticos (Louvain).
- [ ] Tabela-resumo final consolidando as métricas-chave por conjunto
      (`A_TT`, `A_Uni`, `A_Inst`, `A_inter`) e por cluster relevante.

## 5. Conexão com o plano de trabalho

- [ ] Comparação entre o cronograma planejado (Tabela I da proposta) e o
      executado, com reflexão sobre desvios.
- [ ] Para cada objetivo específico da proposta: indicação explícita se foi
      atingido integralmente, parcialmente (com justificativa), ou
      substituído por outra análise (com justificativa).

---

## Como usar este checklist (para o agente)

- Antes de declarar uma rodada de análise como "final", percorrer todos os
  itens marcando `[x]` apenas quando houver evidência localizável (caminho
  do arquivo/figura/tabela) que comprove o item.
- Para cada item ainda não marcado, registrar em `reports/index.md` (ou em
  uma seção "Pendências") qual análise adicional é necessária para atendê-lo.
- Itens marcados como "não aplicável" (ex.: premiados Nobel/Turing não
  identificáveis nos dados) devem ser justificados explicitamente, não apenas
  ignorados.
# Análise comparativa: robustez da definição de elite
Data: 2026-06-12
Conjunto de dados: `metrics/article_metrics.csv`

## 1. Motivação
- O checklist final ainda pedia um teste de robustez além do Louvain.
- A comparação principal do projeto depende de como definimos "artigos de elite". Aqui variamos essa definição para testar se os achados centrais são estáveis ou se dependem demais do recorte original do ETL.

## 2. Metodologia
- Mantivemos `A_Uni` fixo e trocamos apenas o conjunto de referência de alto impacto:
  - `Top-tier ETL`: artigos marcados por `is_top_tier`.
  - `>=500 citações`: artigos com `is_high_impact_500=True`.
  - `Top 1% por citações`: top 1% do próprio corpus GraphML por `cited_by_count` (492 artigos).
- Para cada definição, medimos mediana de impacto/centralidade, espalhamento por comunidades e distância de distribuição em relação à Unicamp por KS empírico.
- Também comparamos as comunidades dominantes de cada definição com as do conjunto `Top-tier ETL`.

## 3. Resultados
Resumo dos conjuntos:

| Conjunto | n | Mediana citações | Mediana in-degree | Mediana PageRank | Mediana taxa/idade | # comunidades | Fração nas top-5 comunidades |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Top-tier ETL | 298 | 11615 | 98 | 0.000185 | 556.83 | 24 | 65.4% |
| >=500 citações | 4.731 | 1084 | 20 | 3.35e-05 | 66.36 | 38 | 54.0% |
| Top 1% por citações | 492 | 8438 | 86 | 0.000146 | 387.64 | 28 | 63.6% |

Distância das distribuições em relação à Unicamp/IC (KS empírico):

| Conjunto de elite | KS citações | KS in-degree | KS PageRank | KS taxa/idade |
| --- | --- | --- | --- | --- |
| Top-tier ETL | 0.999 | 0.791 | 0.794 | 0.997 |
| >=500 citações | 0.993 | 0.504 | 0.485 | 0.941 |
| Top 1% por citações | 0.999 | 0.809 | 0.821 | 0.997 |

Comunidades dominantes e estabilidade temática:

| Conjunto | Top-8 comunidades | Jaccard com top-8 ETL |
| --- | --- | --- |
| Top-tier ETL | 0, 4, 1, 5, 7, 10, 9, 3 | 1.000 |
| >=500 citações | 0, 1, 4, 5, 3, 8, 14, 2 | 0.455 |
| Top 1% por citações | 0, 4, 1, 5, 3, 9, 10, 7 | 1.000 |

Leitura:
- A conclusão principal permaneceu estável em todas as definições: a Unicamp continua muito distante da elite em `cited_by_count`, `PageRank` e taxa normalizada por idade. Em outras palavras, a assimetria observada não depende só do rótulo `is_top_tier`.
- `Top-tier ETL` e `Top 1% por citações` são especialmente consistentes entre si: além de magnitudes parecidas, eles preservam exatamente o mesmo conjunto de 8 comunidades dominantes.
- O recorte `>=500 citações` amplia bastante o conjunto e espalha mais os artigos por comunidades, mas ainda concentra 54.0% dos nós nas 5 comunidades mais fortes. Isso sugere que a elite expandida muda a escala, mas não dissolve os polos estruturais do grafo.
- O fato de a Unicamp aparecer em 39 comunidades enquanto os conjuntos de elite se concentram mais fortemente em poucas comunidades reforça um contraste não trivial: a presença da Unicamp é mais distribuída, enquanto o impacto extremo permanece mais focalizado.

## 4. Problemas encontrados
- Este teste varia a definição do conjunto de elite, não a lista de instituições nem o limiar do ETL original.
- O `Top 1% por citações` é sensível ao corpus observado; se a cobertura do GraphML mudar, o corte muda junto.

## 5. Importância e interpretação
- Esta análise fecha a pendência de robustez além do Louvain com uma verificação diretamente conectada à pergunta central do projeto.
- O resultado mais importante é que a leitura substantiva não colapsa sob redefinições razoáveis do grupo de comparação: a distância entre Unicamp e artigos de elite é robusta, enquanto os polos comunitários da elite também permanecem relativamente estáveis.

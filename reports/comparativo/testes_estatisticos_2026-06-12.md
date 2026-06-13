# Análise comparativa: testes estatísticos
Data: 2026-06-12
Conjunto de dados: `metrics/article_metrics.csv`

## 1. Motivação
- O checklist final exige estatística + p-valor para as diferenças entre Unicamp, top-tier e instituições importantes.
- A análise descritiva anterior já mostrava separação de distribuições; aqui verificamos se essa separação permanece sob testes não paramétricos adequados para caudas longas e muitos empates.

## 2. Metodologia
- Teste KS de duas amostras: compara as distribuições completas.
- Teste Mann-Whitney U: compara a posição relativa/ranqueamento das amostras sem assumir normalidade.
- Como não há `scipy` no ambiente, os p-valores foram obtidos por aproximações assintóticas em Python puro.
- Grupos: `Unicamp/IC`, `Top-tier ETL` e `Instituições importantes` (excluindo top-tier e Unicamp para manter grupos disjuntos).

## 3. Resultados
| Métrica | Comparação | n1 | n2 | Mediana 1 | Mediana 2 | KS | p(KS) | U | z(U) | p(U) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| in_degree | Unicamp/IC vs Top-tier ETL | 1.885 | 298 | 4 | 98 | 0.791 | 1.37e-142 | 28179 | -25.054 | 1.58e-138 |
| in_degree | Unicamp/IC vs Instituições importantes | 1.885 | 7.142 | 4 | 2 | 0.159 | 1.47e-33 | 7857459 | 11.276 | 1.72e-29 |
| pagerank | Unicamp/IC vs Top-tier ETL | 1.885 | 298 | 1.11e-05 | 0.000185 | 0.794 | 1.49e-143 | 27482 | -25.073 | 9.88e-139 |
| pagerank | Unicamp/IC vs Instituições importantes | 1.885 | 7.142 | 1.11e-05 | 7.91e-06 | 0.226 | 4.33e-67 | 8312078 | 15.777 | 4.47e-56 |
| citation_rate_by_age | Unicamp/IC vs Top-tier ETL | 1.885 | 298 | 1.9 | 557 | 0.997 | 2.81e-226 | 148 | -27.763 | 1.2e-169 |
| citation_rate_by_age | Unicamp/IC vs Instituições importantes | 1.885 | 7.142 | 1.9 | 6.3 | 0.380 | 1.21e-188 | 3322503 | -33.872 | 1.74e-251 |
| cited_by_count | Unicamp/IC vs Top-tier ETL | 1.885 | 298 | 22 | 1.16e+04 | 0.999 | 5.35e-227 | 30 | -27.787 | 6.2e-170 |
| cited_by_count | Unicamp/IC vs Instituições importantes | 1.885 | 7.142 | 22 | 73 | 0.382 | 2.61e-190 | 3486180 | -32.249 | 3.66e-228 |

Leitura:
- Para `cited_by_count` e `citation_rate_by_age`, os p-valores muito pequenos reforçam que a distância entre Unicamp e top-tier não é ruído amostral; ela corresponde a uma separação estrutural real no grafo/corpus.
- A comparação Unicamp vs. instituições importantes tende a produzir estatísticas menores que Unicamp vs. top-tier, o que sustenta a interpretação já observada: a elite institucional ampla está mais próxima da Unicamp do que a cauda extrema dos artigos top-tier.
- KS e Mann-Whitney respondem perguntas diferentes; quando ambos apontam diferença forte, a conclusão fica mais robusta.

## 4. Problemas encontrados
- Os p-valores são assintóticos, não exatos, porque o ambiente não tem `scipy`.
- Há muitos empates em métricas discretas como `in_degree`; o teste U foi corrigido por empates, mas continua sendo uma aproximação.

## 5. Importância e interpretação
- Esta evidência fecha a lacuna do checklist sobre significância estatística.
- Ela transforma as diferenças visuais dos histogramas e percentis em afirmações sustentadas por teste formal, o que fortalece a discussão final.

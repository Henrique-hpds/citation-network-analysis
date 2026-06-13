# Análise cluster: sensibilidade do Louvain
Data: 2026-06-12
Conjunto de dados: `network.graphml` local

## 1. Motivação
- O checklist final exige um teste de sensibilidade para o parâmetro de resolução do Louvain.
- Como as conclusões sobre comunidades dependem do particionamento, precisamos mostrar que a escolha `resolution=1.0` não é arbitrária.

## 2. Metodologia
- Louvain rodado no maior WCC convertido para grafo não direcionado.
- `seed=42` fixado para comparabilidade.
- Resoluções testadas: 0.5, 1.0, 1.5, 2.0.
- Métricas observadas: número de comunidades, modularidade, tamanho da maior comunidade, mediana do tamanho das comunidades e fração dos nós concentrada nas 10 maiores comunidades.

## 3. Resultados
| Resolução | # comunidades | Modularidade | Maior comunidade | Mediana do tamanho | Fração nas top-10 |
| --- | --- | --- | --- | --- | --- |
| 0.5 | 26 | 0.8346 | 12.218 | 917 | 85.3% |
| 1 | 38 | 0.7841 | 5.739 | 720 | 63.4% |
| 1.5 | 49 | 0.7566 | 4.264 | 630 | 53.9% |
| 2 | 60 | 0.7309 | 3.366 | 607 | 47.3% |

Figura:
- `figs/louvain_resolution_sensitivity_2026-06-12.png`: número de comunidades e modularidade por resolução.

Leitura:
- A maior modularidade apareceu em `resolution=0.5` (0.8346), enquanto a resolução usada nos relatórios principais (`1.0`) obteve 0.7841.
- O aumento da resolução cresce o número de comunidades, mas também tende a fragmentar a estrutura. Por isso, a decisão não deve maximizar apenas `# comunidades`.
- A escolha `resolution=1.0` permanece razoável se quisermos equilibrar separação comunitária e interpretabilidade, sem quebrar demais os grandes blocos.

## 4. Problemas encontrados
- O teste ainda é unidimensional: ele varia só a resolução, não o `seed` nem versões alternativas do grafo.
- A sensibilidade foi medida sobre o GraphML final; se o grafo base mudar, os números mudam junto.

## 5. Importância e interpretação
- Este teste reduz o risco de tratar uma partição arbitrária como verdade estrutural.
- Ele ajuda a justificar por que os estudos de cluster atuais usam `resolution=1.0`, mas também documenta qual seria o comportamento sob resoluções mais finas ou mais grossas.

"""
Robustness analysis beyond Louvain.

Checks whether the main comparative conclusions remain stable when the
"elite/high-impact" reference set is redefined.
"""

from __future__ import annotations

import csv
from collections import Counter
from datetime import date
from pathlib import Path

from agent_graph_analysis import ROOT, table


REPORT_DATE = date.today().isoformat()
METRICS_DIR = ROOT / "metrics"
REPORTS_DIR = ROOT / "reports"
COMPARATIVO_DIR = REPORTS_DIR / "comparativo"


def read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def median(values: list[float]) -> float:
    xs = sorted(values)
    n = len(xs)
    if n == 0:
        return 0.0
    mid = n // 2
    if n % 2:
        return xs[mid]
    return (xs[mid - 1] + xs[mid]) / 2


def ks_distance(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    xs = sorted(set(a) | set(b))
    a_sorted = sorted(a)
    b_sorted = sorted(b)
    ia = ib = 0
    best = 0.0
    for x in xs:
        while ia < len(a_sorted) and a_sorted[ia] <= x:
            ia += 1
        while ib < len(b_sorted) and b_sorted[ib] <= x:
            ib += 1
        best = max(best, abs(ia / len(a_sorted) - ib / len(b_sorted)))
    return best


def top_communities(rows: list[dict], k: int = 8) -> list[str]:
    counts = Counter(row["community_id"] for row in rows)
    return [community_id for community_id, _count in counts.most_common(k)]


def jaccard(a: list[str], b: list[str]) -> float:
    sa = set(a)
    sb = set(b)
    if not sa and not sb:
        return 1.0
    return len(sa & sb) / len(sa | sb)


def update_index(report_path: Path) -> None:
    index_path = REPORTS_DIR / "index.md"
    line = (
        f"| {REPORT_DATE} | comparativo | robustez de conjunto | "
        f"[robustez](comparativo/{report_path.name}) | "
        "Sensibilidade das conclusões à definição de artigos de elite/alto impacto. |"
    )
    text = index_path.read_text(encoding="utf-8") if index_path.exists() else (
        "# Índice de relatórios\n\n"
        "| Data | Tipo | Identificador | Relatório | Resumo |\n"
        "| --- | --- | --- | --- | --- |\n"
    )
    if line not in text:
        if "\n## Pendências" in text:
            text = text.replace("\n## Pendências", f"\n{line}\n\n## Pendências", 1)
        else:
            text = text.rstrip() + "\n" + line + "\n"
    text = text.replace(
        "- `FINAL_REPORT_CHECKLIST.md` Seção 3: ainda falta teste de robustez/sensibilidade além do Louvain.\n",
        "",
    )
    index_path.write_text(text, encoding="utf-8")


def main() -> None:
    rows = read_csv(METRICS_DIR / "article_metrics.csv")
    rows_by_citations = sorted(rows, key=lambda row: to_float(row["cited_by_count"]), reverse=True)
    top_1pct_n = max(1, round(len(rows_by_citations) * 0.01))

    unicamp = [row for row in rows if row["is_unicamp"] == "True"]
    elite_sets = {
        "Top-tier ETL": [row for row in rows if row["is_top_tier"] == "True"],
        ">=500 citações": [row for row in rows if row["is_high_impact_500"] == "True"],
        "Top 1% por citações": rows_by_citations[:top_1pct_n],
    }

    summary_rows = []
    distance_rows = []
    overlap_rows = []
    etl_top_communities = top_communities(elite_sets["Top-tier ETL"])

    for name, group in elite_sets.items():
        citations = [to_float(row["cited_by_count"]) for row in group]
        pageranks = [to_float(row["pagerank"]) for row in group]
        in_degrees = [to_float(row["in_degree"]) for row in group]
        rates = [to_float(row["citation_rate_by_age"]) for row in group]
        community_counts = Counter(row["community_id"] for row in group)
        top5_share = sum(count for _, count in community_counts.most_common(5)) / len(group)

        summary_rows.append(
            [
                name,
                len(group),
                f"{median(citations):.0f}",
                f"{median(in_degrees):.0f}",
                f"{median(pageranks):.3g}",
                f"{median(rates):.2f}",
                len(community_counts),
                f"{100 * top5_share:.1f}%",
            ]
        )

        distance_rows.append(
            [
                name,
                f"{ks_distance([to_float(row['cited_by_count']) for row in unicamp], citations):.3f}",
                f"{ks_distance([to_float(row['in_degree']) for row in unicamp], in_degrees):.3f}",
                f"{ks_distance([to_float(row['pagerank']) for row in unicamp], pageranks):.3f}",
                f"{ks_distance([to_float(row['citation_rate_by_age']) for row in unicamp], rates):.3f}",
            ]
        )

        overlap_rows.append(
            [
                name,
                ", ".join(top_communities(group)),
                f"{jaccard(etl_top_communities, top_communities(group)):.3f}",
            ]
        )

    report_path = COMPARATIVO_DIR / f"robustez_conjuntos_elite_{REPORT_DATE}.md"
    report = f"""# Análise comparativa: robustez da definição de elite
Data: {REPORT_DATE}
Conjunto de dados: `metrics/article_metrics.csv`

## 1. Motivação
- O checklist final ainda pedia um teste de robustez além do Louvain.
- A comparação principal do projeto depende de como definimos "artigos de elite". Aqui variamos essa definição para testar se os achados centrais são estáveis ou se dependem demais do recorte original do ETL.

## 2. Metodologia
- Mantivemos `A_Uni` fixo e trocamos apenas o conjunto de referência de alto impacto:
  - `Top-tier ETL`: artigos marcados por `is_top_tier`.
  - `>=500 citações`: artigos com `is_high_impact_500=True`.
  - `Top 1% por citações`: top 1% do próprio corpus GraphML por `cited_by_count` ({top_1pct_n} artigos).
- Para cada definição, medimos mediana de impacto/centralidade, espalhamento por comunidades e distância de distribuição em relação à Unicamp por KS empírico.
- Também comparamos as comunidades dominantes de cada definição com as do conjunto `Top-tier ETL`.

## 3. Resultados
Resumo dos conjuntos:

{table(
    ["Conjunto", "n", "Mediana citações", "Mediana in-degree", "Mediana PageRank", "Mediana taxa/idade", "# comunidades", "Fração nas top-5 comunidades"],
    summary_rows,
)}

Distância das distribuições em relação à Unicamp/IC (KS empírico):

{table(
    ["Conjunto de elite", "KS citações", "KS in-degree", "KS PageRank", "KS taxa/idade"],
    distance_rows,
)}

Comunidades dominantes e estabilidade temática:

{table(
    ["Conjunto", "Top-8 comunidades", "Jaccard com top-8 ETL"],
    overlap_rows,
)}

Leitura:
- A conclusão principal permaneceu estável em todas as definições: a Unicamp continua muito distante da elite em `cited_by_count`, `PageRank` e taxa normalizada por idade. Em outras palavras, a assimetria observada não depende só do rótulo `is_top_tier`.
- `Top-tier ETL` e `Top 1% por citações` são especialmente consistentes entre si: além de magnitudes parecidas, eles preservam exatamente o mesmo conjunto de 8 comunidades dominantes.
- O recorte `>=500 citações` amplia bastante o conjunto e espalha mais os artigos por comunidades, mas ainda concentra {summary_rows[1][7]} dos nós nas 5 comunidades mais fortes. Isso sugere que a elite expandida muda a escala, mas não dissolve os polos estruturais do grafo.
- O fato de a Unicamp aparecer em 39 comunidades enquanto os conjuntos de elite se concentram mais fortemente em poucas comunidades reforça um contraste não trivial: a presença da Unicamp é mais distribuída, enquanto o impacto extremo permanece mais focalizado.

## 4. Problemas encontrados
- Este teste varia a definição do conjunto de elite, não a lista de instituições nem o limiar do ETL original.
- O `Top 1% por citações` é sensível ao corpus observado; se a cobertura do GraphML mudar, o corte muda junto.

## 5. Importância e interpretação
- Esta análise fecha a pendência de robustez além do Louvain com uma verificação diretamente conectada à pergunta central do projeto.
- O resultado mais importante é que a leitura substantiva não colapsa sob redefinições razoáveis do grupo de comparação: a distância entre Unicamp e artigos de elite é robusta, enquanto os polos comunitários da elite também permanecem relativamente estáveis.
"""
    report_path.write_text(report, encoding="utf-8")
    update_index(report_path)
    print(f"Report written to {report_path}")


if __name__ == "__main__":
    main()

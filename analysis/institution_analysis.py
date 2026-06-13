"""
Institution-level analysis for the citation graph.

Computes impact, structural centrality, thematic breadth and bilateral citation
flow with Unicamp for selected benchmark institutions.
"""

from __future__ import annotations

import csv
import os
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-citation-network")

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from agent_graph_analysis import ROOT, load_graph, table


REPORT_DATE = date.today().isoformat()
METRICS_DIR = ROOT / "metrics"
REPORTS_DIR = ROOT / "reports"
INSTITUTION_DIR = REPORTS_DIR / "institution"
FIGS_DIR = INSTITUTION_DIR / "figs"

UNICAMP_ID = "I181391015"
SELECTED_INSTITUTIONS = {
    "I181391015": "Universidade Estadual de Campinas (UNICAMP)",
    "I63966007": "Massachusetts Institute of Technology",
    "I97018004": "Stanford University",
    "I74973139": "Carnegie Mellon University",
    "I17974374": "Universidade de São Paulo",
    "I99065089": "Tsinghua University",
}


def read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_bool(value: object) -> bool:
    return str(value) == "True"


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def median(values: list[float]) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    mid = len(xs) // 2
    if len(xs) % 2:
        return xs[mid]
    return (xs[mid - 1] + xs[mid]) / 2


def pct(part: int, total: int) -> str:
    return f"{100 * part / total:.1f}%" if total else "0.0%"


def title(row: dict | None, limit: int = 82) -> str:
    if not row:
        return "-"
    text = str(row.get("title") or row.get("representative_title") or "")
    return text if len(text) <= limit else text[: limit - 1] + "..."


def plot_cluster_heatmap(selected_names: list[str], cluster_labels: list[str], matrix: list[list[int]], path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 4.8))
    im = ax.imshow(matrix, aspect="auto", cmap="Blues")
    ax.set_yticks(range(len(selected_names)))
    ax.set_yticklabels(selected_names)
    ax.set_xticks(range(len(cluster_labels)))
    ax.set_xticklabels(cluster_labels, rotation=45, ha="right")
    ax.set_title("Distribuição instituição x cluster (top clusters)", fontsize=12, fontweight="bold")
    ax.set_xlabel("community_id")
    ax.set_ylabel("Instituição")
    for i, row in enumerate(matrix):
        for j, value in enumerate(row):
            if value:
                ax.text(j, i, str(value), ha="center", va="center", fontsize=8, color="black")
    fig.colorbar(im, ax=ax, fraction=0.025, pad=0.02, label="# artigos")
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    INSTITUTION_DIR.mkdir(parents=True, exist_ok=True)
    FIGS_DIR.mkdir(parents=True, exist_ok=True)

    article_rows = read_csv(METRICS_DIR / "article_metrics.csv")
    cluster_rows = read_csv(METRICS_DIR / "cluster_metrics.csv")
    article_by_id = {row["openalex_id"]: row for row in article_rows}
    cluster_by_id = {row["community_id"]: row for row in cluster_rows}

    print("Loading GraphML affiliations and CITES edges...")
    G, _attrs, institutions, subfields, _venues, _schema = load_graph()

    articles_by_institution: dict[str, set[str]] = {iid: set() for iid in SELECTED_INSTITUTIONS}
    subfields_by_institution: dict[str, Counter[str]] = {iid: Counter() for iid in SELECTED_INSTITUTIONS}

    for article_id, inst_list in institutions.items():
        inst_ids = {inst.get("openalex_id") for inst in inst_list}
        for iid in SELECTED_INSTITUTIONS:
            if iid in inst_ids:
                articles_by_institution[iid].add(article_id)
                for subfield in subfields.get(article_id, []):
                    if subfield.get("display_name"):
                        subfields_by_institution[iid][subfield["display_name"]] += 1

    unicamp_articles = articles_by_institution[UNICAMP_ID]
    summaries = []
    cluster_counters: dict[str, Counter[str]] = {}
    top_articles_by_inst: dict[str, list[dict]] = {}

    for iid, name in SELECTED_INSTITUTIONS.items():
        article_ids = articles_by_institution[iid]
        rows = [article_by_id[aid] for aid in article_ids if aid in article_by_id]
        clusters = Counter(row["community_id"] for row in rows if row["community_id"] != "")
        cluster_counters[iid] = clusters
        top_articles_by_inst[iid] = sorted(rows, key=lambda row: to_float(row["pagerank"]), reverse=True)[:5]

        cites_unicamp = sum(1 for source, target in G.edges if source in article_ids and target in unicamp_articles)
        cited_by_unicamp = sum(1 for source, target in G.edges if source in unicamp_articles and target in article_ids)

        total_cited_by = sum(to_float(row["cited_by_count"]) for row in rows)
        total_indegree = sum(to_float(row["in_degree"]) for row in rows)
        top_cluster, top_cluster_count = clusters.most_common(1)[0] if clusters else ("", 0)

        summaries.append(
            {
                "institution_id": iid,
                "institution": name,
                "articles": len(rows),
                "total_cited_by_count": total_cited_by,
                "cited_by_per_article": total_cited_by / len(rows) if rows else 0,
                "internal_indegree": total_indegree,
                "internal_indegree_per_article": total_indegree / len(rows) if rows else 0,
                "pagerank_mean": mean([to_float(row["pagerank"]) for row in rows]),
                "pagerank_median": median([to_float(row["pagerank"]) for row in rows]),
                "top_tier_count": sum(as_bool(row["is_top_tier"]) for row in rows),
                "important_count": sum(as_bool(row["is_important_institution"]) for row in rows),
                "clusters": len(clusters),
                "top_cluster": top_cluster,
                "top_cluster_count": top_cluster_count,
                "top_cluster_fraction": top_cluster_count / len(rows) if rows else 0,
                "subfields": len(subfields_by_institution[iid]),
                "top_subfields": "; ".join(f"{sf} ({count})" for sf, count in subfields_by_institution[iid].most_common(5)),
                "cites_unicamp": cites_unicamp,
                "cited_by_unicamp": cited_by_unicamp,
            }
        )

    metrics_path = METRICS_DIR / "institution_metrics.csv"
    with metrics_path.open("w", encoding="utf-8", newline="") as f:
        fields = list(summaries[0].keys())
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(summaries)

    top_cluster_ids = [cid for cid, _ in Counter(cid for c in cluster_counters.values() for cid in c).most_common(10)]
    matrix = [[cluster_counters[iid].get(cid, 0) for cid in top_cluster_ids] for iid in SELECTED_INSTITUTIONS]
    figure_path = FIGS_DIR / f"instituicao_cluster_heatmap_{REPORT_DATE}.png"
    plot_cluster_heatmap(list(SELECTED_INSTITUTIONS.values()), top_cluster_ids, matrix, figure_path)

    summary_rows = [
        [
            row["institution"],
            row["articles"],
            f"{row['cited_by_per_article']:.1f}",
            f"{row['internal_indegree_per_article']:.2f}",
            f"{row['pagerank_median']:.3g}",
            row["clusters"],
            f"{row['top_cluster']} ({100 * row['top_cluster_fraction']:.1f}%)",
            row["top_tier_count"],
            row["cites_unicamp"],
            row["cited_by_unicamp"],
        ]
        for row in summaries
    ]

    sections = []
    for row in summaries:
        iid = row["institution_id"]
        top_cluster = cluster_by_id.get(str(row["top_cluster"]))
        sections.append(f"""
### {row['institution']}

{table(["Métrica", "Valor"], [
            ["Artigos no GraphML", row["articles"]],
            ["Citações OpenAlex por artigo", f"{row['cited_by_per_article']:.1f}"],
            ["In-degree interno por artigo", f"{row['internal_indegree_per_article']:.2f}"],
            ["Mediana PageRank", f"{row['pagerank_median']:.3g}"],
            ["Clusters com presença", row["clusters"]],
            ["Cluster dominante", f"{row['top_cluster']} ({100 * row['top_cluster_fraction']:.1f}% dos artigos)"],
            ["Representante do cluster dominante", title(top_cluster, 86)],
            ["Subfields distintos", row["subfields"]],
            ["Cita artigos Unicamp", row["cites_unicamp"]],
            ["É citado por artigos Unicamp", row["cited_by_unicamp"]],
        ])}

Subfields dominantes: {row['top_subfields'] or 'sem subfield mapeado'}.

Artigos mais centrais por PageRank:

{table(["Rank", "OpenAlex", "Ano", "Citações", "Cluster", "Top-tier", "Título"], [
            [idx + 1, article["openalex_id"], article["publication_year"], article["cited_by_count"], article["community_id"], article["is_top_tier"], title(article)]
            for idx, article in enumerate(top_articles_by_inst[iid])
        ])}
""")

    unicamp = next(row for row in summaries if row["institution_id"] == UNICAMP_ID)
    non_unicamp = [row for row in summaries if row["institution_id"] != UNICAMP_ID]
    highest_per_article = max(non_unicamp, key=lambda row: row["cited_by_per_article"])
    strongest_flow = max(non_unicamp, key=lambda row: row["cites_unicamp"] + row["cited_by_unicamp"])

    report_path = INSTITUTION_DIR / f"comparativo_instituicoes_chave_{REPORT_DATE}.md"
    report = f"""# Análise instituição: Unicamp e instituições de referência
Data: {REPORT_DATE}
Conjunto de dados: `network.graphml` local + `metrics/article_metrics.csv`

## 1. Motivação
- O checklist final pede pelo menos um estudo de caso institucional além da Unicamp.
- Esta análise compara a Unicamp com instituições internacionais e nacionais que aparecem fortemente no grafo: MIT, Stanford, Carnegie Mellon, USP e Tsinghua.
- As métricas escolhidas seguem o `AGENTS.md`: impacto agregado/per capita, PageRank médio/mediano, amplitude temática, distribuição em clusters e fluxo bilateral de citações com a Unicamp.

## 2. Metodologia
- Artigos foram selecionados por `AFFILIATED_WITH` no GraphML.
- Impacto bruto usa `cited_by_count` do OpenAlex; impacto interno usa in-degree no subgrafo `CITES`.
- Fluxo bilateral preserva a direção de citação: `instituição -> Unicamp` significa artigo da instituição citando artigo Unicamp; `Unicamp -> instituição` significa artigo Unicamp citando artigo da instituição.
- Como artigos podem ter múltiplas afiliações, os conjuntos institucionais não são disjuntos.

## 3. Resultados
Resumo comparativo:

{table(["Instituição", "Artigos", "Citações/artigo", "In-degree/artigo", "Mediana PR", "Clusters", "Cluster dominante", "Top-tier", "Cita Uni", "Citada pela Uni"], summary_rows)}

Figura:
- `figs/{figure_path.name}`: heatmap instituição x cluster para os 10 clusters mais presentes entre as instituições selecionadas.

Fatos não triviais:
- A Unicamp tem mais artigos no grafo ({unicamp['articles']}) que cada instituição de referência selecionada, mas isso não implica maior impacto per capita; {highest_per_article['institution']} tem {highest_per_article['cited_by_per_article']:.1f} citações OpenAlex por artigo.
- O fluxo bilateral mais intenso com a Unicamp, entre as instituições analisadas, é de {strongest_flow['institution']} ({strongest_flow['cites_unicamp']} citações para Unicamp, {strongest_flow['cited_by_unicamp']} citações recebidas da Unicamp no subgrafo). Isso mede proximidade estrutural direta, não apenas reputação.
- A distribuição por clusters distingue amplitude temática de concentração: uma instituição pode ter muitos artigos, mas concentrados em poucos clusters, enquanto outra ocupa menos artigos e mais comunidades.

{''.join(sections)}

## 4. Problemas encontrados
- Afiliações OpenAlex podem refletir coautorias múltiplas; portanto, a soma de artigos por instituição excede o número de artigos únicos.
- `cited_by_count` é externo ao subgrafo e mede impacto amplo; `in_degree` mede apenas citações internas no GraphML. As duas métricas não são substitutas.
- Fluxos bilaterais são baixos para algumas instituições porque o grafo foi construído a partir de caminhos e filtros, não como universo completo de todas as citações institucionais.

## 5. Importância e interpretação
- A comparação mostra que o impacto da Unicamp precisa ser lido em duas escalas: volume de presença no grafo e impacto/permeabilidade estrutural por artigo.
- Instituições com alto impacto per capita ajudam a calibrar a distância entre produção local e centros internacionais de alta centralidade.
- O fluxo bilateral com Unicamp aponta candidatos para estudos de caso de colaboração/influência indireta, especialmente quando coincide com clusters onde a Unicamp já é presente.
"""
    report_path.write_text(report, encoding="utf-8")
    update_index(report_path)
    print(f"Report written to {report_path}")


def update_index(report_path: Path) -> None:
    index_path = REPORTS_DIR / "index.md"
    line = (
        f"| {REPORT_DATE} | institution | Unicamp e referências | "
        f"[instituições](institution/{report_path.name}) | "
        "Impacto institucional, clusters e fluxo bilateral com a Unicamp. |"
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
        "- `FINAL_REPORT_CHECKLIST.md` Seção 2: ainda faltam estudos de caso de 2-3 artigos específicos e 1 instituição além da Unicamp.",
        "- `FINAL_REPORT_CHECKLIST.md` Seção 2: ainda faltam estudos de caso de 2-3 artigos específicos.",
    )
    index_path.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()

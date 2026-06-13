"""
Article case studies report.
"""

from __future__ import annotations

import csv
import os
from collections import Counter
from datetime import date
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-citation-network")

from agent_graph_analysis import ROOT, load_graph, table


REPORT_DATE = date.today().isoformat()
METRICS_DIR = ROOT / "metrics"
REPORTS_DIR = ROOT / "reports"
ARTICLE_DIR = REPORTS_DIR / "article"

CASE_STUDIES = [
    ("W2148043549", "artigo-ponte recorrente nos caminhos top-tier -> Unicamp"),
    ("W2040340473", "artigo da Unicamp com PageRank elevado"),
    ("W2163605009", "artigo top-tier com maior PageRank no grafo"),
]


def read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def title(row: dict | None, limit: int = 88) -> str:
    if not row:
        return "-"
    text = str(row.get("title") or "")
    return text if len(text) <= limit else text[: limit - 1] + "..."


def update_index(report_path: Path) -> None:
    index_path = REPORTS_DIR / "index.md"
    line = (
        f"| {REPORT_DATE} | article | estudos de caso | "
        f"[artigos](article/{report_path.name}) | "
        "Três artigos analisados em profundidade: ponte, Unicamp central e top-tier. |"
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
        "- `FINAL_REPORT_CHECKLIST.md` Seção 2: ainda faltam estudos de caso de 2-3 artigos específicos.\n",
        "",
    )
    index_path.write_text(text, encoding="utf-8")


def main() -> None:
    ARTICLE_DIR.mkdir(parents=True, exist_ok=True)
    article_rows = read_csv(METRICS_DIR / "article_metrics.csv")
    cluster_rows = read_csv(METRICS_DIR / "cluster_metrics.csv")
    bridge_rows = read_csv(METRICS_DIR / "path_bridge_metrics.csv")

    article_by_id = {row["openalex_id"]: row for row in article_rows}
    cluster_by_id = {row["community_id"]: row for row in cluster_rows}
    bridge_by_id: dict[str, list[dict]] = {}
    for row in bridge_rows:
        bridge_by_id.setdefault(row["openalex_id"], []).append(row)

    print("Loading GraphML neighborhoods...")
    G, _attrs, institutions, subfields, venues, _schema = load_graph()

    sections = []
    for openalex_id, reason in CASE_STUDIES:
        row = article_by_id[openalex_id]
        cluster = cluster_by_id.get(row["community_id"])
        outgoing = list(G.successors(openalex_id))
        incoming = list(G.predecessors(openalex_id))
        in_neighbors = [article_by_id[n] for n in incoming if n in article_by_id]
        out_neighbors = [article_by_id[n] for n in outgoing if n in article_by_id]
        bridge_info = bridge_by_id.get(openalex_id, [])
        bridge_summary = Counter(item["source_set"] for item in bridge_info)

        institution_names = sorted(
            {
                inst.get("display_name")
                for inst in institutions.get(openalex_id, [])
                if inst.get("display_name")
            }
        )
        subfield_names = sorted(
            {
                sf.get("display_name")
                for sf in subfields.get(openalex_id, [])
                if sf.get("display_name")
            }
        )
        venue_names = sorted(
            {
                v.get("display_name")
                for v in venues.get(openalex_id, [])
                if v.get("display_name")
            }
        )

        top_in = sorted(in_neighbors, key=lambda r: to_float(r["pagerank"]), reverse=True)[:5]
        top_out = sorted(out_neighbors, key=lambda r: to_float(r["pagerank"]), reverse=True)[:5]

        sections.append(f"""
## {openalex_id}

Motivação do estudo de caso: {reason}.

{table(["Métrica", "Valor"], [
            ["Título", title(row, 120)],
            ["Ano", row["publication_year"]],
            ["Citações OpenAlex", row["cited_by_count"]],
            ["In-degree", row["in_degree"]],
            ["Out-degree", row["out_degree"]],
            ["PageRank", f"{to_float(row['pagerank']):.3g}"],
            ["Authority", f"{to_float(row['authority']):.3g}"],
            ["Hub", f"{to_float(row['hub']):.3g}"],
            ["Taxa de citação por idade", f"{to_float(row['citation_rate_by_age']):.2f}"],
            ["Cluster", row["community_id"]],
            ["Cluster representante", title(cluster, 96)],
            ["Unicamp", row["is_unicamp"]],
            ["Top-tier", row["is_top_tier"]],
            ["Instituição importante", row["is_important_institution"]],
        ])}

Contexto:
- Instituições: {", ".join(institution_names) if institution_names else "sem afiliação mapeada"}.
- Subfields: {", ".join(subfield_names[:6]) if subfield_names else "sem subfield mapeado"}.
- Veículo: {", ".join(venue_names[:3]) if venue_names else "sem venue mapeado"}.
- Participação em caminhos: {dict(bridge_summary) if bridge_summary else "não apareceu entre os principais artigos-ponte cacheados"}.

Vizinhança recebida mais central (quem o cita):

{table(["OpenAlex", "Ano", "Cluster", "PageRank", "Título"], [
            [neighbor["openalex_id"], neighbor["publication_year"], neighbor["community_id"], f"{to_float(neighbor['pagerank']):.3g}", title(neighbor)]
            for neighbor in top_in
        ] if top_in else [["-", "-", "-", "-", "sem citadores no cache"]])}

Vizinhança referenciada mais central (quem ele cita):

{table(["OpenAlex", "Ano", "Cluster", "PageRank", "Título"], [
            [neighbor["openalex_id"], neighbor["publication_year"], neighbor["community_id"], f"{to_float(neighbor['pagerank']):.3g}", title(neighbor)]
            for neighbor in top_out
        ] if top_out else [["-", "-", "-", "-", "sem referências no cache"]])}

Leitura:
- Este artigo ocupa um papel diferente dos demais estudos de caso: {reason}. A utilidade do caso é mostrar que impacto extremo, centralidade institucional e recorrência em caminhos não são a mesma coisa.
- O contraste entre `in_degree`, `PageRank` e participação em caminhos ajuda a evitar leituras simplistas: um artigo pode ser muito citado sem necessariamente servir de ponte, ou pode servir de ponte sem estar no topo do impacto bruto.
""")

    report_path = ARTICLE_DIR / f"estudos_de_caso_artigos_{REPORT_DATE}.md"
    report = f"""# Análise artigo: estudos de caso
Data: {REPORT_DATE}
Conjunto de dados: `metrics/article_metrics.csv`, `metrics/path_bridge_metrics.csv`, `network.graphml`

## 1. Motivação
- O checklist final exige 2-3 estudos de caso de artigos específicos analisados em profundidade.
- Estes três casos cobrem papéis complementares: um artigo-ponte recorrente nos caminhos até a Unicamp, um artigo central da própria Unicamp e um artigo top-tier dominante em PageRank.

## 2. Metodologia
- Os artigos foram escolhidos a partir dos caches já produzidos: `path_bridge_metrics.csv` e `article_metrics.csv`.
- A análise combina métricas de artigo (`in_degree`, `PageRank`, `authority`, `hub`, taxa normalizada por idade) com contexto local (vizinhança, cluster, afiliação, subfield e participação em caminhos).
- O objetivo não é esgotar cada artigo, mas explicar por que ele importa estruturalmente.

## 3. Resultados
{''.join(sections)}

## 4. Problemas encontrados
- A vizinhança usada aqui é de 1 salto para manter a leitura compacta; uma análise posterior pode expandir para 2 saltos.
- A participação em caminhos depende do cache `path_bridge_metrics.csv`, que por sua vez depende dos caminhos podados do ETL.

## 5. Importância e interpretação
- Os três estudos de caso fecham a lacuna entre análises agregadas e exemplos concretos.
- Eles ajudam a mostrar que a rede contém artigos com funções distintas: reservatórios de impacto, transmissores de conhecimento e âncoras institucionais.
"""
    report_path.write_text(report, encoding="utf-8")
    update_index(report_path)
    print(f"Report written to {report_path}")


if __name__ == "__main__":
    main()

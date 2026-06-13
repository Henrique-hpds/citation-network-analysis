"""
Generate a cluster-specific analysis report.

The report is intentionally interpretive: each metric is tied to a structural
claim about the citation graph, following the examples in entregas/examples/.
"""

from __future__ import annotations

import csv
import math
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
CLUSTER_REPORTS_DIR = REPORTS_DIR / "cluster"
FIGS_DIR = CLUSTER_REPORTS_DIR / "figs"


def read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def to_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_bool(value: object) -> bool:
    return str(value) == "True"


def enrichment(observed: int, cluster_size: int, total_marked: int, total_nodes: int) -> float:
    expected = cluster_size * total_marked / total_nodes if total_nodes else 0
    if expected == 0:
        return 0.0
    return observed / expected


def pct(part: int, total: int) -> float:
    return 100 * part / total if total else 0.0


def title(row: dict, limit: int = 72) -> str:
    text = str(row.get("title") or row.get("representative_title") or "")
    return text if len(text) <= limit else text[: limit - 1] + "..."


def choose_clusters(cluster_rows: list[dict], article_by_cluster: dict[int, list[dict]]) -> list[tuple[int, str]]:
    large_clusters = [row for row in cluster_rows if to_int(row["size"]) >= 100]

    def cid(row: dict) -> int:
        return to_int(row["community_id"])

    choices: dict[int, list[str]] = {}

    def add(row: dict, reason: str) -> None:
        cluster_id = cid(row)
        choices.setdefault(cluster_id, [])
        if reason not in choices[cluster_id]:
            choices[cluster_id].append(reason)

    add(max(cluster_rows, key=lambda row: to_float(row["influence_score"])), "maior influência composta")
    add(max(cluster_rows, key=lambda row: to_int(row["unicamp_count"])), "maior presença absoluta da Unicamp/IC")
    add(
        max(cluster_rows, key=lambda row: sum(as_bool(a["is_top_tier"]) for a in article_by_cluster[cid(row)])),
        "maior quantidade de artigos top-tier",
    )
    add(
        max(cluster_rows, key=lambda row: sum(as_bool(a["is_important_institution"]) for a in article_by_cluster[cid(row)])),
        "maior quantidade de artigos de instituições importantes",
    )
    add(
        max(large_clusters, key=lambda row: to_float(row["unicamp_fraction"])),
        "maior fração Unicamp/IC entre clusters com ao menos 100 artigos",
    )
    add(
        max(large_clusters, key=lambda row: to_float(row["conductance_out"])),
        "maior condutância de saída entre clusters grandes",
    )

    return [(cluster_id, "; ".join(reasons)) for cluster_id, reasons in choices.items()][:6]


def flow_rows(counter: Counter, cluster_rows_by_id: dict[int, dict], limit: int = 5) -> list[list[object]]:
    rows = []
    for cid, count in counter.most_common(limit):
        cluster = cluster_rows_by_id.get(cid, {})
        rows.append([
            cid,
            count,
            cluster.get("size", ""),
            cluster.get("unicamp_count", ""),
            title(cluster, 64),
        ])
    return rows or [["-", 0, "-", "-", "sem fluxo registrado"]]


def plot_selected_profiles(selected: list[tuple[int, str]], summaries: dict[int, dict], path: Path) -> None:
    labels = [str(cid) for cid, _ in selected]
    top_tier = [summaries[cid]["top_tier_fraction"] for cid, _ in selected]
    unicamp = [summaries[cid]["unicamp_fraction_pct"] for cid, _ in selected]
    important = [summaries[cid]["important_fraction"] for cid, _ in selected]

    x = range(len(labels))
    width = 0.25
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar([i - width for i in x], top_tier, width=width, label="Top-tier", color="#2563EB")
    ax.bar(list(x), unicamp, width=width, label="Unicamp/IC", color="#EA580C")
    ax.bar([i + width for i in x], important, width=width, label="Inst. importantes", color="#16A34A")
    ax.set_title("Composição dos clusters selecionados", fontsize=12, fontweight="bold")
    ax.set_xlabel("community_id")
    ax.set_ylabel("% dos artigos do cluster")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.legend()
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    CLUSTER_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    FIGS_DIR.mkdir(parents=True, exist_ok=True)

    article_rows = read_csv(METRICS_DIR / "article_metrics.csv")
    cluster_rows = read_csv(METRICS_DIR / "cluster_metrics.csv")
    cluster_rows_by_id = {to_int(row["community_id"]): row for row in cluster_rows}

    article_by_id = {row["openalex_id"]: row for row in article_rows}
    article_by_cluster: dict[int, list[dict]] = defaultdict(list)
    for row in article_rows:
        if row["community_id"] != "":
            article_by_cluster[to_int(row["community_id"])].append(row)

    selected = choose_clusters(cluster_rows, article_by_cluster)

    total_nodes = len(article_rows)
    total_unicamp = sum(as_bool(row["is_unicamp"]) for row in article_rows)
    total_top_tier = sum(as_bool(row["is_top_tier"]) for row in article_rows)
    total_important = sum(as_bool(row["is_important_institution"]) for row in article_rows)

    print("Loading GraphML for inter-community flow...")
    G, *_ = load_graph()
    incoming: dict[int, Counter] = defaultdict(Counter)
    outgoing: dict[int, Counter] = defaultdict(Counter)
    for source, target in G.edges:
        source_row = article_by_id.get(source)
        target_row = article_by_id.get(target)
        if not source_row or not target_row:
            continue
        if source_row["community_id"] == "" or target_row["community_id"] == "":
            continue
        source_cluster = to_int(source_row["community_id"])
        target_cluster = to_int(target_row["community_id"])
        if source_cluster == target_cluster:
            continue
        outgoing[source_cluster][target_cluster] += 1
        incoming[target_cluster][source_cluster] += 1

    summaries: dict[int, dict] = {}
    selected_rows = []
    sections = []

    for cluster_id, reason in selected:
        cluster = cluster_rows_by_id[cluster_id]
        members = article_by_cluster[cluster_id]
        size = to_int(cluster["size"])
        top_tier_count = sum(as_bool(row["is_top_tier"]) for row in members)
        important_count = sum(as_bool(row["is_important_institution"]) for row in members)
        unicamp_count = sum(as_bool(row["is_unicamp"]) for row in members)
        high_impact_count = sum(as_bool(row["is_high_impact_500"]) for row in members)
        top_unicamp = sorted(
            [row for row in members if as_bool(row["is_unicamp"])],
            key=lambda row: to_float(row["pagerank"]),
            reverse=True,
        )[:5]
        top_articles = sorted(members, key=lambda row: to_float(row["pagerank"]), reverse=True)[:5]

        summaries[cluster_id] = {
            "top_tier_fraction": pct(top_tier_count, size),
            "unicamp_fraction_pct": pct(unicamp_count, size),
            "important_fraction": pct(important_count, size),
        }

        selected_rows.append([
            cluster_id,
            reason,
            size,
            unicamp_count,
            f"{pct(unicamp_count, size):.2f}%",
            top_tier_count,
            f"{enrichment(top_tier_count, size, total_top_tier, total_nodes):.2f}x",
            important_count,
            f"{enrichment(important_count, size, total_important, total_nodes):.2f}x",
            f"{to_float(cluster['influence_score']):.3f}",
            title(cluster, 58),
        ])

        sections.append(f"""
### Cluster {cluster_id}: {reason}

**Perfil estrutural.**
{table(["Métrica", "Valor"], [
            ["Tamanho", size],
            ["Rank por tamanho", cluster["rank_by_size"]],
            ["Arestas internas", cluster["internal_edges"]],
            ["Densidade interna", f"{to_float(cluster['density']):.4g}"],
            ["Condutância de saída", f"{to_float(cluster['conductance_out']):.4g}"],
            ["Influência composta", f"{to_float(cluster['influence_score']):.3f}"],
            ["Ano médio", f"{to_float(cluster['mean_year']):.1f}"],
            ["Representante", title(cluster, 90)],
        ])}

**Composição e enriquecimento.**
{table(["Conjunto", "Artigos", "% do cluster", "Enriquecimento vs grafo"], [
            ["Unicamp/IC", unicamp_count, f"{pct(unicamp_count, size):.2f}%", f"{enrichment(unicamp_count, size, total_unicamp, total_nodes):.2f}x"],
            ["Top-tier", top_tier_count, f"{pct(top_tier_count, size):.2f}%", f"{enrichment(top_tier_count, size, total_top_tier, total_nodes):.2f}x"],
            ["Instituições importantes", important_count, f"{pct(important_count, size):.2f}%", f"{enrichment(important_count, size, total_important, total_nodes):.2f}x"],
            ["Alto impacto >=500 citações", high_impact_count, f"{pct(high_impact_count, size):.2f}%", "-"],
        ])}

**Caracterização temática.**
{table(["Dimensão", "Top valores"], [
            ["Subfields", cluster["top_subfields"]],
            ["Instituições", cluster["top_institutions"]],
            ["Veículos", cluster["top_venues"]],
        ])}

**Fluxo intercomunidades.**

Principais origens que citam este cluster:

{table(["Cluster origem", "Citações", "Tamanho", "Unicamp", "Representante"], flow_rows(incoming[cluster_id], cluster_rows_by_id))}

Principais destinos citados por este cluster:

{table(["Cluster destino", "Citações", "Tamanho", "Unicamp", "Representante"], flow_rows(outgoing[cluster_id], cluster_rows_by_id))}

**Artigos representativos por PageRank.**
{table(["Rank", "OpenAlex", "Ano", "Citações", "Unicamp", "Top-tier", "Título"], [
            [idx + 1, row["openalex_id"], row["publication_year"], row["cited_by_count"], row["is_unicamp"], row["is_top_tier"], title(row, 82)]
            for idx, row in enumerate(top_articles)
        ])}

**Artigos Unicamp mais centrais no cluster.**
{table(["Rank", "OpenAlex", "Ano", "Citações", "PageRank", "Título"], [
            [idx + 1, row["openalex_id"], row["publication_year"], row["cited_by_count"], f"{to_float(row['pagerank']):.3g}", title(row, 86)]
            for idx, row in enumerate(top_unicamp)
        ] if top_unicamp else [["-", "-", "-", "-", "-", "sem artigo Unicamp neste cluster"]])}

**Leitura não trivial.**
- Este cluster tem enriquecimento top-tier de {enrichment(top_tier_count, size, total_top_tier, total_nodes):.2f}x e enriquecimento Unicamp de {enrichment(unicamp_count, size, total_unicamp, total_nodes):.2f}x. A comparação separa dois fenômenos diferentes: proximidade da cauda extrema de impacto e presença institucional.
- A condutância de saída ({to_float(cluster['conductance_out']):.3f}) indica quanto o cluster conversa com outras comunidades. Valores altos sugerem papel de circulação/difusão; valores baixos sugerem bloco mais autocontido.
- O par de fluxos de entrada e saída mostra quem alimenta o cluster e para onde ele referencia conhecimento, evitando interpretar comunidade apenas como tema isolado.
""")

    figure_path = FIGS_DIR / f"clusters_especificos_{REPORT_DATE}.png"
    plot_selected_profiles(selected, summaries, figure_path)

    report_path = CLUSTER_REPORTS_DIR / f"analise_especifica_clusters_{REPORT_DATE}.md"
    report = f"""# Análise específica de clusters selecionados
Data: {REPORT_DATE}
Conjunto de dados: `network.graphml` local + `metrics/article_metrics.csv` + `metrics/cluster_metrics.csv`

## 1. Motivação
- Esta análise aprofunda a detecção de comunidades seguindo o escopo do `AGENTS.md`: densidade, condutância, fluxo intercomunidades, representatividade temática, participação da Unicamp e influência composta.
- Os clusters foram escolhidos automaticamente para cobrir papéis estruturais diferentes, não apenas os maiores: influência global, presença Unicamp, presença top-tier, presença de instituições importantes, alta fração Unicamp e alta condutância.
- O objetivo é transformar métricas em fatos explicáveis sobre disseminação de conhecimento no grafo.

## 2. Metodologia
- Comunidades: Louvain recalculado previamente no maior WCC não direcionado e cacheado em `metrics/article_metrics.csv`.
- Fluxo intercomunidades: contagem dirigida de arestas `CITES` entre pares de `community_id`; a direção preserva `artigo origem cita artigo destino`.
- Enriquecimento: razão entre contagem observada no cluster e contagem esperada se o conjunto estivesse distribuído proporcionalmente ao tamanho do cluster.
- Interpretação: top-tier (`is_top_tier`) representa `_top_cited_cs`; instituições importantes (`is_important_institution`) representa `by_institution`, excluindo Unicamp.

## 3. Resultados
Clusters selecionados:

{table(["Cluster", "Motivo", "Tamanho", "Unicamp", "% Uni", "Top-tier", "Enr. TT", "Inst. imp.", "Enr. inst.", "Influência", "Representante"], selected_rows)}

Figura:
- `figs/{figure_path.name}`: compara a composição percentual dos clusters selecionados por Unicamp, top-tier e instituições importantes.

{''.join(sections)}

## 4. Problemas encontrados
- A análise usa os `community_id` cacheados; se o algoritmo de Louvain for reexecutado com outro seed ou outra versão de grafo, os IDs podem mudar.
- A caracterização por `Subfield` ainda usa frequência bruta; subáreas genéricas podem aparecer em muitos clusters. Um refinamento natural é TF-IDF por comunidade.
- Betweenness restrita a caminhos `A_fonte -> A_Uni` ainda não foi integrada; por isso, o papel de ponte em caminhos mínimos é inferido por fluxo/condutância, não por contagem direta em caminhos.

## 5. Importância e interpretação
- Clusters com top-tier enriquecido mostram onde o grafo concentra a cauda extrema de impacto.
- Clusters com Unicamp enriquecida mostram onde a instituição está mais inserida tematicamente.
- Quando os dois enriquecimentos aparecem juntos, há indício de proximidade entre produção Unicamp e comunidades de alto impacto; quando divergem, o cluster ajuda a separar impacto extremo de presença institucional.
- Fluxos intercomunidades indicam possíveis rotas de disseminação: clusters muito citados por vários outros funcionam como reservatórios de conhecimento; clusters com alta saída funcionam como consumidores/integradores de literatura.
"""
    report_path.write_text(report, encoding="utf-8")

    update_index(report_path)
    print(f"Report written to {report_path}")


def update_index(report_path: Path) -> None:
    index_path = REPORTS_DIR / "index.md"
    line = (
        f"| {REPORT_DATE} | cluster | clusters selecionados | "
        f"[análise específica](cluster/{report_path.name}) | "
        "Análise interpretável de clusters por influência, Unicamp, top-tier, instituições e fluxo. |"
    )
    if not index_path.exists():
        index_path.write_text(
            "# Índice de relatórios\n\n"
            "| Data | Tipo | Identificador | Relatório | Resumo |\n"
            "| --- | --- | --- | --- | --- |\n"
            f"{line}\n",
            encoding="utf-8",
        )
        return

    text = index_path.read_text(encoding="utf-8")
    if line not in text:
        if "\n## Pendências" in text:
            text = text.replace("\n## Pendências", f"\n{line}\n\n## Pendências", 1)
        else:
            text = text.rstrip() + "\n" + line + "\n"
        index_path.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()

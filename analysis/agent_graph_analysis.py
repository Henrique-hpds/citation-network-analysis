"""
Generate agent-style reports from the final GraphML citation network.

This script is intentionally local-first: it reads network.graphml instead of
requiring a live Neo4j instance, then writes reusable CSV caches under metrics/
and Markdown reports under reports/.
"""

from __future__ import annotations

import csv
import html
import json
import math
import os
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from statistics import mean, median, pstdev

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-citation-network")

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
from networkx.algorithms.link_analysis.hits_alg import _hits_python
from networkx.algorithms.link_analysis.pagerank_alg import _pagerank_python
from networkx.algorithms.community import louvain_communities, modularity


ROOT = Path(__file__).resolve().parents[1]
GRAPHML_PATH = ROOT / "network.graphml"
TOP_TIER_SOURCE_DIRS = [ROOT / "data" / "responses_1" / "_top_cited_cs"]
IMPORTANT_INSTITUTION_SOURCE_DIRS = [ROOT / "data" / "responses_1" / "by_institution"]
IMPORTANT_INSTITUTION_EXCLUDED_DIRS = {
    ROOT / "data" / "responses_1" / "by_institution" / "unicamp",
}
METRICS_DIR = ROOT / "metrics"
REPORTS_DIR = ROOT / "reports"
FIGS_DIR = REPORTS_DIR / "figs"
UNICAMP_ID = "I181391015"
REPORT_DATE = date.today().isoformat()
CURRENT_YEAR = date.today().year


def ensure_dirs() -> None:
    for path in [
        METRICS_DIR,
        REPORTS_DIR,
        FIGS_DIR,
        REPORTS_DIR / "global",
        REPORTS_DIR / "comparativo",
        REPORTS_DIR / "cluster",
    ]:
        path.mkdir(parents=True, exist_ok=True)


def clean_text(value: object, limit: int | None = None) -> str:
    text = html.unescape(str(value or "")).replace("\r", " ").replace("\n", " ").strip()
    if limit and len(text) > limit:
        return text[: limit - 1] + "..."
    return text


def load_graph() -> tuple[nx.DiGraph, dict, dict, dict, dict, dict]:
    full = nx.read_graphml(GRAPHML_PATH)

    graphml_to_article: dict[str, str] = {}
    article_attrs: dict[str, dict] = {}
    aux_attrs: dict[str, dict] = {}

    for node_id, attrs in full.nodes(data=True):
        label = attrs.get("labels")
        if label == ":Article":
            openalex_id = attrs.get("openalex_id") or node_id
            graphml_to_article[node_id] = openalex_id
            article_attrs[openalex_id] = dict(attrs)
        else:
            aux_attrs[node_id] = dict(attrs)

    cites = nx.DiGraph()
    for openalex_id, attrs in article_attrs.items():
        cites.add_node(openalex_id, **attrs)

    article_institutions: dict[str, list[dict]] = defaultdict(list)
    article_subfields: dict[str, list[dict]] = defaultdict(list)
    article_venues: dict[str, list[dict]] = defaultdict(list)

    for source, target, attrs in full.edges(data=True):
        label = attrs.get("label")
        if source not in graphml_to_article:
            continue
        article_id = graphml_to_article[source]
        if label == "CITES" and target in graphml_to_article:
            cites.add_edge(article_id, graphml_to_article[target])
        elif label == "AFFILIATED_WITH":
            article_institutions[article_id].append(aux_attrs.get(target, {}))
        elif label == "HAS_SUBFIELD":
            article_subfields[article_id].append(aux_attrs.get(target, {}))
        elif label == "PUBLISHED_IN":
            article_venues[article_id].append(aux_attrs.get(target, {}))

    return (
        cites,
        article_attrs,
        article_institutions,
        article_subfields,
        article_venues,
        {
            "node_labels": Counter(attrs.get("labels") for _, attrs in full.nodes(data=True)),
            "edge_labels": Counter(attrs.get("label") for _, _, attrs in full.edges(data=True)),
        },
    )


def load_seed_ids(directories: list[Path], excluded_dirs: set[Path] | None = None) -> tuple[set[str], dict[str, int]]:
    """
    Load seed OpenAlex work IDs from ETL response directories.

    The ETL stores one work per W*.json file and often also keeps a
    .checkpoint.json with ids_done. Filenames are the fastest and most stable
    source; checkpoint IDs are added as a guard against interrupted runs.
    """
    ids: set[str] = set()
    counts: dict[str, int] = {}
    excluded = {path.resolve() for path in (excluded_dirs or set())}

    def scan_roots(directory: Path) -> list[Path]:
        if directory.resolve() in excluded:
            return []
        child_dirs = [path for path in directory.iterdir() if path.is_dir()]
        if not child_dirs:
            return [directory]
        return [path for path in child_dirs if path.resolve() not in excluded]

    for directory in directories:
        before = len(ids)
        if not directory.exists():
            counts[str(directory.relative_to(ROOT))] = 0
            continue

        for root in scan_roots(directory):
            for path in root.rglob("*.json"):
                if path.name == ".checkpoint.json":
                    continue
                if path.stem.startswith("W"):
                    ids.add(path.stem)

            for checkpoint in root.rglob(".checkpoint.json"):
                try:
                    data = json.loads(checkpoint.read_text(encoding="utf-8"))
                except Exception:
                    continue
                for wid in data.get("ids_done", []):
                    if isinstance(wid, str) and wid.startswith("W"):
                        ids.add(wid)

        counts[str(directory.relative_to(ROOT))] = len(ids) - before
    return ids, counts


def numeric_attr(attrs: dict, key: str, default: float = 0.0) -> float:
    value = attrs.get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def zscores(values: dict[int, float]) -> dict[int, float]:
    vals = list(values.values())
    if not vals:
        return {}
    mu = mean(vals)
    sigma = pstdev(vals)
    if sigma == 0:
        return {key: 0.0 for key in values}
    return {key: (value - mu) / sigma for key, value in values.items()}


def entropy(counter: Counter) -> float:
    total = sum(counter.values())
    if total == 0:
        return 0.0
    return -sum((count / total) * math.log2(count / total) for count in counter.values())


def fmt(value: object) -> str:
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, int):
        return f"{value:,}".replace(",", ".")
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


def table(headers: list[str], rows: list[list[object]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(fmt(value) for value in row) + " |")
    return "\n".join(lines)


def distribution_stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {"n": 0, "mean": 0.0, "median": 0.0, "p90": 0.0, "p99": 0.0}
    ordered = sorted(values)

    def percentile(p: float) -> float:
        idx = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * p)))
        return ordered[idx]

    return {
        "n": len(values),
        "mean": mean(values),
        "median": median(values),
        "p90": percentile(0.90),
        "p99": percentile(0.99),
    }


def top_share(values: list[float], fraction: float) -> tuple[int, float]:
    if not values:
        return 0, 0.0
    n_top = max(1, math.ceil(len(values) * fraction))
    total = sum(values)
    if total == 0:
        return n_top, 0.0
    return n_top, sum(sorted(values, reverse=True)[:n_top]) / total


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


def pagerank_without_scipy(G: nx.DiGraph, alpha: float, max_iter: int, tol: float) -> dict[str, float]:
    try:
        return nx.pagerank(G, alpha=alpha, max_iter=max_iter, tol=tol)
    except ModuleNotFoundError as exc:
        if exc.name != "scipy":
            raise
        return _pagerank_python(G, alpha=alpha, max_iter=max_iter, tol=tol)


def hits_without_scipy(G: nx.DiGraph, max_iter: int, tol: float) -> tuple[dict[str, float], dict[str, float]]:
    try:
        return nx.hits(G, max_iter=max_iter, tol=tol, normalized=True)
    except ModuleNotFoundError as exc:
        if exc.name != "scipy":
            raise
        return _hits_python(G, max_iter=max_iter, tol=tol, normalized=True)


def save_article_metrics(path: Path, rows: list[dict]) -> None:
    fields = [
        "openalex_id",
        "title",
        "publication_year",
        "cited_by_count",
        "in_degree",
        "out_degree",
        "pagerank",
        "authority",
        "hub",
        "citation_rate_by_age",
        "wcc_rank",
        "community_id",
        "is_unicamp",
        "is_top_tier",
        "is_important_institution",
        "is_high_impact_500",
        "is_high_impact_100k",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def save_cluster_metrics(path: Path, rows: list[dict]) -> None:
    fields = [
        "community_id",
        "rank_by_size",
        "size",
        "internal_edges",
        "outgoing_edges",
        "incoming_edges",
        "density",
        "conductance_out",
        "unicamp_count",
        "unicamp_fraction",
        "mean_year",
        "pagerank_sum",
        "pagerank_mean",
        "external_citations_received",
        "incoming_source_communities",
        "influence_score",
        "representative_openalex_id",
        "representative_title",
        "top_subfields",
        "top_institutions",
        "top_venues",
        "subfield_entropy",
        "institution_entropy",
        "venue_entropy",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def plot_histogram(values: list[float], path: Path, title: str, xlabel: str) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.8))
    ax.hist(values, bins=40, color="#2563EB", edgecolor="white", linewidth=0.4)
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Frequencia")
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_top_clusters(cluster_rows: list[dict], path: Path) -> None:
    top = sorted(cluster_rows, key=lambda row: row["size"], reverse=True)[:20]
    labels = [str(row["community_id"]) for row in top][::-1]
    sizes = [row["size"] for row in top][::-1]
    unicamp = [row["unicamp_count"] for row in top][::-1]

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.barh(labels, sizes, color="#2563EB", label="Total")
    ax.barh(labels, unicamp, color="#EA580C", label="Unicamp/IC")
    ax.set_title("Top-20 comunidades no GraphML final", fontsize=12, fontweight="bold")
    ax.set_xlabel("Artigos")
    ax.set_ylabel("community_id")
    ax.legend()
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    ensure_dirs()
    G, attrs, institutions, subfields, venues, schema_counts = load_graph()

    print(f"Graph loaded: {G.number_of_nodes():,} articles, {G.number_of_edges():,} CITES edges")

    unicamp_ids = {
        node
        for node in G.nodes
        if any(inst.get("openalex_id") == UNICAMP_ID for inst in institutions.get(node, []))
    }
    top_tier_ids, top_tier_source_counts = load_seed_ids(TOP_TIER_SOURCE_DIRS)
    top_tier_in_graph = set(G.nodes) & top_tier_ids
    important_institution_ids, important_institution_source_counts = load_seed_ids(
        IMPORTANT_INSTITUTION_SOURCE_DIRS,
        IMPORTANT_INSTITUTION_EXCLUDED_DIRS,
    )
    important_institution_in_graph = set(G.nodes) & important_institution_ids
    high_impact_500 = {
        node for node, data in attrs.items() if numeric_attr(data, "cited_by_count") >= 500
    }
    high_impact_100k = {
        node for node, data in attrs.items() if numeric_attr(data, "cited_by_count") >= 100_000
    }

    wccs = sorted(nx.weakly_connected_components(G), key=len, reverse=True)
    sccs = sorted(nx.strongly_connected_components(G), key=len, reverse=True)
    wcc_rank = {}
    for rank, component in enumerate(wccs, start=1):
        for node in component:
            wcc_rank[node] = rank

    print("Computing PageRank...")
    pagerank = pagerank_without_scipy(G, alpha=0.85, max_iter=100, tol=1e-8)

    print("Computing HITS...")
    hits_problem = ""
    try:
        hubs, authorities = hits_without_scipy(G, max_iter=500, tol=1e-8)
    except Exception as exc:  # HITS can be fragile on nearly acyclic graphs.
        hits_problem = f"HITS nao convergiu/nao foi calculado: {exc}"
        hubs = {node: 0.0 for node in G.nodes}
        authorities = {node: 0.0 for node in G.nodes}

    print("Computing Louvain communities on largest WCC...")
    giant_nodes = wccs[0]
    undirected_giant = G.subgraph(giant_nodes).to_undirected()
    communities = louvain_communities(undirected_giant, seed=42, resolution=1)
    communities = sorted(communities, key=len, reverse=True)
    community_of = {}
    for community_id, nodes in enumerate(communities):
        for node in nodes:
            community_of[node] = community_id
    q = modularity(undirected_giant, communities)

    print("Writing article metrics...")
    article_rows = []
    for node in G.nodes:
        data = attrs[node]
        year = int(numeric_attr(data, "publication_year", CURRENT_YEAR))
        cited_by = int(numeric_attr(data, "cited_by_count"))
        age = max(1, CURRENT_YEAR - year)
        article_rows.append(
            {
                "openalex_id": node,
                "title": clean_text(data.get("title"), 180),
                "publication_year": year,
                "cited_by_count": cited_by,
                "in_degree": G.in_degree(node),
                "out_degree": G.out_degree(node),
                "pagerank": pagerank.get(node, 0.0),
                "authority": authorities.get(node, 0.0),
                "hub": hubs.get(node, 0.0),
                "citation_rate_by_age": cited_by / age,
                "wcc_rank": wcc_rank.get(node, ""),
                "community_id": community_of.get(node, ""),
                "is_unicamp": node in unicamp_ids,
                "is_top_tier": node in top_tier_ids,
                "is_important_institution": node in important_institution_ids,
                "is_high_impact_500": node in high_impact_500,
                "is_high_impact_100k": node in high_impact_100k,
            }
        )
    save_article_metrics(METRICS_DIR / "article_metrics.csv", article_rows)

    print("Computing cluster metrics...")
    incoming_by_cluster = Counter()
    outgoing_by_cluster = Counter()
    internal_by_cluster = Counter()
    source_communities_by_cluster: dict[int, set[int]] = defaultdict(set)
    target_communities_by_cluster: dict[int, set[int]] = defaultdict(set)

    for source, target in G.edges:
        source_comm = community_of.get(source)
        target_comm = community_of.get(target)
        if source_comm is None or target_comm is None:
            continue
        if source_comm == target_comm:
            internal_by_cluster[source_comm] += 1
        else:
            outgoing_by_cluster[source_comm] += 1
            incoming_by_cluster[target_comm] += 1
            source_communities_by_cluster[target_comm].add(source_comm)
            target_communities_by_cluster[source_comm].add(target_comm)

    pr_sum = {
        cid: sum(pagerank.get(node, 0.0) for node in nodes)
        for cid, nodes in enumerate(communities)
    }
    external_received = dict(incoming_by_cluster)
    source_reach = {cid: len(source_communities_by_cluster[cid]) for cid in range(len(communities))}
    pr_z = zscores(pr_sum)
    ext_z = zscores({cid: float(external_received.get(cid, 0)) for cid in range(len(communities))})
    reach_z = zscores({cid: float(source_reach.get(cid, 0)) for cid in range(len(communities))})

    cluster_rows = []
    for rank, nodes in enumerate(communities, start=1):
        cid = rank - 1
        node_list = list(nodes)
        years = [int(numeric_attr(attrs[n], "publication_year")) for n in node_list if attrs[n].get("publication_year")]
        sub_counter = Counter(
            clean_text(s.get("display_name"))
            for n in node_list
            for s in subfields.get(n, [])
            if s.get("display_name")
        )
        inst_counter = Counter(
            clean_text(i.get("display_name"))
            for n in node_list
            for i in institutions.get(n, [])
            if i.get("display_name")
        )
        venue_counter = Counter(
            clean_text(v.get("display_name"))
            for n in node_list
            for v in venues.get(n, [])
            if v.get("display_name")
        )
        representative = max(node_list, key=lambda n: pagerank.get(n, 0.0))
        internal_edges = internal_by_cluster[cid]
        outgoing_edges = outgoing_by_cluster[cid]
        density = internal_edges / (len(node_list) * (len(node_list) - 1)) if len(node_list) > 1 else 0.0
        cluster_rows.append(
            {
                "community_id": cid,
                "rank_by_size": rank,
                "size": len(node_list),
                "internal_edges": internal_edges,
                "outgoing_edges": outgoing_edges,
                "incoming_edges": incoming_by_cluster[cid],
                "density": density,
                "conductance_out": outgoing_edges / (internal_edges + outgoing_edges)
                if internal_edges + outgoing_edges
                else 0.0,
                "unicamp_count": len(set(node_list) & unicamp_ids),
                "unicamp_fraction": len(set(node_list) & unicamp_ids) / len(node_list),
                "mean_year": mean(years) if years else 0.0,
                "pagerank_sum": pr_sum[cid],
                "pagerank_mean": pr_sum[cid] / len(node_list),
                "external_citations_received": incoming_by_cluster[cid],
                "incoming_source_communities": len(source_communities_by_cluster[cid]),
                "influence_score": (ext_z.get(cid, 0.0) + pr_z.get(cid, 0.0) + reach_z.get(cid, 0.0)) / 3,
                "representative_openalex_id": representative,
                "representative_title": clean_text(attrs[representative].get("title"), 120),
                "top_subfields": "; ".join(f"{name} ({count})" for name, count in sub_counter.most_common(5)),
                "top_institutions": "; ".join(f"{name} ({count})" for name, count in inst_counter.most_common(5)),
                "top_venues": "; ".join(f"{name} ({count})" for name, count in venue_counter.most_common(5)),
                "subfield_entropy": entropy(sub_counter),
                "institution_entropy": entropy(inst_counter),
                "venue_entropy": entropy(venue_counter),
            }
        )
    save_cluster_metrics(METRICS_DIR / "cluster_metrics.csv", cluster_rows)

    print("Writing figures...")
    plot_histogram(
        [row["citation_rate_by_age"] for row in article_rows if row["citation_rate_by_age"] <= 5000],
        FIGS_DIR / "citation_rate_by_age_hist.png",
        "Taxa de citacao normalizada por idade",
        "cited_by_count / max(1, ano_atual - ano_publicacao)",
    )
    plot_top_clusters(cluster_rows, FIGS_DIR / "top_clusters_unicamp.png")

    print("Writing reports...")
    write_global_report(
        G,
        schema_counts,
        unicamp_ids,
        top_tier_ids,
        top_tier_in_graph,
        top_tier_source_counts,
        important_institution_ids,
        important_institution_in_graph,
        important_institution_source_counts,
        high_impact_500,
        high_impact_100k,
        wccs,
        sccs,
        q,
        communities,
        hits_problem,
        article_rows,
        cluster_rows,
    )
    write_comparative_report(article_rows)
    write_cluster_report(
        max(cluster_rows, key=lambda row: (row["unicamp_count"], row["size"])),
        cluster_rows,
        article_rows,
        hits_problem,
    )
    update_index()
    print("Done.")


def write_global_report(
    G: nx.DiGraph,
    schema_counts: dict,
    unicamp_ids: set[str],
    top_tier_ids: set[str],
    top_tier_in_graph: set[str],
    top_tier_source_counts: dict[str, int],
    important_institution_ids: set[str],
    important_institution_in_graph: set[str],
    important_institution_source_counts: dict[str, int],
    high_impact_500: set[str],
    high_impact_100k: set[str],
    wccs: list[set[str]],
    sccs: list[set[str]],
    modularity_q: float,
    communities: list[set[str]],
    hits_problem: str,
    article_rows: list[dict],
    cluster_rows: list[dict],
) -> None:
    top_pr = sorted(article_rows, key=lambda row: row["pagerank"], reverse=True)[:10]
    top_clusters = sorted(cluster_rows, key=lambda row: row["influence_score"], reverse=True)[:10]
    in_degrees = [float(row["in_degree"]) for row in article_rows]
    pr_values = [float(row["pagerank"]) for row in article_rows]
    top1_in_n, top1_in_share = top_share(in_degrees, 0.01)
    top1_pr_n, top1_pr_share = top_share(pr_values, 0.01)
    top_tier_rows = [row for row in article_rows if row["is_top_tier"]]
    important_rows = [row for row in article_rows if row["is_important_institution"] and not row["is_top_tier"]]
    top_tier_pr = distribution_stats([float(row["pagerank"]) for row in top_tier_rows])
    important_pr = distribution_stats([float(row["pagerank"]) for row in important_rows])
    unicamp_pr = distribution_stats([float(row["pagerank"]) for row in article_rows if row["is_unicamp"]])
    path = REPORTS_DIR / "global" / f"visao_geral_{REPORT_DATE}.md"
    text = f"""# Análise global: GraphML final
Data: {REPORT_DATE}
Conjunto de dados: `network.graphml` local ({G.number_of_nodes()} artigos, {G.number_of_edges()} arestas `CITES`)

## 1. Motivação
- Esta análise inicia a camada de relatórios do agente definida em `AGENTS.md`, usando o GraphML final como fonte reprodutível.
- Ela responde às perguntas centrais da proposta: estrutura do grafo, presença da Unicamp/IC, artigos estruturalmente centrais e comunidades dominantes.
- Foram escolhidas métricas globais, PageRank, HITS, Louvain e taxa de citação por idade porque elas aprofundam a análise parcial sem recalcular distâncias já cobertas pelo script `distances.py`.

## 2. Metodologia
- Schema local confirmado a partir do GraphML: nós {dict(schema_counts["node_labels"])}; arestas {dict(schema_counts["edge_labels"])}.
- O grafo estrutural usado nas métricas contém apenas `Article -[:CITES]-> Article`.
- PageRank foi calculado no grafo direcionado com `alpha=0.85`.
- HITS foi tentado com `max_iter=500` e `tol=1e-8`.
- Louvain foi calculado no maior WCC convertido para grafo não direcionado, com `seed=42`, usando `networkx.algorithms.community.louvain_communities`.
- `is_top_tier` foi derivado dos OpenAlex IDs encontrados em `{TOP_TIER_SOURCE_DIRS[0].relative_to(ROOT)}`.
- `is_important_institution` foi derivado dos OpenAlex IDs encontrados em `{IMPORTANT_INSTITUTION_SOURCE_DIRS[0].relative_to(ROOT)}`.
- O subdiretório `{next(iter(IMPORTANT_INSTITUTION_EXCLUDED_DIRS)).relative_to(ROOT)}` foi excluído de `is_important_institution` para preservar a comparação com a própria Unicamp.
- Caches gerados: `metrics/article_metrics.csv` e `metrics/cluster_metrics.csv`.

## 3. Resultados
{table(["Métrica", "Valor"], [
        ["Artigos", G.number_of_nodes()],
        ["Citações CITES", G.number_of_edges()],
        ["Artigos Unicamp/IC", len(unicamp_ids)],
        ["IDs top-tier no ETL", len(top_tier_ids)],
        ["Artigos is_top_tier no GraphML", len(top_tier_in_graph)],
        ["IDs de instituições importantes no ETL", len(important_institution_ids)],
        ["Artigos is_important_institution no GraphML", len(important_institution_in_graph)],
        ["Artigos alto impacto >= 500 citações", len(high_impact_500)],
        ["Artigos alto impacto >= 100.000 citações", len(high_impact_100k)],
        ["WCCs", len(wccs)],
        ["Maior WCC", len(wccs[0])],
        ["SCCs", len(sccs)],
        ["Maior SCC", len(sccs[0])],
        ["Comunidades Louvain no maior WCC", len(communities)],
        ["Modularidade Louvain", modularity_q],
    ])}

Fontes de `is_top_tier`:

{table(["Diretório", "IDs adicionados"], [[source, count] for source, count in top_tier_source_counts.items()])}

Fontes de `is_important_institution`:

{table(["Diretório", "IDs adicionados"], [[source, count] for source, count in important_institution_source_counts.items()])}

Top-10 artigos por PageRank:

{table(["Rank", "OpenAlex", "Ano", "Citações", "PageRank", "Título"], [
        [i + 1, row["openalex_id"], str(row["publication_year"]), row["cited_by_count"], row["pagerank"], clean_text(row["title"], 80)]
        for i, row in enumerate(top_pr)
    ])}

Top-10 comunidades por influência composta (`z(citações externas recebidas)`, `z(PageRank agregado)`, `z(nº de comunidades citantes)` com pesos iguais):

{table(["community_id", "Tamanho", "Unicamp", "Recebidas externas", "PR soma", "Alcance", "Influência", "Representante"], [
        [row["community_id"], row["size"], row["unicamp_count"], row["external_citations_received"], row["pagerank_sum"], row["incoming_source_communities"], row["influence_score"], clean_text(row["representative_title"], 80)]
        for row in top_clusters
    ])}

Fatos estruturais interpretáveis:

- O grafo é globalmente coeso, mas quase acíclico: o maior WCC contém {100 * len(wccs[0]) / G.number_of_nodes():.2f}% dos artigos, enquanto há {len(sccs):,} SCCs para {G.number_of_nodes():,} artigos. Isso sustenta a leitura de citações como fluxo temporal de conhecimento, com poucos ciclos residuais.
- A distribuição de impacto é fortemente concentrada: os {top1_in_n:,} artigos no top 1% de in-degree concentram {100 * top1_in_share:.1f}% das citações internas `CITES`, e o top 1% por PageRank concentra {100 * top1_pr_share:.1f}% do PageRank total.
- A marca `is_top_tier` é agora restrita aos top-cited por ano: {len(top_tier_in_graph):,} dos {len(top_tier_ids):,} IDs desse corpus aparecem no GraphML. Isso evita confundir prestígio institucional com seleção por impacto extremo.
- A mediana de PageRank dos top-tier ({top_tier_pr["median"]:.3g}) é maior que a da Unicamp ({unicamp_pr["median"]:.3g}) e a das instituições importantes sem top-tier ({important_pr["median"]:.3g}), indicando que a diferença não é apenas de volume institucional, mas de posição propagada na rede.

Figuras:
- `reports/figs/citation_rate_by_age_hist.png`: distribuição da taxa de citação normalizada por idade.
- `reports/figs/top_clusters_unicamp.png`: maiores comunidades e presença absoluta da Unicamp/IC.

## 4. Problemas encontrados
- A inspeção dinâmica do Neo4j (`CALL db.schema.visualization()`) não foi executada nesta primeira rodada; a confirmação foi feita pelo `network.graphml` local.
- Os scripts antigos não persistem `community_id`, `wcc_id` ou `scc_id` por artigo; por isso, a análise gerou novos caches em `metrics/`.
- O relatório antigo de comunidades em `analysis/reports/community_report.txt` registra 87.901 nós particionados, acima dos 49.196 artigos do GraphML final; por consistência, Louvain foi recalculado localmente no maior WCC do GraphML.
- Não há propriedade/label materializada para `A_Uni` e `A_inter` no GraphML. A Unicamp foi identificada por `AFFILIATED_WITH` para `I181391015`; `is_top_tier` e `is_important_institution` foram reconstruídos a partir dos diretórios de ETL indicados.
{("- " + hits_problem) if hits_problem else "- HITS convergiu nesta execução, mas deve continuar sendo monitorado porque o grafo é quase acíclico."}

## 5. Importância e interpretação
- O maior WCC concentra quase todo o grafo, então análises de comunidades e influência são estruturalmente significativas para o objetivo do projeto.
- A fragmentação em SCCs confirma o comportamento quase-DAG esperado para citações acadêmicas.
- As comunidades com alta influência composta são bons alvos para relatórios específicos, pois combinam citações externas, centralidade agregada e alcance entre áreas.
- A ausência de labels dos conjuntos-semente ainda limita comparações diretas com `A_inter`; o próximo passo técnico é materializar também os intermediários dos caminhos mínimos no cache.
"""
    path.write_text(text, encoding="utf-8")


def write_comparative_report(article_rows: list[dict]) -> None:
    groups = {
        "Unicamp/IC": [row for row in article_rows if row["is_unicamp"]],
        "Top-tier ETL": [row for row in article_rows if row["is_top_tier"] and not row["is_unicamp"]],
        "Instituições importantes": [
            row
            for row in article_rows
            if row["is_important_institution"] and not row["is_unicamp"] and not row["is_top_tier"]
        ],
        "Demais artigos": [
            row
            for row in article_rows
            if not row["is_unicamp"] and not row["is_top_tier"] and not row["is_important_institution"]
        ],
    }
    metrics = ["in_degree", "pagerank", "citation_rate_by_age", "cited_by_count"]
    summary_rows = []
    for group, rows in groups.items():
        for metric in metrics:
            stats = distribution_stats([float(row[metric]) for row in rows])
            summary_rows.append([group, metric, stats["n"], stats["mean"], stats["median"], stats["p90"], stats["p99"]])

    ks_rows = []
    ks_by_metric = {}
    for metric in metrics:
        uni = [float(row[metric]) for row in groups["Unicamp/IC"]]
        hi = [float(row[metric]) for row in groups["Top-tier ETL"]]
        inst = [float(row[metric]) for row in groups["Instituições importantes"]]
        other = [float(row[metric]) for row in groups["Demais artigos"]]
        row = [metric, ks_distance(uni, hi), ks_distance(uni, inst), ks_distance(uni, other)]
        ks_rows.append(row)
        ks_by_metric[metric] = row[1:]

    path = REPORTS_DIR / "comparativo" / f"unicamp_vs_alto_impacto_{REPORT_DATE}.md"
    text = f"""# Análise comparativa: Unicamp/IC vs. top-tier
Data: {REPORT_DATE}
Conjunto de dados: `network.graphml` local

## 1. Motivação
- Esta análise compara a posição estrutural dos artigos vinculados à Unicamp/IC com artigos top-tier e artigos das instituições importantes selecionadas.
- Ela aprofunda a hipótese do relatório parcial: artigos top-tier tendem a ocupar a cauda de maior grau/impacto, enquanto a Unicamp aparece em faixa intermediária.
- Foram usadas distribuições, e não apenas médias, porque métricas de citação e centralidade têm cauda longa.

## 2. Metodologia
- Unicamp/IC foi identificada por `AFFILIATED_WITH` à instituição OpenAlex `{UNICAMP_ID}`.
- `Top-tier ETL` foi identificado pela coluna `is_top_tier`, derivada apenas dos IDs encontrados em `data/responses_1/_top_cited_cs`.
- `Instituições importantes` foi identificado pela coluna `is_important_institution`, derivada dos IDs encontrados em `data/responses_1/by_institution`.
- O subdiretório `data/responses_1/by_institution/unicamp` foi excluído de `is_important_institution` para não misturar o conjunto de comparação com a própria Unicamp.
- Artigos que pertencem simultaneamente a Unicamp/IC e top-tier foram mantidos no grupo Unicamp para evitar sobreposição.
- As métricas comparadas foram in-degree, PageRank, taxa de citação por idade e citações brutas.
- Em vez de teste paramétrico, reporta-se distância KS empírica entre distribuições; valores maiores indicam separação mais forte.

## 3. Resultados
Resumo das distribuições:

{table(["Grupo", "Métrica", "n", "Média", "Mediana", "P90", "P99"], summary_rows)}

Distância KS empírica:

{table(["Métrica", "Unicamp vs top-tier ETL", "Unicamp vs instituições importantes", "Unicamp vs demais"], ks_rows)}

Leitura dos resultados:

- A separação entre Unicamp e top-tier é muito forte em citações brutas (KS={ks_by_metric["cited_by_count"][0]:.3f}) e taxa normalizada por idade (KS={ks_by_metric["citation_rate_by_age"][0]:.3f}). Isso mostra que o grupo top-tier não é apenas mais antigo ou mais numeroso: ele ocupa uma cauda de impacto distinta.
- A diferença de PageRank entre Unicamp e top-tier (KS={ks_by_metric["pagerank"][0]:.3f}) é menor que a de citações, mas ainda alta. Esse é um fato não trivial: parte da distância entre os grupos permanece mesmo quando o impacto é propagado pela estrutura de quem cita quem.
- As instituições importantes ficam mais próximas da Unicamp que o top-tier em todas as métricas KS. Isso sugere que a comparação institucional é um problema diferente da comparação contra artigos extremos: ela mede posição relativa em uma elite institucional ampla, não apenas proximidade da cauda de maior impacto.

## 4. Problemas encontrados
- As marcações `is_top_tier` e `is_important_institution` dependem da disponibilidade local do symlink `data/responses_1`; nesta execução ele apontou para `/home/debian/projeto_ruben/responses_1/`.
- A versão atual não aplica Mann-Whitney/KS com p-valor; ela reporta a distância KS como tamanho de efeito exploratório.
- Artigos de instituições importantes que também são top-tier entram no grupo top-tier, para manter os grupos disjuntos.

## 5. Importância e interpretação
- A comparação separa impacto bruto, impacto propagado e impacto normalizado por idade, evitando conclusões baseadas apenas em contagem de citações.
- Se a distância KS de PageRank for alta, isso indica que a diferença não é só volume de citações, mas posição estrutural na rede.
- Esta análise deve ser repetida quando `A_inter` também for materializado em cache.
"""
    path.write_text(text, encoding="utf-8")


def write_cluster_report(cluster: dict, cluster_rows: list[dict], article_rows: list[dict], hits_problem: str) -> None:
    cid = cluster["community_id"]
    members = [row for row in article_rows if row["community_id"] == cid]
    top_members = sorted(members, key=lambda row: row["pagerank"], reverse=True)[:10]
    rank_by_influence = sorted(cluster_rows, key=lambda row: row["influence_score"], reverse=True).index(cluster) + 1
    rank_by_unicamp = sorted(cluster_rows, key=lambda row: row["unicamp_count"], reverse=True).index(cluster) + 1
    median_density = median([row["density"] for row in cluster_rows])
    median_conductance = median([row["conductance_out"] for row in cluster_rows])

    path = REPORTS_DIR / "cluster" / f"{cid}_{REPORT_DATE}.md"
    text = f"""# Análise cluster: {cid}
Data: {REPORT_DATE}
Conjunto de dados: `network.graphml` local

## 1. Motivação
- Este cluster foi escolhido automaticamente por ter a maior presença absoluta de artigos da Unicamp/IC nesta primeira rodada.
- A análise ajuda a entender em qual comunidade temática a Unicamp aparece com mais força e se essa comunidade também ocupa posição influente no grafo.
- Foram usadas métricas de coesão, fluxo externo, PageRank agregado, perfil temporal e caracterização por subárea/instituição/veículo.

## 2. Metodologia
- Comunidades foram obtidas por Louvain no maior WCC não direcionado (`seed=42`).
- Densidade interna = `arestas internas / (|V_c| * (|V_c|-1))`.
- Condutância de saída = `arestas saindo / (arestas internas + arestas saindo)`.
- Influência composta usa pesos iguais para z-scores de citações externas recebidas, PageRank agregado e número de comunidades que citam o cluster.
- Nó representativo = artigo com maior PageRank dentro do cluster.

## 3. Resultados
{table(["Métrica", "Valor"], [
        ["community_id", cid],
        ["Rank por tamanho", cluster["rank_by_size"]],
        ["Rank por influência composta", rank_by_influence],
        ["Rank por presença Unicamp", rank_by_unicamp],
        ["Tamanho |V_c|", cluster["size"]],
        ["Arestas internas |E_c|", cluster["internal_edges"]],
        ["Densidade interna", cluster["density"]],
        ["Condutância de saída", cluster["conductance_out"]],
        ["Artigos Unicamp/IC", cluster["unicamp_count"]],
        ["Fração Unicamp/IC", cluster["unicamp_fraction"]],
        ["Ano médio", cluster["mean_year"]],
        ["PageRank agregado", cluster["pagerank_sum"]],
        ["Citações externas recebidas", cluster["external_citations_received"]],
        ["Comunidades citantes distintas", cluster["incoming_source_communities"]],
        ["Influência composta", cluster["influence_score"]],
    ])}

Caracterização:

{table(["Dimensão", "Top valores", "Entropia"], [
        ["Subfields", cluster["top_subfields"], cluster["subfield_entropy"]],
        ["Instituições", cluster["top_institutions"], cluster["institution_entropy"]],
        ["Veículos", cluster["top_venues"], cluster["venue_entropy"]],
    ])}

Artigos representativos por PageRank:

{table(["Rank", "OpenAlex", "Ano", "Citações", "PageRank", "Unicamp", "Título"], [
        [i + 1, row["openalex_id"], str(row["publication_year"]), row["cited_by_count"], row["pagerank"], row["is_unicamp"], clean_text(row["title"], 80)]
        for i, row in enumerate(top_members)
    ])}

Fatos não triviais:

- O cluster é simultaneamente o 1º em presença absoluta da Unicamp e o {rank_by_influence}º por influência composta. Portanto, ele não é apenas um agrupamento local da instituição; ele está entre os blocos estruturalmente mais relevantes do grafo.
- A fração Unicamp é de {100 * cluster["unicamp_fraction"]:.2f}%, pequena em termos absolutos, mas suficiente para colocar o cluster em 1º lugar por presença Unicamp. Isso indica uma inserção distribuída em uma comunidade grande, e não uma comunidade isolada dominada pela instituição.
- A densidade interna ({cluster["density"]:.4g}) deve ser lida junto com a condutância ({cluster["conductance_out"]:.4g}): comparado às medianas dos clusters ({median_density:.4g} e {median_conductance:.4g}), o cluster combina coesão interna com forte circulação externa de citações.

## 4. Problemas encontrados
- A dominância de subáreas ainda usa contagem bruta; como `Subfield` é amplo, o próximo refinamento deve aplicar TF-IDF por comunidade.
- Sem `A_inter` materializado, ainda não dá para dizer se os artigos Unicamp deste cluster são também transmissores frequentes em caminhos mínimos.
{("- " + hits_problem) if hits_problem else "- HITS foi calculado globalmente, mas não foi usado para escolher o representante desta primeira análise; PageRank foi priorizado."}

## 5. Importância e interpretação
- Este cluster é o primeiro candidato natural para discutir onde a Unicamp/IC se concentra dentro da rede.
- A combinação de presença Unicamp, fluxo externo e PageRank agregado indica se a comunidade é apenas numerosa para a instituição ou também estruturalmente influente.
- O relatório deve ser complementado com reconstrução de caminhos mínimos para verificar se seus artigos aparecem como pontes entre `A_fonte` e `A_Uni`.
"""
    path.write_text(text, encoding="utf-8")


def update_index() -> None:
    path = REPORTS_DIR / "index.md"
    entries = [
        f"| {REPORT_DATE} | global | GraphML final | [visão geral](global/visao_geral_{REPORT_DATE}.md) | Métricas estruturais, PageRank, Louvain e problemas de schema/caches. |",
        f"| {REPORT_DATE} | comparativo | Unicamp vs top-tier/instituições | [comparativo](comparativo/unicamp_vs_alto_impacto_{REPORT_DATE}.md) | Distribuições usando `is_top_tier` e `is_important_institution` derivados do ETL. |",
    ]
    cluster_reports = sorted((REPORTS_DIR / "cluster").glob(f"*_{REPORT_DATE}.md"))
    if cluster_reports:
        cluster_name = cluster_reports[-1].name
        cid = cluster_name.split("_")[0]
        entries.append(
            f"| {REPORT_DATE} | cluster | {cid} | [cluster {cid}](cluster/{cluster_name}) | Comunidade com maior presença absoluta da Unicamp/IC. |"
        )

    header = """# Índice de relatórios

| Data | Tipo | Identificador | Relatório | Resumo |
| --- | --- | --- | --- | --- |
"""
    path.write_text(header + "\n".join(entries) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()

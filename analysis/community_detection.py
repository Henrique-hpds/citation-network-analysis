"""
Detecção de comunidades no grafo de citações usando o algoritmo de Louvain.

Etapas:
  1. Carrega o grafo CITES do Neo4j (nós, arestas, subfields)
  2. Converte para grafo não-dirigido e restringe ao maior WCC
  3. Executa Louvain e obtém a partição das comunidades
  4. Para cada comunidade reporta:
       - Tamanho, nós da Unicamp, subfields mais comuns,
         artigos mais citados dentro da comunidade
  5. Gera figuras de distribuição de tamanho das comunidades

Saída:
  analysis/reports/community_report.txt
  analysis/figures/community_size_distribution_*.png
"""

import os
from collections import Counter, defaultdict

import community as community_louvain
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import networkx as nx
import numpy as np
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

UNICAMP_ID = "I181391015"

FIGURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "figures")
REPORTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
os.makedirs(FIGURES_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

PALETTE = {
    "blue":   "#2563EB",
    "orange": "#EA580C",
    "gray":   "#6B7280",
    "red":    "#DC2626",
    "green":  "#16A34A",
}

# ---------------------------------------------------------------------------
# Neo4j helpers
# ---------------------------------------------------------------------------

driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")),
)


def _run(cypher: str, params: dict = None):
    with driver.session() as s:
        return [r.data() for r in s.run(cypher, params or {})]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_graph():
    print("Carregando arestas CITES...")
    edges = _run("""
        MATCH (a:Article)-[:CITES]->(b:Article)
        RETURN a.openalex_id AS src, b.openalex_id AS tgt
    """)

    print("Carregando metadados dos artigos...")
    articles = _run("""
        MATCH (a:Article)
        RETURN a.openalex_id AS id,
               a.cited_by_count AS cited_by_count,
               a.title AS title,
               a.publication_year AS year
    """)

    print("Carregando IDs de artigos da Unicamp...")
    unicamp_rows = _run("""
        MATCH (a:Article)-[:AFFILIATED_WITH]->(i:Institution {openalex_id: $uid})
        RETURN a.openalex_id AS id
    """, {"uid": UNICAMP_ID})
    unicamp_ids = {r["id"] for r in unicamp_rows}

    print("Carregando subfields por artigo...")
    subfield_rows = _run("""
        MATCH (a:Article)-[:HAS_SUBFIELD]->(s:Subfield)
        RETURN a.openalex_id AS id, s.display_name AS subfield
    """)
    article_subfields: dict[str, list[str]] = defaultdict(list)
    for r in subfield_rows:
        article_subfields[r["id"]].append(r["subfield"])

    meta: dict[str, dict] = {}
    for r in articles:
        meta[r["id"]] = {
            "cited_by_count": r["cited_by_count"] or 0,
            "title":          r["title"] or "",
            "year":           r["year"],
            "subfields":      article_subfields.get(r["id"], []),
            "is_unicamp":     r["id"] in unicamp_ids,
        }

    G = nx.DiGraph()
    G.add_nodes_from(meta.keys())
    G.add_edges_from((r["src"], r["tgt"]) for r in edges)

    print(f"  {G.number_of_nodes():,} nós  |  {G.number_of_edges():,} arestas  "
          f"|  {len(unicamp_ids):,} artigos da Unicamp")

    driver.close()
    return G, meta, unicamp_ids


# ---------------------------------------------------------------------------
# Louvain
# ---------------------------------------------------------------------------

def run_louvain(G: nx.DiGraph):
    print("\nConvertendo para grafo não-dirigido (maior WCC)...")
    U = G.to_undirected()

    largest_wcc = max(nx.connected_components(U), key=len)
    U_giant = U.subgraph(largest_wcc).copy()
    print(f"  Maior WCC: {U_giant.number_of_nodes():,} nós, "
          f"{U_giant.number_of_edges():,} arestas")

    print("Executando algoritmo de Louvain...")
    partition = community_louvain.best_partition(U_giant, random_state=42)
    modularity = community_louvain.modularity(partition, U_giant)
    print(f"  Número de comunidades: {len(set(partition.values())):,}")
    print(f"  Modularidade Q: {modularity:.4f}")

    return partition, modularity, U_giant


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def build_report(partition: dict, meta: dict, modularity: float) -> str:
    communities: dict[int, list[str]] = defaultdict(list)
    for node, comm_id in partition.items():
        communities[comm_id].append(node)

    sorted_comms = sorted(communities.items(), key=lambda x: len(x[1]), reverse=True)
    n_comms = len(sorted_comms)

    lines = []
    sep = "=" * 70

    lines.append(sep)
    lines.append("DETECÇÃO DE COMUNIDADES — ALGORITMO DE LOUVAIN")
    lines.append(sep)
    lines.append(f"  Total de comunidades : {n_comms:,}")
    lines.append(f"  Modularidade Q       : {modularity:.4f}")
    lines.append(f"  Nós particionados    : {len(partition):,}")
    lines.append("")

    top_n = min(20, n_comms)
    lines.append(f"Top-{top_n} maiores comunidades")
    lines.append("-" * 70)
    header = f"  {'Rank':>4}  {'Tamanho':>8}  {'Unicamp':>7}  {'Subfield predominante':<30}"
    lines.append(header)
    lines.append(f"  {'-'*4}  {'-'*8}  {'-'*7}  {'-'*30}")

    for rank, (comm_id, nodes) in enumerate(sorted_comms[:top_n], 1):
        n_unicamp = sum(1 for n in nodes if meta.get(n, {}).get("is_unicamp"))
        all_subs = [s for n in nodes for s in meta.get(n, {}).get("subfields", [])]
        top_sub = Counter(all_subs).most_common(1)[0][0] if all_subs else "—"
        lines.append(f"  {rank:>4}  {len(nodes):>8,}  {n_unicamp:>7,}  {top_sub:<30}")

    lines.append("")
    lines.append(sep)
    lines.append("DETALHES DAS COMUNIDADES (top-10)")
    lines.append(sep)

    for rank, (comm_id, nodes) in enumerate(sorted_comms[:10], 1):
        node_set = set(nodes)
        n_unicamp = sum(1 for n in nodes if meta.get(n, {}).get("is_unicamp"))

        # Subfields
        all_subs = [s for n in nodes for s in meta.get(n, {}).get("subfields", [])]
        top_subs = Counter(all_subs).most_common(5)

        # Top artigos por citações
        top_articles = sorted(
            [(n, meta.get(n, {}).get("cited_by_count", 0),
              meta.get(n, {}).get("title", "")[:60],
              meta.get(n, {}).get("year", ""))
             for n in nodes],
            key=lambda x: x[1], reverse=True
        )[:5]

        lines.append("")
        lines.append(f"Comunidade #{rank}  (id interno: {comm_id})")
        lines.append(f"  Tamanho     : {len(nodes):,} artigos")
        lines.append(f"  Unicamp     : {n_unicamp:,} artigos")
        lines.append(f"  Subfields mais comuns:")
        for sub, cnt in top_subs:
            pct = 100 * cnt / len(all_subs) if all_subs else 0
            lines.append(f"    {cnt:>6,} ({pct:5.1f}%)  {sub}")
        lines.append(f"  Artigos mais citados (dentro da comunidade):")
        for nid, cit, title, year in top_articles:
            lines.append(f"    [{year}] {cit:>7,} citações — {title}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------

def _style_ax(ax, title, xlabel, ylabel, logy=False):
    ax.set_title(title, fontsize=13, fontweight="bold", pad=10)
    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel(ylabel, fontsize=11)
    ax.spines[["top", "right"]].set_visible(False)
    if logy:
        ax.yaxis.set_major_formatter(ticker.ScalarFormatter())
    else:
        ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))


def _savefig(fig, path):
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Salvo: {path}")


def plot_community_sizes(partition: dict):
    communities: dict[int, int] = Counter(partition.values())
    sizes = sorted(communities.values(), reverse=True)

    size_count = Counter(sizes)
    xs = np.array(sorted(size_count.keys()), dtype=float)
    ys = np.array([size_count[int(x)] for x in xs], dtype=float)

    giant = sizes[0]
    is_giant_outlier = len(sizes) > 1 and giant > sizes[1] * 5

    xlabel = "Tamanho da Comunidade (Número de Vértices)"
    ylabel = "Frequência (Número de Comunidades)"
    title_base = "Distribuição do Tamanho das Comunidades (Louvain)"

    # Figura 1 — linear (excluindo o gigante se outlier)
    mask_small = xs < giant if is_giant_outlier else np.ones_like(xs, dtype=bool)
    xs_s, ys_s = xs[mask_small], ys[mask_small]
    min_gap = float(np.diff(xs_s).min()) if len(xs_s) > 1 else 1.0
    bar_w = min_gap * 0.8

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(xs_s, ys_s, width=bar_w, color=PALETTE["orange"],
           edgecolor="white", linewidth=0.4)
    if len(xs_s) <= 30:
        ax.set_xticks(xs_s)
        ax.set_xticklabels([str(int(x)) for x in xs_s], rotation=45, ha="right", fontsize=7)
    title_lin = (f"{title_base}\n(excluindo comunidade gigante k={giant:,})"
                 if is_giant_outlier else title_base)
    _style_ax(ax, title_lin, xlabel, ylabel)
    _savefig(fig, os.path.join(FIGURES_DIR, "community_size_distribution_linear.png"))

    # Figura 2 — log-y
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(xs_s, ys_s, width=bar_w, color=PALETTE["orange"],
           edgecolor="white", linewidth=0.4)
    if len(xs_s) <= 30:
        ax.set_xticks(xs_s)
        ax.set_xticklabels([str(int(x)) for x in xs_s], rotation=45, ha="right", fontsize=7)
    ax.set_yscale("log")
    title_logy = (f"{title_base} (Escala Log)\n(excluindo comunidade gigante k={giant:,})"
                  if is_giant_outlier else f"{title_base} (Escala Log)")
    _style_ax(ax, title_logy, xlabel, f"{ylabel} — Escala Log", logy=True)
    _savefig(fig, os.path.join(FIGURES_DIR, "community_size_distribution_logy.png"))

    # Figura 3 — log-log
    mask_pos = ys > 0
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.scatter(xs[mask_pos], ys[mask_pos], color=PALETTE["orange"], s=30, alpha=0.85, zorder=3)
    if is_giant_outlier:
        ax.scatter([giant], [size_count[giant]], color=PALETTE["red"], s=70,
                   zorder=4, label=f"Gigante (k={giant:,})")
        ax.legend(fontsize=9)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(ticker.ScalarFormatter())
    _style_ax(ax, f"{title_base} (Log-Log)", xlabel, ylabel, logy=True)
    _savefig(fig, os.path.join(FIGURES_DIR, "community_size_distribution_loglog.png"))

    # Figura 4 — top-20 maiores comunidades (barras horizontais)
    top20_sizes = sizes[:20]
    top20_labels = [f"#{i+1}" for i in range(len(top20_sizes))]
    fig, ax = plt.subplots(figsize=(8, 6))
    bars = ax.barh(top20_labels[::-1], top20_sizes[::-1],
                   color=PALETTE["blue"], edgecolor="white", linewidth=0.4)
    ax.bar_label(bars, labels=[f"{s:,}" for s in top20_sizes[::-1]],
                 padding=4, fontsize=8)
    _style_ax(ax, "Top-20 Maiores Comunidades (Louvain)",
              "Número de Vértices", "Comunidade")
    _savefig(fig, os.path.join(FIGURES_DIR, "community_top20.png"))


def plot_unicamp_per_community(partition: dict, meta: dict):
    communities: dict[int, list[str]] = defaultdict(list)
    for node, comm_id in partition.items():
        communities[comm_id].append(node)

    sorted_comms = sorted(communities.items(), key=lambda x: len(x[1]), reverse=True)
    top20 = sorted_comms[:20]

    labels = [f"#{i+1}" for i in range(len(top20))]
    sizes = [len(nodes) for _, nodes in top20]
    unicamp_counts = [sum(1 for n in nodes if meta.get(n, {}).get("is_unicamp"))
                      for _, nodes in top20]
    other_counts = [s - u for s, u in zip(sizes, unicamp_counts)]

    fig, ax = plt.subplots(figsize=(9, 6))
    y = np.arange(len(labels))
    ax.barh(y, other_counts, color=PALETTE["blue"], label="Outros", edgecolor="white")
    ax.barh(y, unicamp_counts, left=other_counts, color=PALETTE["orange"],
            label="Unicamp/IC", edgecolor="white")
    ax.set_yticks(y)
    ax.set_yticklabels(labels[::-1] if False else labels)
    ax.legend(fontsize=9)
    _style_ax(ax, "Composição das Top-20 Comunidades (Unicamp vs. Outros)",
              "Número de Artigos", "Comunidade (rank por tamanho)")
    _savefig(fig, os.path.join(FIGURES_DIR, "community_unicamp_composition.png"))


def plot_community_graph(partition: dict, G: nx.DiGraph, meta: dict, top_n: int = 30):
    """
    Grafo de comunidades: cada nó representa uma comunidade, cada aresta
    representa citações entre artigos de comunidades distintas.

    Codificação visual:
      - Tamanho do nó  ∝ número de artigos na comunidade
      - Cor do nó      ∝ fração de artigos do IC (branco→laranja)
      - Espessura da aresta ∝ log(nº de citações inter-comunidade)
    """
    communities: dict[int, list[str]] = defaultdict(list)
    for node, comm_id in partition.items():
        communities[comm_id].append(node)

    sorted_comms = sorted(communities.items(), key=lambda x: len(x[1]), reverse=True)
    top_n = min(top_n, len(sorted_comms))
    top_ids = {cid for cid, _ in sorted_comms[:top_n]}
    rank_of  = {cid: r + 1 for r, (cid, _) in enumerate(sorted_comms[:top_n])}

    # Conta citações entre pares de comunidades (dígrafo → soma ambos os sentidos)
    inter: dict[tuple[int, int], int] = defaultdict(int)
    for u, v in G.edges():
        cu, cv = partition.get(u), partition.get(v)
        if cu is None or cv is None or cu == cv:
            continue
        if cu in top_ids and cv in top_ids:
            inter[(min(cu, cv), max(cu, cv))] += 1

    # Grafo de comunidades (não-dirigido, peso = citações inter)
    CG = nx.Graph()
    for cid in top_ids:
        nodes = communities[cid]
        n_uni = sum(1 for n in nodes if meta.get(n, {}).get("is_unicamp"))
        CG.add_node(
            cid,
            size=len(nodes),
            unicamp_frac=n_uni / len(nodes) if nodes else 0.0,
            rank=rank_of[cid],
        )

    # Inclui apenas arestas acima da mediana de peso (reduz ruído visual)
    all_weights = sorted(inter.values())
    threshold = all_weights[len(all_weights) // 2] if all_weights else 1
    for (cu, cv), w in inter.items():
        if w >= threshold:
            CG.add_edge(cu, cv, weight=w)

    # Nós sem nenhuma aresta no grafo de comunidades distorcem o layout —
    # ficam longe do cluster principal e comprimem todos os outros.
    CG.remove_nodes_from(list(nx.isolates(CG)))

    if len(CG) == 0:
        print("  Nenhuma aresta inter-comunidade suficiente para plotar.")
        return

    n = len(CG)
    # k ∝ 1/sqrt(n) espaça os nós proporcionalmente ao tamanho do grafo
    k = 4.0 / np.sqrt(n)
    pos = nx.spring_layout(CG, seed=42, weight="weight", k=k, iterations=500)

    # Rescale para coordenadas de dados em [-5, 5] em cada eixo,
    # clipando ao percentil 5–95 para não deixar outliers distorcerem a escala.
    FIG_W, FIG_H = 18, 14
    MARGIN = 1.0

    def _rescale_axis(coords: np.ndarray, lo: float, hi: float) -> np.ndarray:
        span = hi - lo if hi != lo else 1.0
        return (coords - lo) / span * 10.0 - 5.0

    all_xs = np.array([pos[n][0] for n in CG.nodes()])
    all_ys = np.array([pos[n][1] for n in CG.nodes()])
    p5x, p95x = np.percentile(all_xs, 5), np.percentile(all_xs, 95)
    p5y, p95y = np.percentile(all_ys, 5), np.percentile(all_ys, 95)
    sx = _rescale_axis(np.clip(all_xs, p5x, p95x), p5x, p95x)
    sy = _rescale_axis(np.clip(all_ys, p5y, p95y), p5y, p95y)
    pos = {node: (float(sx[i]), float(sy[i])) for i, node in enumerate(CG.nodes())}

    node_list = list(CG.nodes())
    sizes_raw     = np.array([CG.nodes[n]["size"]         for n in node_list], dtype=float)
    unicamp_fracs = np.array([CG.nodes[n]["unicamp_frac"] for n in node_list], dtype=float)
    node_sizes    = 120 + 600 * np.sqrt(sizes_raw / sizes_raw.max())

    edge_list = list(CG.edges(data=True))
    ew = np.array([d["weight"] for _, _, d in edge_list], dtype=float) if edge_list else np.array([])
    edge_widths = (0.4 + 4.0 * (np.log1p(ew) / np.log1p(ew.max()))) if ew.size else []
    edge_alphas = (0.2 + 0.5 * (ew / ew.max()))                       if ew.size else []

    labels = {n: f"{CG.nodes[n]['size']:,}" for n in node_list}

    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))
    ax.set_xlim(-5 - MARGIN, 5 + MARGIN)
    ax.set_ylim(-5 - MARGIN, 5 + MARGIN)

    for (u, v, _), lw, alpha in zip(edge_list, edge_widths, edge_alphas):
        nx.draw_networkx_edges(
            CG, pos, edgelist=[(u, v)],
            width=float(lw), alpha=float(alpha),
            edge_color=PALETTE["gray"], ax=ax,
        )

    sc = nx.draw_networkx_nodes(
        CG, pos,
        nodelist=node_list,
        node_size=node_sizes,
        node_color=unicamp_fracs,
        cmap=plt.cm.YlOrRd,
        vmin=0.0,
        vmax=max(unicamp_fracs.max(), 0.01),
        ax=ax,
    )
    nx.draw_networkx_labels(CG, pos, labels=labels, font_size=7, ax=ax)

    cbar = plt.colorbar(sc, ax=ax, fraction=0.02, pad=0.01)
    cbar.set_label("Fração de artigos do IC/UNICAMP", fontsize=10)

    ax.set_title(
        f"Grafo de Comunidades — Top-{top_n} (Louvain)\n"
        "Tamanho ∝ artigos na comunidade  ·  "
        "Cor ∝ fração IC  ·  "
        "Espessura ∝ log(citações inter-comunidade)",
        fontsize=12, fontweight="bold", pad=12,
    )
    ax.axis("off")
    _savefig(fig, os.path.join(FIGURES_DIR, "community_graph.png"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    G, meta, unicamp_ids = load_graph()
    partition, modularity, U_giant = run_louvain(G)

    print("\nGerando relatório...")
    report_text = build_report(partition, meta, modularity)

    report_path = os.path.join(REPORTS_DIR, "community_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"  Salvo: {report_path}")
    print(report_text)

    print("\nGerando figuras...")
    plot_community_sizes(partition)
    plot_unicamp_per_community(partition, meta)
    plot_community_graph(partition, G, meta)

    print("\nConcluído. Figuras em:", FIGURES_DIR)


if __name__ == "__main__":
    main()

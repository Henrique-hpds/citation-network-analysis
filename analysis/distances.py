"""
Análise de distâncias de difusão entre artigos de alto impacto e artigos do IC/UNICAMP.

Modelo de difusão de conhecimento:
  No dígrafo de citações, a aresta A → B significa "A cita B".
  O conhecimento flui no sentido inverso: de B (citado) para A (que cita).
  Um caminho de difusão de H até P tem a forma:

      H  ←cita—  X₁  ←cita—  X₂  ←cita—  P

  No grafo original, isso equivale ao caminho dirigido P → X₂ → X₁ → H.
  Portanto, a distância de difusão de H até P = comprimento do caminho mais
  curto de P a H no dígrafo original = BFS no grafo TRANSPOSTO a partir de H.

Estratégia:
  - Grafo transposto do dígrafo CITES, restrito ao maior WCC não-dirigido.
  - BFS multi-source a partir de todos os artigos de alto impacto no transposto
    → distância de difusão de cada nó ao artigo de alto impacto mais próximo.
  - Reporta a distribuição dessas distâncias para os artigos do IC.

Artigos de "alto impacto" são definidos pelo flag --min-citations
(padrão: 500 citações). Inclui apenas artigos não afiliados à Unicamp
para evitar distância trivialmente zero.

Uso:
    python analysis/distances.py
    python analysis/distances.py --min-citations 1000

Saída:
  analysis/reports/distances_report.txt
  analysis/figures/distances_ic_to_highimpact.png
  analysis/figures/distances_ic_cdf.png
  analysis/figures/distances_all_vs_ic_logy.png
"""

import argparse
import os
from collections import Counter, deque, defaultdict

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
# Neo4j
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

def load_data(min_citations: int):
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

    driver.close()

    meta: dict[str, dict] = {
        r["id"]: {
            "cited_by_count": r["cited_by_count"] or 0,
            "title":          r["title"] or "",
            "year":           r["year"],
        }
        for r in articles
    }

    # High-impact: high citation count AND not Unicamp (to avoid trivial distance=0)
    high_impact_ids = {
        nid for nid, m in meta.items()
        if m["cited_by_count"] >= min_citations and nid not in unicamp_ids
    }

    print(f"  Artigos totais      : {len(meta):,}")
    print(f"  Artigos da Unicamp  : {len(unicamp_ids):,}")
    print(f"  Alto impacto (≥{min_citations:,} citações): {len(high_impact_ids):,}")

    G = nx.DiGraph()
    G.add_nodes_from(meta.keys())
    G.add_edges_from((r["src"], r["tgt"]) for r in edges)

    return G, meta, unicamp_ids, high_impact_ids


# ---------------------------------------------------------------------------
# Multi-source BFS no grafo transposto
# ---------------------------------------------------------------------------

def multisource_bfs_transposed(G: nx.DiGraph, sources: set[str]) -> dict[str, int]:
    """
    BFS no grafo TRANSPOSTO a partir dos nós em sources.

    No transposto, uma aresta u → v do original vira v → u.
    BFS no transposto a partir de H alcança todos os nós P tais que
    existe caminho dirigido de P a H no grafo original — ou seja,
    todos os P que "podem difundir conhecimento" até H.

    Retorna dist[P] = distância de difusão do H mais próximo até P.
    """
    dist: dict[str, int] = {}
    queue: deque = deque()

    for s in sources:
        if s in G:
            dist[s] = 0
            queue.append(s)

    while queue:
        u = queue.popleft()
        # predecessores no original = sucessores no transposto
        for v in G.predecessors(u):
            if v not in dist:
                dist[v] = dist[u] + 1
                queue.append(v)

    return dist


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def build_report(
    dist_all: dict[str, int],
    unicamp_ids: set[str],
    high_impact_ids: set[str],
    meta: dict,
    min_citations: int,
    giant_size: int,
) -> str:
    ic_in_giant = [nid for nid in unicamp_ids if nid in dist_all]
    ic_dists = [dist_all[nid] for nid in ic_in_giant]

    unreachable_ic = len(unicamp_ids) - len(ic_in_giant)

    lines = []
    sep = "=" * 70
    lines.append(sep)
    lines.append("DISTÂNCIAS DE DIFUSÃO DE CONHECIMENTO: ALTO IMPACTO → IC/UNICAMP")
    lines.append(sep)
    lines.append("  Modelo: aresta A→B = 'A cita B'; conhecimento flui de B para A.")
    lines.append("  Distância de difusão de H até P = menor nº de hops no caminho")
    lines.append("  P→...→H no dígrafo original (BFS no grafo transposto a partir de H).")
    lines.append("")
    lines.append(f"  Limiar de alto impacto : ≥ {min_citations:,} citações")
    lines.append(f"  Artigos de alto impacto: {len(high_impact_ids):,}")
    lines.append(f"  Artigos da Unicamp      : {len(unicamp_ids):,}")
    lines.append(f"    — alcançados pelo BFS : {len(ic_in_giant):,}")
    lines.append(f"    — não alcançados      : {unreachable_ic:,} (sem caminho de difusão)")
    lines.append(f"  Tamanho do maior WCC    : {giant_size:,}")
    lines.append("")

    if not ic_dists:
        lines.append("  Nenhum artigo do IC alcançável no maior WCC.")
        return "\n".join(lines)

    arr = np.array(ic_dists)
    lines.append("Estatísticas das distâncias (artigos do IC no maior WCC):")
    lines.append(f"  Mínima  : {arr.min()}")
    lines.append(f"  Máxima  : {arr.max()}")
    lines.append(f"  Média   : {arr.mean():.2f}")
    lines.append(f"  Mediana : {np.median(arr):.1f}")
    lines.append(f"  Desvio padrão: {arr.std():.2f}")
    lines.append("")

    dist_counter = Counter(ic_dists)
    lines.append("Distribuição das distâncias:")
    lines.append(f"  {'Distância':>10}  {'# artigos IC':>13}  {'% artigos IC':>13}")
    lines.append(f"  {'-'*10}  {'-'*13}  {'-'*13}")
    for d in sorted(dist_counter.keys()):
        pct = 100 * dist_counter[d] / len(ic_dists)
        lines.append(f"  {d:>10}  {dist_counter[d]:>13,}  {pct:>12.1f}%")

    lines.append("")
    lines.append("Artigos do IC mais próximos de um artigo de alto impacto (top-10):")
    closest = sorted(ic_in_giant, key=lambda n: dist_all[n])[:10]
    for nid in closest:
        d = dist_all[nid]
        title = meta[nid]["title"][:60]
        year = meta[nid]["year"]
        cit = meta[nid]["cited_by_count"]
        lines.append(f"  dist={d}  [{year}] {cit:>5} cit.  {title}")

    lines.append("")
    lines.append("Artigos do IC mais distantes de um artigo de alto impacto (top-10):")
    farthest = sorted(ic_in_giant, key=lambda n: dist_all[n], reverse=True)[:10]
    for nid in farthest:
        d = dist_all[nid]
        title = meta[nid]["title"][:60]
        year = meta[nid]["year"]
        cit = meta[nid]["cited_by_count"]
        lines.append(f"  dist={d}  [{year}] {cit:>5} cit.  {title}")

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


def plot_distances(dist_all: dict, unicamp_ids: set, min_citations: int):
    ic_dists = [dist_all[n] for n in unicamp_ids if n in dist_all]
    if not ic_dists:
        print("  Nenhum dado para plotar.")
        return

    dist_counter = Counter(ic_dists)
    xs = np.array(sorted(dist_counter.keys()), dtype=float)
    ys = np.array([dist_counter[int(x)] for x in xs], dtype=float)

    # Figura 1 — histograma linear
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(xs, ys, color=PALETTE["blue"], edgecolor="white", linewidth=0.4, width=0.7)
    ax.set_xticks(xs)
    mean_d = np.average(xs, weights=ys)
    ax.axvline(mean_d, color=PALETTE["red"], linewidth=1.5, linestyle="--",
               label=f"Média = {mean_d:.2f}")
    ax.legend(fontsize=10)
    _style_ax(
        ax,
        f"Distância dos Artigos do IC ao Artigo de Alto Impacto Mais Próximo\n"
        f"(limiar: ≥ {min_citations:,} citações)",
        "Distância Mínima (hops)",
        "Número de Artigos do IC",
    )
    _savefig(fig, os.path.join(FIGURES_DIR, "distances_ic_to_highimpact.png"))

    # Figura 2 — CDF acumulada
    total = len(ic_dists)
    cumulative = np.cumsum(ys) / total
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.step(xs, cumulative, color=PALETTE["blue"], linewidth=2, where="post")
    ax.fill_between(xs, cumulative, alpha=0.15, color=PALETTE["blue"], step="post")
    ax.set_yticks(np.arange(0, 1.1, 0.1))
    ax.yaxis.set_major_formatter(ticker.PercentFormatter(xmax=1))
    ax.set_xticks(xs)
    _style_ax(
        ax,
        f"CDF — Distância dos Artigos do IC ao Alto Impacto Mais Próximo\n"
        f"(limiar: ≥ {min_citations:,} citações)",
        "Distância Mínima (hops)",
        "Fração Acumulada de Artigos do IC",
    )
    _savefig(fig, os.path.join(FIGURES_DIR, "distances_ic_cdf.png"))


def plot_all_distances(dist_all: dict, unicamp_ids: set, min_citations: int):
    """Plot distribution for all nodes (not just IC) for context."""
    all_dists = list(dist_all.values())
    if not all_dists:
        return

    dist_counter = Counter(all_dists)
    xs = np.array(sorted(dist_counter.keys()), dtype=float)
    ys_all = np.array([dist_counter[int(x)] for x in xs], dtype=float)

    ic_counter = Counter(dist_all[n] for n in unicamp_ids if n in dist_all)
    ys_ic = np.array([ic_counter.get(int(x), 0) for x in xs], dtype=float)

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(xs, ys_all, color=PALETTE["gray"], alpha=0.6, label="Todos os artigos",
           edgecolor="white", linewidth=0.4, width=0.8)
    ax.bar(xs, ys_ic, color=PALETTE["orange"], alpha=0.85, label="Artigos do IC",
           edgecolor="white", linewidth=0.4, width=0.8)
    ax.set_xticks(xs)
    ax.set_yscale("log")
    ax.legend(fontsize=10)
    _style_ax(
        ax,
        f"Distribuição de Distâncias ao Alto Impacto Mais Próximo (Escala Log)\n"
        f"(limiar: ≥ {min_citations:,} citações)",
        "Distância Mínima (hops)",
        "Número de Artigos (Escala Log)",
        logy=True,
    )
    _savefig(fig, os.path.join(FIGURES_DIR, "distances_all_vs_ic_logy.png"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--min-citations", type=int, default=500,
        help="Limiar mínimo de citações para definir artigos de alto impacto (padrão: 500)"
    )
    args = parser.parse_args()

    G, meta, unicamp_ids, high_impact_ids = load_data(args.min_citations)

    # Restringe ao maior WCC (usando o grafo subjacente não-dirigido) para
    # garantir que fontes e destinos estejam na mesma componente conexa.
    print("\nRestringindo ao maior WCC (grafo não-dirigido subjacente)...")
    largest_wcc = max(nx.weakly_connected_components(G), key=len)
    G_giant = G.subgraph(largest_wcc).copy()
    giant_size = G_giant.number_of_nodes()
    high_impact_in_giant = high_impact_ids & set(G_giant.nodes)
    print(f"  Maior WCC: {giant_size:,} nós")
    print(f"  Alto impacto no maior WCC: {len(high_impact_in_giant):,}")

    print("\nExecutando BFS multi-source no grafo transposto (difusão de conhecimento)...")
    dist_all = multisource_bfs_transposed(G_giant, high_impact_in_giant)
    reachable_ic = sum(1 for n in unicamp_ids if n in dist_all)
    print(f"  Nós alcançados pelo BFS: {len(dist_all):,} / {giant_size:,}")
    print(f"  Artigos do IC alcançados: {reachable_ic:,} / {len(unicamp_ids):,}")

    print("\nGerando relatório...")
    report_text = build_report(
        dist_all, unicamp_ids, high_impact_ids, meta,
        args.min_citations, giant_size,
    )
    report_path = os.path.join(REPORTS_DIR, "distances_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"  Salvo: {report_path}")
    print(report_text)

    print("\nGerando figuras...")
    plot_distances(dist_all, unicamp_ids, args.min_citations)
    plot_all_distances(dist_all, unicamp_ids, args.min_citations)

    print("\nConcluído. Figuras em:", FIGURES_DIR)


if __name__ == "__main__":
    main()

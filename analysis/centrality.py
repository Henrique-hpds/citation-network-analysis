"""
Análise de centralidade dos artigos do IC/UNICAMP.

Calcula PageRank e in-degree no dígrafo de citações (CITES) sobre o maior WCC.
Gera relatórios e figuras para avaliar o impacto estrutural dos artigos.

Uso:
    python analysis/centrality.py
    python analysis/centrality.py --min-citations 500   # opcional: apenas para filtro de artigos do IC nos relatórios

Saída:
  analysis/reports/centrality_report.txt
  analysis/figures/pagerank_distribution.png
"""

import argparse
import os
from collections import Counter, defaultdict
import math

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
# Data loading (reused from distances.py)
# ---------------------------------------------------------------------------

def load_data():
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

    print(f"  Artigos totais      : {len(meta):,}")
    print(f"  Artigos da Unicamp  : {len(unicamp_ids):,}")

    G = nx.DiGraph()
    G.add_nodes_from(meta.keys())
    G.add_edges_from((r["src"], r["tgt"]) for r in edges)

    return G, meta, unicamp_ids


# ---------------------------------------------------------------------------
# Centrality calculations
# ---------------------------------------------------------------------------

def compute_centralities(G: nx.DiGraph):
    """
    Compute PageRank and in-degree for the directed graph G.
    Returns two dicts: pagerank, in_degree.
    """
    print("Calculando PageRank (isso pode levar alguns minutos)...")
    # Use networkx's pagerank; default alpha=0.85, max_iter=100, tol=1e-06
    pagerank = nx.pagerank(G, alpha=0.85, max_iter=200)
    print("  PageRank calculado.")

    print("Calculando in-degree...")
    in_degree = dict(G.in_degree())
    print(f"  In-degree calculado para {len(in_degree)} nós.")

    return pagerank, in_degree


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def build_report(pagerank: dict, in_degree: dict, meta: dict, unicamp_ids: set,
                 top_n: int = 20) -> str:
    """
    Build a centrality report with:
      - Top-N articles globally by PageRank
      - Top-N articles from IC by PageRank
      - Correlation between in-degree (local) and cited_by_count (global)
    """
    lines = []
    sep = "=" * 80
    lines.append(sep)
    lines.append("ANÁLISE DE CENTRALIDADE: PAGE RANK E IN-DEGREE")
    lines.append(sep)
    lines.append("")
    lines.append(f"Total de nós no grafo: {len(pagerank):,}")
    lines.append(f"Artigos da Unicamp: {len(unicamp_ids):,}")
    lines.append("")

    # Global top PageRank
    sorted_pagerank = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)
    lines.append(f"Top {top_n} artigos globalmente por PageRank:")
    lines.append(f"  {'Rank':>4}  {'PageRank':>12}  {'Citações':>10}  {'Ano':>6}  {'Título'}")
    lines.append(f"  {'-'*4}  {'-'*12}  {'-'*10}  {'-'*6}  {'-'*60}")
    for i, (nid, pr) in enumerate(sorted_pagerank[:top_n], start=1):
        m = meta[nid]
        title = m["title"][:50]
        lines.append(
            f"  {i:>4}  {pr:>12.6f}  {m['cited_by_count']:>10,}  {m['year'] or 'N/A':>6}  {title}"
        )
    lines.append("")

    # IC top PageRank
    ic_pagerank = [(nid, pr) for nid, pr in pagerank.items() if nid in unicamp_ids]
    ic_pagerank_sorted = sorted(ic_pagerank, key=lambda x: x[1], reverse=True)
    lines.append(f"Top {top_n} artigos da Unicamp por PageRank:")
    lines.append(f"  {'Rank':>4}  {'PageRank':>12}  {'Citações':>10}  {'Ano':>6}  {'Título'}")
    lines.append(f"  {'-'*4}  {'-'*12}  {'-'*10}  {'-'*6}  {'-'*60}")
    for i, (nid, pr) in enumerate(ic_pagerank_sorted[:top_n], start=1):
        m = meta[nid]
        title = m["title"][:50]
        lines.append(
            f"  {i:>4}  {pr:>12.6f}  {m['cited_by_count']:>10,}  {m['year'] or 'N/A':>6}  {title}"
        )
    lines.append("")

    # Correlation: in-degree vs cited_by_count (only for nodes with both)
    # We'll compute Pearson correlation
    common_nodes = set(in_degree.keys()) & set(meta.keys())
    if common_nodes:
        in_deg_vals = [in_degree[n] for n in common_nodes]
        cited_vals = [meta[n]["cited_by_count"] for n in common_nodes]
        # Filter out nodes with zero cited_by_count? Keep all.
        if len(in_deg_vals) > 1:
            # Compute correlation coefficient
            mean_in = np.mean(in_deg_vals)
            mean_cit = np.mean(cited_vals)
            num = sum((x - mean_in) * (y - mean_cit) for x, y in zip(in_deg_vals, cited_vals))
            den_in = sum((x - mean_in) ** 2 for x in in_deg_vals)
            den_cit = sum((y - mean_cit) ** 2 for y in cited_vals)
            if den_in > 0 and den_cit > 0:
                pearson = num / math.sqrt(den_in * den_cit)
            else:
                pearson = 0.0
        else:
            pearson = float('nan')
    else:
        pearson = float('nan')

    lines.append("Correlação entre in-degree (local) e cited_by_count (global):")
    if not math.isnan(pearson):
        lines.append(f"  Pearson r = {pearson:.4f}")
    else:
        lines.append("  Não foi possível calcular (dados insuficientes).")
    lines.append("")

    # Additional statistics
    lines.append("Estatísticas de PageRank:")
    pr_vals = list(pagerank.values())
    lines.append(f"  Mínimo    : {min(pr_vals):.6e}")
    lines.append(f"  Máximo    : {max(pr_vals):.6e}")
    lines.append(f"  Média     : {np.mean(pr_vals):.6e}")
    lines.append(f"  Mediana   : {np.median(pr_vals):.6e}")
    lines.append(f"  Desvio-padrão: {np.std(pr_vals):.6e}")
    lines.append("")

    lines.append("Estatísticas de in-degree:")
    in_vals = list(in_degree.values())
    lines.append(f"  Mínimo    : {min(in_vals)}")
    lines.append(f"  Máximo    : {max(in_vals)}")
    lines.append(f"  Média     : {np.mean(in_vals):.2f}")
    lines.append(f"  Mediana   : {np.median(in_vals):.1f}")
    lines.append(f"  Desvio-padrão: {np.std(in_vals):.2f}")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------

def _style_ax(ax, title, xlabel, ylabel, logx=False, logy=False):
    ax.set_title(title, fontsize=13, fontweight="bold", pad=10)
    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel(ylabel, fontsize=11)
    ax.spines[["top", "right"]].set_visible(False)
    if logx:
        ax.set_xscale("log")
    if logy:
        ax.set_yscale("log")
    # Force integer ticks on y if not log
    if not logy:
        ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))


def _savefig(fig, path):
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Salvo: {path}")


def plot_pagerank_distribution(pagerank: dict, unicamp_ids: set):
    """
    Histograma log-log de PageRank, destacando os artigos do IC.
    Salva em analysis/figures/pagerank_distribution.png
    """
    all_pr = list(pagerank.values())
    ic_pr = [pagerank[nid] for nid in unicamp_ids if nid in pagerank]

    if not all_pr:
        print("  Nenhum dado de PageRank para plotar.")
        return

    # We'll histogram the values in log space
    # Use log bins
    min_pr = min(all_pr)
    max_pr = max(all_pr)
    # Avoid zero or negative values (PageRank should be positive)
    if min_pr <= 0:
        # Shift slightly? Actually PageRank is positive.
        min_pr = 1e-12

    num_bins = 50
    log_bins = np.logspace(np.log10(min_pr), np.log10(max_pr), num_bins)

    fig, ax = plt.subplots(figsize=(9, 5))
    # Histogram for all articles
    ax.hist(all_pr, bins=log_bins, alpha=0.6, label="Todos os artigos",
            color=PALETTE["gray"], edgecolor="white", linewidth=0.4)
    # Histogram for IC articles
    ax.hist(ic_pr, bins=log_bins, alpha=0.85, label="Artigos do IC",
            color=PALETTE["orange"], edgecolor="white", linewidth=0.4)

    ax.set_xticks([1e-4, 1e-3, 1e-2, 1e-1, 1e0, 1e1])  # adjust as needed
    ax.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    ax.set_yscale("log")  # frequency log scale? Actually we want log-log: x log, y log.
    # But we already set histogram; we can set y log as well.
    ax.set_yscale("log")

    _style_ax(
        ax,
        "Distribuição de PageRank (Escala Log-Log)\n"
        "Artigos do IC destacados em laranja",
        "PageRank",
        "Frequência (escala log)",
        logx=True,
        logy=True,
    )
    ax.legend(fontsize=10)
    _savefig(fig, os.path.join(FIGURES_DIR, "pagerank_distribution.png"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--min-citations", type=int, default=None,
        help="Se fornecido, filtra os artigos do IC nos relatórios top-N por aqueles com pelo menos este número de citações globais."
    )
    args = parser.parse_args()

    # Load data
    G, meta, unicamp_ids = load_data()

    # Restringe ao maior WCC (usando o grafo subjacente não-dirigido) para
    # garantir que o PageRank seja calculado na componente principal.
    print("\nRestringindo ao maior WCC (grafo não-dirigido subjacente)...")
    largest_wcc = max(nx.weakly_connected_components(G), key=len)
    G_giant = G.subgraph(largest_wcc).copy()
    giant_size = G_giant.number_of_nodes()
    print(f"  Maior WCC: {giant_size:,} nós")
    # Update meta and unicamp_ids to only those in the giant WCC? We'll keep original meta
    # but for centrality we only consider nodes in G_giant.
    nodes_in_giant = set(G_giant.nodes())
    # Filter meta and unicamp_ids to giant WCC for reporting consistency
    meta_giant = {nid: meta[nid] for nid in nodes_in_giant if nid in meta}
    unicamp_ids_giant = unicamp_ids & nodes_in_giant
    print(f"  Artigos da Unicamp no maior WCC: {len(unicamp_ids_giant):,}")

    # Compute centralities on the giant WCC
    pagerank, in_degree = compute_centralities(G_giant)

    # Build report
    print("\nGerando relatório de centralidade...")
    report_text = build_report(
        pagerank, in_degree, meta_giant, unicamp_ids_giant,
        top_n=20
    )
    report_path = os.path.join(REPORTS_DIR, "centrality_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"  Salvo: {report_path}")
    print(report_text)

    # Generate figure
    print("\nGerando figura de distribuição de PageRank...")
    plot_pagerank_distribution(pagerank, unicamp_ids_giant)

    print("\nConcluído. Figuras em:", FIGURES_DIR)


if __name__ == "__main__":
    main()
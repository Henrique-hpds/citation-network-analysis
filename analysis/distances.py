"""
Análise de distâncias de difusão entre artigos de alto impacto e artigos do IC/UNICAMP.

Modelo de difusão de conhecimento:
  No dígrafo de citações, a aresta A → B significa "A cita B".
  O conhecimento flui no sentido inverso: de B (citado) para A (que cita).
  Um caminho de difusão de H até P tem a forma:

      H  ←cita—  X₁  ←cita—  X₂  ←cita—  P

  No grafo original, isso equivale ao caminho dirigido H → X₂ → X₁ → P.
  Portanto, a distância seguida = comprimento do caminho mais
  curto de H a P no dígrafo original = BFS no grafo ORIGINAL a partir de H.

Estratégia:
  - Grafo original do dígrafo CITES, restrito ao maior WCC não-dirigido.
  - BFS multi-source a partir de todos os artigos de alto impacto no original
    → distância seguida de cada nó ao artigo de alto impacto mais próximo.
  - Reporta a distribuição dessas distâncias para os artigos do IC.

Artigos de "alto impacto" são definidos pelo flag --min-citations
(padrão: 500 citações). Inclui apenas artigos não afiliados à Unicamp
para evitar distância trivialmente zero.

Uso:
    python analysis/distances.py
    python analysis/distances.py --min-citations 1000
    python analysis/distances.py --thresholds 500 1000 10000 100000

Saída:
  analysis/reports/distances_report.txt
  analysis/figures/distances_ic_to_highimpact.png
  analysis/figures/distances_ic_cdf.png
  analysis/figures/distances_all_vs_ic_logy.png
  analysis/figures/distances_ic_by_year.png   (nova)
  analysis/figures/distances_ic_vs_citations.png (nova)

"""

import argparse
import json
import os
from collections import Counter, deque, defaultdict
import math

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import networkx as nx
import numpy as np
from dotenv import load_dotenv
from neo4j import GraphDatabase

# For statistical fitting (D1.2)
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    print("Aviso: scipy não está instalado. O ajuste de distribuições (D1.2) será pulado.")

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

def load_data(min_citations: int = None, article_ids: set[str] = None):
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

    # Determine high-impact IDs
    if article_ids is not None:
        # Use provided article IDs, but exclude Unicamp to avoid trivial distance=0
        high_impact_ids = {nid for nid in article_ids if nid not in unicamp_ids}
        print(f"  Usando lista personalizada de {len(article_ids):,} artigos")
        print(f"  Após remover artigos da Unicamp: {len(high_impact_ids):,} artigos de alto impacto")
    else:
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


def multisource_bfs_original(G: nx.DiGraph, sources: set[str]) -> dict[str, int]:
    """
    BFS no grafo ORIGINAL a partir dos nós em sources.

    No original, uma aresta u → v significa "u cita v".
    BFS no original a partir de H alcança todos os nós P tais que
    existe caminho dirigido de H a P no grafo original — ou seja,
    todos os P que são citados (direta ou indiretamente) por H.

    Retorna dist[P] = comprimento do caminho mais curto de H a P no grafo original.
    """
    dist: dict[str, int] = {}
    queue: deque = deque()

    for s in sources:
        if s in G:
            dist[s] = 0
            queue.append(s)

    while queue:
        u = queue.popleft()
        # sucessores no grafo original
        for v in G.successors(u):
            if v not in dist:
                dist[v] = dist[u] + 1
                queue.append(v)

    return dist


# ---------------------------------------------------------------------------
# Statistical fitting (D1.2)
# ---------------------------------------------------------------------------

def fit_distributions(data: list[int]) -> dict:
    """
    Fit truncated geometric and truncated Poisson distributions to the data.
    Returns a dictionary with fit parameters and goodness-of-fit measures.
    """
    if not SCIPY_AVAILABLE or not data:
        return {}

    # Data must be positive integers (distance >= 1)
    data_int = [int(x) for x in data if x >= 1]
    if not data_int:
        return {}

    # Convert to numpy array for fitting
    data_arr = np.array(data_int)
    min_val = data_arr.min()
    max_val = data_arr.max()

    # Truncated geometric distribution: P(X=k) = (1-p)^(k-1) * p for k>=1
    # We'll fit using maximum likelihood (equivalent to method of moments for geometric)
    # For truncated geometric, we can use the same MLE as geometric if we ignore truncation?
    # Instead, we'll fit a geometric distribution to the data and then compute goodness.
    # We'll use the method of moments: p = 1 / (mean)
    mean_val = np.mean(data_arr)
    if mean_val <= 0:
        p_geom = 0.5
    else:
        p_geom = 1.0 / mean_val
    # Ensure p in (0,1]
    p_geom = max(min(p_geom, 0.999), 0.001)

    # Truncated Poisson distribution: P(X=k) = (lam^k * e^-lam) / (k! * S) where S is normalization over k>=1
    # We'll fit lambda using MLE (sample mean) for Poisson, then compute goodness.
    lam_pois = mean_val

    # Compute goodness-of-fit using Kolmogorov-Smirnov test (for discrete distributions, we use the CDF)
    # We'll compute the KS statistic and p-value.
    # Note: scipy.stats.kstest can be used for discrete distributions with caution.
    # We'll compute the empirical CDF and the theoretical CDF.

    # Define the truncated PMFs
    def geometric_pmf(k, p):
        k_int = int(k)
        if k_int < 1:
            return 0
        return (1-p)**(k_int-1) * p

    def poisson_pmf(k, lam):
        k_int = int(k)
        if k_int < 1:
            return 0
        return (lam**k_int * math.exp(-lam)) / math.factorial(k_int)

    # Normalize over the observed range? Actually, we truncate at k>=1, so we need to normalize
    # the PMF over k>=1 to infinity. For geometric, the sum from k=1 to inf is 1 (already normalized).
    # For Poisson, the sum from k=1 to inf is (1 - e^-lam). So we need to divide by (1 - e^-lam).
    # We'll compute the normalized PMF for Poisson truncated at k>=1.

    # We'll compute the theoretical CDF for k from 1 to max_val (or a bit beyond) and compare with empirical CDF.
    # Let's set the support up to max_val+10 to capture the tail.
    support_max = max(max_val + 10, 50)

    # Empirical CDF
    data_sorted = np.sort(data_arr)
    n = len(data_sorted)
    ecdf = np.arange(1, n+1) / n

    # Theoretical CDF for geometric (truncated at k>=1, but geometric already starts at 1)
    geo_cdf_vals = []
    for k in range(1, support_max+1):
        # CDF = 1 - (1-p)^k
        geo_cdf_vals.append(1 - (1-p_geom)**k)
    geo_cdf_vals = np.array(geo_cdf_vals)

    # Theoretical CDF for Poisson truncated at k>=1: we need to compute CDF = (sum_{i=1}^k Poisson(i;lam)) / (1 - e^-lam)
    poisson_cdf_vals = []
    poisson_uncumulative = 0
    for k in range(1, support_max+1):
        poisson_uncumulative += poisson_pmf(k, lam_pois)
        poisson_cdf_vals.append(poisson_uncumulative / (1 - math.exp(-lam_pois)))
    poisson_cdf_vals = np.array(poisson_cdf_vals)

    # Compute KS statistic: max absolute difference between ECDF and theoretical CDF
    # We need to evaluate at the same points. We'll interpolate the theoretical CDF at the data points.
    # For simplicity, we'll compute at integer points from 1 to support_max and then compare with ECDF at those points.
    # But ECDF is defined only at data points. We'll compute the theoretical CDF at each data point.

    # Alternatively, we can use scipy.stats.kstest with a custom CDF function.
    # We'll do that for geometric and Poisson.

    def geo_cdf(x):
        if x < 1:
            return 0
        return 1 - (1-p_geom)**x

    def pois_cdf(x):
        if x < 1:
            return 0
        # Compute sum_{i=1}^x Poisson(i;lam) / (1 - e^-lam)
        total = 0
        for i in range(1, int(x)+1):
            total += poisson_pmf(i, lam_pois)
        return total / (1 - math.exp(-lam_pois))

    # Use Kstest from scipy
    try:
        geo_ks_stat, geo_ks_pval = stats.kstest(data_sorted, geo_cdf)
    except Exception as e:
        geo_ks_stat, geo_ks_pval = np.nan, np.nan

    try:
        pois_ks_stat, pois_ks_pval = stats.kstest(data_sorted, pois_cdf)
    except Exception as e:
        pois_ks_stat, pois_ks_pval = np.nan, np.nan

    # Also compute R^2 for the PMF fit? We'll compute the sum of squared errors between observed and expected frequencies.
    # Create histogram of data
    hist_counts, bin_edges = np.histogram(data_arr, bins=range(1, max_val+2), density=False)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    # Expected frequencies for geometric and Poisson (truncated)
    geo_expected = np.array([geometric_pmf(k, p_geom) for k in bin_centers]) * len(data_arr)
    pois_expected = np.array([poisson_pmf(k, lam_pois) / (1 - math.exp(-lam_pois)) for k in bin_centers]) * len(data_arr)

    # Avoid division by zero in R^2 calculation
    ss_res_geo = np.sum((hist_counts - geo_expected)**2)
    ss_tot = np.sum((hist_counts - np.mean(hist_counts))**2)
    r2_geo = 1 - (ss_res_geo / ss_tot) if ss_tot != 0 else np.nan

    ss_res_pois = np.sum((hist_counts - pois_expected)**2)
    r2_pois = 1 - (ss_res_pois / ss_tot) if ss_tot != 0 else np.nan

    return {
        "geometric": {
            "p": p_geom,
            "ks_statistic": geo_ks_stat,
            "ks_pvalue": geo_ks_pval,
            "r_squared": r2_geo,
        },
        "poisson": {
            "lambda": lam_pois,
            "ks_statistic": pois_ks_stat,
            "ks_pvalue": pois_ks_pval,
            "r_squared": r2_pois,
        }
    }


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def build_report(
    dist_all: dict[str, int],
    unicamp_ids: set[str],
    high_impact_ids: set[str],
    meta: dict,
    min_citations: int = None,
    giant_size: int = None,
    fit_results: dict = None,
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
    lines.append("  Distância seguida = menor nº de hops no caminho")
    lines.append("  H→...→P no dígrafo original (BFS no grafo original a partir de H).")
    lines.append("")

    if min_citations is not None:
        lines.append(f"  Limiar de alto impacto : ≥ {min_citations:,} citações")
    else:
        lines.append(f"  Artigos de alto impacto: {len(high_impact_ids):,} (lista personalizada)")

    lines.append(f"  Artigos de alto impacto: {len(high_impact_ids):,}")
    lines.append(f"  Artigos da Unicamp      : {len(unicamp_ids):,}")
    lines.append(f"    — alcançados pelo BFS : {len(ic_in_giant):,}")
    lines.append(f"    — não alcançados      : {unreachable_ic:,} (sem caminho de difusão)")
    if giant_size is not None:
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

    # Add statistical fitting results if available
    if fit_results and SCIPY_AVAILABLE:
        lines.append("Ajuste de distribuições estatísticas (distâncias do IC):")
        lines.append("  Distribuição geométrica truncada: P(d) = (1-p)^(d-1) * p")
        lines.append(f"    p = {fit_results['geometric']['p']:.4f}")
        lines.append(f"    KS-test statistic = {fit_results['geometric']['ks_statistic']:.4f}")
        lines.append(f"    KS-test p-value = {fit_results['geometric']['ks_pvalue']:.4f}")
        lines.append(f"    R² = {fit_results['geometric']['r_squared']:.4f}")
        lines.append("  Distribuição de Poisson truncada: P(d) = (λ^d * e^-λ) / (d! * (1-e^-λ)) para d≥1")
        lines.append(f"    λ = {fit_results['poisson']['lambda']:.4f}")
        lines.append(f"    KS-test statistic = {fit_results['poisson']['ks_statistic']:.4f}")
        lines.append(f"    KS-test p-value = {fit_results['poisson']['ks_pvalue']:.4f}")
        lines.append(f"    R² = {fit_results['poisson']['r_squared']:.4f}")
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


def build_comparative_report(thresholds_results: list[dict]) -> str:
    """
    Build a comparative report for multiple thresholds.
    thresholds_results is a list of dicts, each containing:
      - threshold: int
      - sources: int (number of high-impact articles)
      - ic_reached: int (number of IC articles reached)
      - ic_total: int (total IC articles)
      - mean_dist: float
      - median_dist: float
    """
    lines = []
    sep = "=" * 70
    lines.append(sep)
    lines.append("ANÁLISE COMPARATIVA DE DISTÂNCIAS POR LIMIAR DE ALTO IMPACTO")
    lines.append(sep)
    lines.append("")
    lines.append("Limiar (≥ citações) | Fontes de alto impacto | IC atingido (%) | Média distância | Mediana distância")
    lines.append("-" * 70)
    for res in thresholds_results:
        threshold = res["threshold"]
        sources = res["sources"]
        ic_reached = res["ic_reached"]
        ic_total = res["ic_total"]
        mean_dist = res["mean_dist"]
        median_dist = res["median_dist"]
        pct_ic = 100 * ic_reached / ic_total if ic_total > 0 else 0
        lines.append(
            f"{threshold:>19,} | {sources:>22,} | {ic_reached:>6,} / {ic_total:<6,} ({pct_ic:5.1f}%) | {mean_dist:>13.2f} | {median_dist:>15.1f}"
        )
    lines.append("")
    lines.append("Nota: O IC total é o número de artigos da Unicamp no maior WCC.")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------

def _style_ax(ax, xlabel, ylabel, logy=False):
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


def plot_distances(dist_all: dict, unicamp_ids: set, min_citations: int = None):
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
        "Distância Mínima (hops)",
        "Número de Artigos da Unicamp",
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
        "Distância Mínima (hops)",
        "Fração Acumulada de Artigos da Unicamp",
    )
    _savefig(fig, os.path.join(FIGURES_DIR, "distances_ic_cdf.png"))


def plot_all_distances(dist_all: dict, unicamp_ids: set, min_citations: int = None):
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
    ax.bar(xs, ys_ic, color=PALETTE["orange"], alpha=0.85, label="Artigos da Unicamp",
           edgecolor="white", linewidth=0.4, width=0.8)
    ax.set_xticks(xs)
    ax.set_yscale("log")
    ax.legend(fontsize=10)
    _style_ax(
        ax,
        "Distância Mínima (hops)",
        "Número de Artigos (Escala Log)",
        logy=True,
    )
    _savefig(fig, os.path.join(FIGURES_DIR, "distances_all_vs_ic_logy.png"))


def plot_distance_vs_year(dist_all: dict, unicamp_ids: set, meta: dict):
    """Scatter plot: year vs. distance for IC articles (D1.3)."""
    years = []
    dists = []
    for nid in unicamp_ids:
        if nid in dist_all:
            year = meta[nid]["year"]
            if year is not None:
                years.append(year)
                dists.append(dist_all[nid])
    if not years:
        print("  Nenhum dado de ano para plotar (D1.3).")
        return

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.scatter(years, dists, alpha=0.5, s=10, color=PALETTE["blue"])
    ax.set_xticks(np.arange(min(years), max(years)+1, 2.0))  # ticks every 2 years if range allows
    # Add a trend line (linear regression)
    if len(years) > 1:
        z = np.polyfit(years, dists, 1)
        p = np.poly1d(z)
        ax.plot(years, p(years), color=PALETTE["red"], linewidth=2, label=f"Tendência linear (coef={z[0]:.3f})")
        ax.legend(fontsize=10)
    _style_ax(
        ax,
        "Ano de publicação",
        "Distância mínima (hops)",
    )
    _savefig(fig, os.path.join(FIGURES_DIR, "distances_ic_by_year.png"))


def plot_distance_vs_citations(dist_all: dict, unicamp_ids: set, meta: dict):
    """Scatter plot: citation count vs. distance for IC articles (D1.4)."""
    citations = []
    dists = []
    for nid in unicamp_ids:
        if nid in dist_all:
            cit = meta[nid]["cited_by_count"]
            if cit is not None:
                citations.append(cit)
                dists.append(dist_all[nid])
    if not citations:
        print("  Nenhum dado de citações para plotar (D1.4).")
        return

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.scatter(citations, dists, alpha=0.5, s=10, color=PALETTE["green"])
    ax.set_xscale('log')  # citations vary widely
    _style_ax(
        ax,
        "Número de citações (escala log)",
        "Distância mínima (hops)",
    )
    _savefig(fig, os.path.join(FIGURES_DIR, "distances_ic_vs_citations.png"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def analyze_single_threshold(min_citations: int = None, article_ids: set[str] = None) -> dict:
    """
    Run the analysis for a single threshold or custom article IDs and return metrics and data needed for reporting/plotting.
    Returns a dict with:
      - threshold: int or None
      - article_ids_file: str or None
      - G: DiGraph
      - meta: dict
      - unicamp_ids: set
      - high_impact_ids: set
      - dist_all: dict
      - giant_size: int
      - sources: int (number of high-impact articles in giant WCC)
      - ic_reached: int
      - ic_total: int
      - mean_dist: float
      - median_dist: float
      - ic_dists: list (for fitting)
    """
    G, meta, unicamp_ids, high_impact_ids = load_data(min_citations, article_ids)

    # Restringe ao maior WCC (usando o grafo subjacente não-dirigido) para
    # garantir que fontes e destinos estejam na mesma componente conexa.
    print("\nRestringindo ao maior WCC (grafo não-dirigido subjacente)...")
    largest_wcc = max(nx.weakly_connected_components(G), key=len)
    G_giant = G.subgraph(largest_wcc).copy()
    giant_size = G_giant.number_of_nodes()
    high_impact_in_giant = high_impact_ids & set(G_giant.nodes)
    print(f"  Maior WCC: {giant_size:,} nós")
    print(f"  Alto impacto no maior WCC: {len(high_impact_in_giant):,}")

    print("\nExecutando BFS multi-source no grafo original (seguindo citações)...")
    dist_all = multisource_bfs_original(G_giant, high_impact_in_giant)
    reachable_ic = sum(1 for n in unicamp_ids if n in dist_all)
    ic_total = len([n for n in unicamp_ids if n in G_giant])  # IC nodes in giant WCC
    print(f"  Nós alcançados pelo BFS: {len(dist_all):,} / {giant_size:,}")
    print(f"  Artigos do IC alcançados: {reachable_ic:,} / {ic_total:,}")

    ic_dists = [dist_all[n] for n in unicamp_ids if n in dist_all]
    mean_dist = np.mean(ic_dists) if ic_dists else 0.0
    median_dist = np.median(ic_dists) if ic_dists else 0.0

    return {
        "threshold": min_citations,
        "G": G_giant,
        "meta": meta,
        "unicamp_ids": unicamp_ids,
        "high_impact_ids": high_impact_ids,
        "dist_all": dist_all,
        "giant_size": giant_size,
        "sources": len(high_impact_in_giant),
        "ic_reached": reachable_ic,
        "ic_total": ic_total,
        "mean_dist": mean_dist,
        "median_dist": median_dist,
        "ic_dists": ic_dists,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--min-citations", type=int, default=500,
        help="Limiar mínimo de citações para definir artigos de alto impacto (padrão: 500)"
    )
    parser.add_argument(
        "--article-ids-file", type=str,
        help="Caminho para arquivo JSON contendo IDs OpenAlex a serem usados como artigos de alto impacto (sobrescreve --min-citations)"
    )
    parser.add_argument(
        "--thresholds", nargs="+", type=int,
        help="Lista de limiares para análise comparativa (sobrescreve --min-citations se fornecido)"
    )
    args = parser.parse_args()

    # Handle mutually exclusive arguments: article-ids-file overrides min-citations and thresholds
    if args.article_ids_file:
        # Load article IDs from JSON file
        print(f"Carregando IDs de artigos do arquivo: {args.article_ids_file}")
        try:
            with open(args.article_ids_file, 'r') as f:
                article_ids_list = json.load(f)
            # Ensure it's a list of strings
            if not isinstance(article_ids_list, list):
                raise ValueError("O arquivo JSON deve conter uma lista de IDs")
            article_ids = set(str(item) for item in article_ids_list)
            print(f"Carregados {len(article_ids):,} IDs únicos do arquivo JSON")
        except Exception as e:
            print(f"Erro ao carregar arquivo JSON: {e}")
            return

        # Single analysis with custom article IDs
        print("\nExecutando análise com lista personalizada de artigos...")
        res = analyze_single_threshold(article_ids=article_ids)

        # Build report (without statistical fitting by default; we can add if SCIPY available)
        fit_results = None
        if SCIPY_AVAILABLE and res["ic_dists"]:
            print("\nExecutando ajuste de distribuições estatísticas (D1.2)...")
            fit_results = fit_distributions(res["ic_dists"])

        report_text = build_report(
            res["dist_all"], res["unicamp_ids"], res["high_impact_ids"], res["meta"],
            None,  # threshold is not applicable
            res["giant_size"], fit_results
        )
        report_path = os.path.join(REPORTS_DIR, "distances_report.txt")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_text)
        print(f"\nSalvo: {report_path}")
        print(report_text)

        print("\nGerando figuras...")
        plot_distances(res["dist_all"], res["unicamp_ids"], None)  # No threshold to display
        plot_all_distances(res["dist_all"], res["unicamp_ids"], None)
        plot_distance_vs_year(res["dist_all"], res["unicamp_ids"], res["meta"])
        plot_distance_vs_citations(res["dist_all"], res["unicamp_ids"], res["meta"])

        print("\nConcluído. Figuras em:", FIGURES_DIR)

    elif args.thresholds:
        # Run comparative analysis for multiple thresholds
        print("Executando análise comparativa para múltiplos limiares...")
        thresholds_results = []
        all_data = []  # store data for each threshold to generate individual reports/figures if needed
        for th in args.thresholds:
            print(f"\n{'='*50}\nProcessando limiar: {th:,} citações\n{'='*50}")
            res = analyze_single_threshold(th)
            thresholds_results.append({
                "threshold": th,
                "sources": res["sources"],
                "ic_reached": res["ic_reached"],
                "ic_total": res["ic_total"],
                "mean_dist": res["mean_dist"],
                "median_dist": res["median_dist"],
            })
            all_data.append(res)

        # Generate comparative report
        comp_report = build_comparative_report(thresholds_results)
        report_path = os.path.join(REPORTS_DIR, "distances_report_comparative.txt")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(comp_report)
        print(f"\nSalvo relatório comparativo: {report_path}")
        print(comp_report)

        # Optionally, also generate individual reports and figures for each threshold?
        # The plan does not specify, but we can generate for the first threshold (or all) if desired.
        # For simplicity, we'll generate the standard report and figures for the first threshold in the list.
        # Or we can let the user run separately for each threshold.
        # We'll just do the comparative analysis and not produce individual ones unless requested.
        # However, to maintain backward compatibility, we can also produce the standard output for the first threshold.
        if args.thresholds:
            first_th = args.thresholds[0]
            print(f"\nGerando relatório e figuras padrão para o primeiro limiar ({first_th:,})...")
            res = all_data[0]
            # Build report for this threshold (without comparative table)
            report_text = build_report(
                res["dist_all"], res["unicamp_ids"], res["high_impact_ids"], res["meta"],
                res["threshold"], res["giant_size"]
            )
            report_path = os.path.join(REPORTS_DIR, "distances_report.txt")
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(report_text)
            print(f"  Salvo: {report_path}")
            print(report_text)

            # Generate figures
            print("\nGerando figuras...")
            plot_distances(res["dist_all"], res["unicamp_ids"], res["threshold"])
            plot_all_distances(res["dist_all"], res["unicamp_ids"], res["threshold"])
            plot_distance_vs_year(res["dist_all"], res["unicamp_ids"], res["meta"])
            plot_distance_vs_citations(res["dist_all"], res["unicamp_ids"], res["meta"])

            # Statistical fitting for this threshold (D1.2)
            if SCIPY_AVAILABLE and res["ic_dists"]:
                print("\nExecutando ajuste de distribuições estatísticas (D1.2)...")
                fit_results = fit_distributions(res["ic_dists"])
                # Update the report with fit results? We already have a separate comparative report.
                # We could append to the standard report, but let's keep the standard report as before.
                # Instead, we can print the fit results here.
                print("Resultados do ajuste:")
                print(f"  Geométrica: p = {fit_results['geometric']['p']:.4f}, R² = {fit_results['geometric']['r_squared']:.4f}")
                print(f"  Poisson: λ = {fit_results['poisson']['lambda']:.4f}, R² = {fit_results['poisson']['r_squared']:.4f}")

    else:
        # Single threshold analysis (original behavior)
        min_citations = args.min_citations
        res = analyze_single_threshold(min_citations)

        # Build report (without statistical fitting by default; we can add if SCIPY available)
        fit_results = None
        if SCIPY_AVAILABLE and res["ic_dists"]:
            print("\nExecutando ajuste de distribuições estatísticas (D1.2)...")
            fit_results = fit_distributions(res["ic_dists"])

        report_text = build_report(
            res["dist_all"], res["unicamp_ids"], res["high_impact_ids"], res["meta"],
            res["threshold"], res["giant_size"], fit_results
        )
        report_path = os.path.join(REPORTS_DIR, "distances_report.txt")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_text)
        print(f"\nSalvo: {report_path}")
        print(report_text)

        print("\nGerando figuras...")
        plot_distances(res["dist_all"], res["unicamp_ids"], res["threshold"])
        plot_all_distances(res["dist_all"], res["unicamp_ids"], res["threshold"])
        plot_distance_vs_year(res["dist_all"], res["unicamp_ids"], res["meta"])
        plot_distance_vs_citations(res["dist_all"], res["unicamp_ids"], res["meta"])

        print("\nConcluído. Figuras em:", FIGURES_DIR)


if __name__ == "__main__":
    main()
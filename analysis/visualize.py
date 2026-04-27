"""
Análise visual do grafo de citações construído no Neo4j.

Gera:
  1. Estatísticas básicas do grafo (vértices, arestas, grau médio)
  2. Distribuição dos graus dos vértices (Article)
  3. Número de componentes fracamente conexas (grafo direcionado → WCC)
  4. Distribuição do tamanho das componentes (se houver mais de uma)

Saída: analysis/figures/
"""

import os
import sys
from collections import defaultdict, deque

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from dotenv import load_dotenv
from neo4j import GraphDatabase

# ---------------------------------------------------------------------------
# Conexão
# ---------------------------------------------------------------------------

load_dotenv()

_driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")),
)

def query(cypher: str, params: dict = None):
    with _driver.session(database=os.getenv("NEO4J_DATABASE")) as s:
        result = s.run(cypher, params or {})
        return [r.data() for r in result]

# ---------------------------------------------------------------------------
# 1. Tamanho do grafo
# ---------------------------------------------------------------------------

def fetch_graph_stats():
    counts = {}
    for label in ("Article", "Author", "Institution", "Venue", "Subfield"):
        r = query(f"MATCH (n:{label}) RETURN count(n) AS c")
        counts[label] = r[0]["c"]

    rels = {}
    for rel in ("CITES", "AUTHORED_BY", "PUBLISHED_IN", "HAS_SUBFIELD", "AFFILIATED_WITH"):
        r = query(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c")
        rels[rel] = r[0]["c"]

    # Grau médio considera apenas artigos e arestas CITES (o grafo de citações)
    r = query("""
        MATCH (a:Article)
        OPTIONAL MATCH (a)-[out:CITES]->()
        WITH a, count(out) AS out_deg
        OPTIONAL MATCH ()-[inn:CITES]->(a)
        WITH a, out_deg, count(inn) AS in_deg
        RETURN avg(toFloat(out_deg + in_deg)) AS avg_deg
    """)
    avg_deg = r[0]["avg_deg"]

    return counts, rels, avg_deg

# ---------------------------------------------------------------------------
# 2. Distribuição de graus
# ---------------------------------------------------------------------------

def fetch_degree_distribution():
    rows = query("""
        MATCH (a:Article)
        OPTIONAL MATCH (a)-[out:CITES]->()
        WITH a, count(out) AS out_deg
        OPTIONAL MATCH ()-[inn:CITES]->(a)
        WITH a, out_deg, count(inn) AS in_deg
        WITH out_deg + in_deg AS degree
        RETURN degree, count(*) AS num_nodes
        ORDER BY degree
    """)
    degrees   = [r["degree"]    for r in rows]
    num_nodes = [r["num_nodes"] for r in rows]
    return degrees, num_nodes

# ---------------------------------------------------------------------------
# 3 & 4. Componentes (WCC — grafo não direcionado subjacente)
# ---------------------------------------------------------------------------

def fetch_wcc():
    nodes = query("MATCH (a:Article) RETURN a.openalex_id AS id")
    edges = query("""
        MATCH (a:Article)-[:CITES]->(b:Article)
        RETURN a.openalex_id AS src, b.openalex_id AS dst
    """)

    adj = defaultdict(set)
    all_nodes = {r["id"] for r in nodes}
    for e in edges:
        adj[e["src"]].add(e["dst"])
        adj[e["dst"]].add(e["src"])

    visited = set()
    components = []

    for node in all_nodes:
        if node in visited:
            continue
        # BFS
        queue = deque([node])
        visited.add(node)
        size = 0
        while queue:
            cur = queue.popleft()
            size += 1
            for nb in adj[cur]:
                if nb not in visited:
                    visited.add(nb)
                    queue.append(nb)
        components.append(size)

    return sorted(components, reverse=True)

# ---------------------------------------------------------------------------
# 5 & 6. Componentes fortemente conexas (SCC — Kosaraju)
# ---------------------------------------------------------------------------

def fetch_scc():
    nodes = query("MATCH (a:Article) RETURN a.openalex_id AS id")
    edges = query("""
        MATCH (a:Article)-[:CITES]->(b:Article)
        RETURN a.openalex_id AS src, b.openalex_id AS dst
    """)

    all_nodes = [r["id"] for r in nodes]
    adj  = defaultdict(set)
    radj = defaultdict(set)
    for e in edges:
        adj[e["src"]].add(e["dst"])
        radj[e["dst"]].add(e["src"])

    # Passo 1: ordem de término (DFS iterativa no grafo original)
    visited = set()
    finish_order = []
    for start in all_nodes:
        if start in visited:
            continue
        stack = [(start, False)]
        while stack:
            node, returning = stack.pop()
            if returning:
                finish_order.append(node)
                continue
            if node in visited:
                continue
            visited.add(node)
            stack.append((node, True))
            for nb in adj[node]:
                if nb not in visited:
                    stack.append((nb, False))

    # Passo 2: DFS no grafo transposto na ordem inversa de término
    visited2 = set()
    components = []
    for start in reversed(finish_order):
        if start in visited2:
            continue
        queue = deque([start])
        visited2.add(start)
        size = 0
        while queue:
            cur = queue.popleft()
            size += 1
            for nb in radj[cur]:
                if nb not in visited2:
                    visited2.add(nb)
                    queue.append(nb)
        components.append(size)

    return sorted(components, reverse=True)


# ---------------------------------------------------------------------------
# Figuras
# ---------------------------------------------------------------------------

FIGURES_DIR = os.path.join(os.path.dirname(__file__), "figures")
os.makedirs(FIGURES_DIR, exist_ok=True)

PALETTE = {
    "blue":  "#2563EB",
    "gray":  "#6B7280",
    "red":   "#DC2626",
    "green": "#16A34A",
}

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


# --- Distribuição de graus: 3 figuras separadas ---

def plot_degree_distribution(degrees, num_nodes):
    d = np.array(degrees, dtype=float)
    n = np.array(num_nodes, dtype=float)

    # Limite do eixo X: percentil 99 dos graus (ponderado por num_nodes) + margem
    all_degrees_expanded = np.repeat(d, n.astype(int))
    x_cap = int(np.percentile(all_degrees_expanded, 99)) + 5

    # Figura 1 — linear
    fig, ax = plt.subplots(figsize=(8, 5))
    mask_lin = d <= x_cap
    ax.bar(d[mask_lin], n[mask_lin], color=PALETTE["blue"], edgecolor="white", linewidth=0.4)
    ax.set_xlim(-0.5, x_cap + 0.5)
    _style_ax(ax, "Distribuição de Graus da Rede de Citações",
              "Grau (Número de Conexões)", "Frequência (Número de Artigos)")
    _savefig(fig, os.path.join(FIGURES_DIR, "degree_distribution_linear.png"))

    # Figura 2 — log-y (histograma com eixo y em escala log)
    fig, ax = plt.subplots(figsize=(8, 5))
    mask_lin = (n > 0) & (d <= x_cap)
    ax.bar(d[mask_lin], n[mask_lin], color=PALETTE["blue"], edgecolor="white", linewidth=0.4)
    ax.set_yscale("log")
    ax.set_xlim(-0.5, x_cap + 0.5)
    _style_ax(ax, "Distribuição de Graus da Rede de Citações (Escala Log)",
              "Grau (Número de Conexões)", "Frequência (Número de Artigos) — Escala Log", logy=True)
    _savefig(fig, os.path.join(FIGURES_DIR, "degree_distribution_logy.png"))

    # Figura 3 — log-log (scatter + fit, usa todos os pontos)
    fig, ax = plt.subplots(figsize=(8, 5))
    mask = (d > 0) & (n > 0)
    ax.scatter(d[mask], n[mask], color=PALETTE["red"], s=20, alpha=0.85, zorder=3)
    if mask.sum() >= 2:
        coeffs = np.polyfit(np.log10(d[mask]), np.log10(n[mask]), 1)
        x_fit  = np.linspace(d[mask].min(), d[mask].max(), 200)
        y_fit  = 10 ** np.polyval(coeffs, np.log10(x_fit))
        ax.plot(x_fit, y_fit, color=PALETTE["red"], linewidth=1.2,
                label=f"γ ≈ {-coeffs[0]:.2f}")
        ax.legend(fontsize=10)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(ticker.ScalarFormatter())
    _style_ax(ax, "Distribuição de Graus da Rede de Citações (Log-Log)",
              "Grau (Número de Conexões)", "Frequência (Número de Artigos)", logy=True)
    _savefig(fig, os.path.join(FIGURES_DIR, "degree_distribution_loglog.png"))


# --- Componentes fracamente conexas: 3 figuras separadas ---

def plot_component_sizes(components):
    if len(components) <= 1:
        print("  Apenas 1 componente WCC — figuras de distribuição não geradas.")
        return

    size_count = defaultdict(int)
    for s in components:
        size_count[s] += 1

    giant  = components[0]
    sizes  = np.array(sorted(size_count.keys()), dtype=float)
    counts = np.array([size_count[int(s)] for s in sizes], dtype=float)

    xlabel = "Tamanho da Componente (Número de Vértices)"
    ylabel = "Frequência (Número de Componentes)"
    title_base = "Distribuição do Tamanho das Componentes Fracamente Conexas"

    mask_small = sizes < giant
    s_small = sizes[mask_small]
    c_small = counts[mask_small]
    min_gap = float(np.diff(s_small).min()) if len(s_small) > 1 else 1.0
    bar_w = min_gap * 0.8

    # Figura 1 — linear (sem o gigante)
    fig, ax = plt.subplots(figsize=(8, 5))
    if mask_small.any():
        ax.bar(s_small, c_small, width=bar_w,
               color=PALETTE["green"], edgecolor="white", linewidth=0.4)
        ax.set_xticks(s_small)
        ax.set_xticklabels([str(int(s)) for s in s_small],
                           rotation=45, ha="right", fontsize=8)
        ax.set_xlim(s_small.min() - min_gap, s_small.max() + min_gap)
    _style_ax(ax, f"{title_base}\n(excluindo componente gigante k={giant:,})", xlabel, ylabel)
    _savefig(fig, os.path.join(FIGURES_DIR, "wcc_size_distribution_linear.png"))

    # Figura 2 — log-y (sem o gigante)
    fig, ax = plt.subplots(figsize=(8, 5))
    if mask_small.any():
        ax.bar(s_small, c_small, width=bar_w,
               color=PALETTE["green"], edgecolor="white", linewidth=0.4)
        ax.set_xticks(s_small)
        ax.set_xticklabels([str(int(s)) for s in s_small],
                           rotation=45, ha="right", fontsize=8)
        ax.set_xlim(s_small.min() - min_gap, s_small.max() + min_gap)
    ax.set_yscale("log")
    _style_ax(ax, f"{title_base} (Escala Log)\n(excluindo componente gigante k={giant:,})",
              xlabel, f"{ylabel} — Escala Log", logy=True)
    _savefig(fig, os.path.join(FIGURES_DIR, "wcc_size_distribution_logy.png"))

    # Figura 3 — log-log (todas as componentes)
    fig, ax = plt.subplots(figsize=(8, 5))
    mask_pos = counts > 0
    ax.scatter(sizes[mask_pos], counts[mask_pos],
               color=PALETTE["green"], s=40, alpha=0.85, zorder=3)
    ax.scatter([giant], [size_count[giant]],
               color=PALETTE["red"], s=70, zorder=4, label=f"Gigante (k={giant:,})")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.legend(fontsize=9)
    ax.yaxis.set_major_formatter(ticker.ScalarFormatter())
    _style_ax(ax, f"{title_base} (Log-Log)", xlabel, ylabel, logy=True)
    _savefig(fig, os.path.join(FIGURES_DIR, "wcc_size_distribution_loglog.png"))


# --- Componentes fortemente conexas: 3 figuras separadas ---

def plot_scc_sizes(components):
    if len(components) <= 1:
        print("  Apenas 1 componente SCC — figuras de distribuição não geradas.")
        return

    size_count = defaultdict(int)
    for s in components:
        size_count[s] += 1

    giant  = components[0]
    sizes  = np.array(sorted(size_count.keys()), dtype=float)
    counts = np.array([size_count[int(s)] for s in sizes], dtype=float)

    xlabel = "Tamanho da Componente (Número de Vértices)"
    ylabel = "Frequência (Número de Componentes)"
    title_base = "Distribuição do Tamanho das Componentes Fortemente Conexas"

    mask_small = sizes < giant
    s_small = sizes[mask_small]
    c_small = counts[mask_small]
    min_gap = float(np.diff(s_small).min()) if len(s_small) > 1 else 1.0
    bar_w = min_gap * 0.8

    # Figura 1 — linear (sem o gigante)
    fig, ax = plt.subplots(figsize=(8, 5))
    if mask_small.any():
        ax.bar(s_small, c_small, width=bar_w,
               color=PALETTE["blue"], edgecolor="white", linewidth=0.4)
        ax.set_xticks(s_small)
        ax.set_xticklabels([str(int(s)) for s in s_small],
                           rotation=45, ha="right", fontsize=8)
        ax.set_xlim(s_small.min() - min_gap, s_small.max() + min_gap)
    _style_ax(ax, f"{title_base}\n(excluindo componente gigante k={giant:,})", xlabel, ylabel)
    _savefig(fig, os.path.join(FIGURES_DIR, "scc_size_distribution_linear.png"))

    # Figura 2 — log-y (sem o gigante)
    fig, ax = plt.subplots(figsize=(8, 5))
    if mask_small.any():
        ax.bar(s_small, c_small, width=bar_w,
               color=PALETTE["blue"], edgecolor="white", linewidth=0.4)
        ax.set_xticks(s_small)
        ax.set_xticklabels([str(int(s)) for s in s_small],
                           rotation=45, ha="right", fontsize=8)
        ax.set_xlim(s_small.min() - min_gap, s_small.max() + min_gap)
    ax.set_yscale("log")
    _style_ax(ax, f"{title_base} (Escala Log)\n(excluindo componente gigante k={giant:,})",
              xlabel, f"{ylabel} — Escala Log", logy=True)
    _savefig(fig, os.path.join(FIGURES_DIR, "scc_size_distribution_logy.png"))

    # Figura 3 — log-log (todas as componentes)
    fig, ax = plt.subplots(figsize=(8, 5))
    mask_pos = counts > 0
    ax.scatter(sizes[mask_pos], counts[mask_pos],
               color=PALETTE["blue"], s=40, alpha=0.85, zorder=3)
    ax.scatter([giant], [size_count[giant]],
               color=PALETTE["red"], s=70, zorder=4, label=f"Gigante (k={giant:,})")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.legend(fontsize=9)
    ax.yaxis.set_major_formatter(ticker.ScalarFormatter())
    _style_ax(ax, f"{title_base} (Log-Log)", xlabel, ylabel, logy=True)
    _savefig(fig, os.path.join(FIGURES_DIR, "scc_size_distribution_loglog.png"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("\n=== Coletando dados do Neo4j ===")
    counts, rels, avg_deg = fetch_graph_stats()
    degrees, num_nodes     = fetch_degree_distribution()
    components             = fetch_wcc()
    scc_components         = fetch_scc()

    n_articles = counts["Article"]
    n_cites    = rels["CITES"]

    print("\n=== Tamanho do grafo (foco em Article + CITES) ===")
    print(f"  Vértices (Article):  {n_articles:,}")
    print(f"  Arestas  (CITES):    {n_cites:,}")
    print(f"  Grau médio:          {avg_deg:.4f}")
    print("\n  Outros nós:")
    for label in ("Author", "Institution", "Venue", "Subfield"):
        print(f"    {label}: {counts[label]:,}")
    print("\n  Outras arestas:")
    for rel in ("AUTHORED_BY", "PUBLISHED_IN", "HAS_SUBFIELD", "AFFILIATED_WITH"):
        print(f"    {rel}: {rels[rel]:,}")

    print("\n=== Componentes fracamente conexas ===")
    print(f"  Total de componentes: {len(components)}")
    print(f"  Maior componente:     {components[0]:,} vértices")
    if len(components) > 1:
        print(f"  Menor componente:     {components[-1]} vértice(s)")
        print(f"  Componentes unitárias (k=1): {sum(1 for c in components if c == 1)}")

    print("\n=== Componentes fortemente conexas ===")
    print(f"  Total de componentes: {len(scc_components)}")
    print(f"  Maior componente:     {scc_components[0]:,} vértices")
    if len(scc_components) > 1:
        print(f"  Menor componente:     {scc_components[-1]} vértice(s)")
        print(f"  Triviais (k=1):       {sum(1 for c in scc_components if c == 1)}")

    print("\n=== Gerando figuras ===")
    plot_degree_distribution(degrees, num_nodes)
    plot_component_sizes(components)
    plot_scc_sizes(scc_components)

    print("\nConcluído. Figuras em:", FIGURES_DIR)
    _driver.close()


if __name__ == "__main__":
    main()

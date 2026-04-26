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

def _style_ax(ax, title, xlabel, ylabel):
    ax.set_title(title, fontsize=13, fontweight="bold", pad=10)
    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel(ylabel, fontsize=11)
    ax.spines[["top", "right"]].set_visible(False)
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))


def plot_degree_distribution(degrees, num_nodes):
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Distribuição dos Graus — Grafo de Citações (Article)", fontsize=14, fontweight="bold")

    total = sum(num_nodes)

    # --- linear ---
    ax = axes[0]
    ax.bar(degrees, num_nodes, color=PALETTE["blue"], edgecolor="white", linewidth=0.4)
    ax.set_xlim(-1, 100)
    _style_ax(ax, "Escala linear", "Grau (k)", "Número de vértices")

    # --- log-log ---
    ax = axes[1]
    d = np.array(degrees, dtype=float)
    n = np.array(num_nodes, dtype=float)
    mask = (d > 0) & (n > 0)
    ax.scatter(d[mask], n[mask], color=PALETTE["blue"], s=40, alpha=0.8, zorder=3)

    if mask.sum() >= 2:
        coeffs = np.polyfit(np.log10(d[mask]), np.log10(n[mask]), 1)
        x_fit = np.linspace(d[mask].min(), d[mask].max(), 200)
        y_fit = 10 ** np.polyval(coeffs, np.log10(x_fit))
        ax.plot(x_fit, y_fit, color=PALETTE["red"], linewidth=1.5,
                label=f"γ ≈ {-coeffs[0]:.2f}")
        ax.legend(fontsize=10)

    ax.set_xscale("log")
    ax.set_yscale("log")
    _style_ax(ax, "Escala log-log", "Grau (k)", "Número de vértices")
    ax.yaxis.set_major_locator(ticker.LogLocator(numticks=6))
    ax.yaxis.set_major_formatter(ticker.ScalarFormatter())

    fig.tight_layout()
    path = os.path.join(FIGURES_DIR, "degree_distribution.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Salvo: {path}")


def plot_component_sizes(components):
    if len(components) <= 1:
        print("  Apenas 1 componente — figura de distribuição não gerada.")
        return

    # Contagem: quantas componentes têm exatamente k vértices
    size_count = defaultdict(int)
    for s in components:
        size_count[s] += 1

    sizes  = np.array(sorted(size_count.keys()), dtype=float)
    counts = np.array([size_count[int(s)] for s in sizes], dtype=float)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle(
        f"Distribuição do tamanho das componentes fracamente conexas\n"
        f"({len(components)} componentes  |  maior: {components[0]:,} vértices  |  "
        f"isoladas (k=1): {size_count[1]})",
        fontsize=13, fontweight="bold",
    )

    # --- escala linear (exclui o gigante para não esmagar os menores) ---
    ax = axes[0]
    giant = components[0]
    mask_small = sizes < giant
    if mask_small.any():
        ax.bar(sizes[mask_small], counts[mask_small],
               color=PALETTE["green"], edgecolor="white", linewidth=0.4)
    ax.set_xticks(sizes[mask_small])
    ax.set_xticklabels([str(int(s)) for s in sizes[mask_small]], rotation=45, ha="right", fontsize=8)
    _style_ax(ax, f"Escala linear (excluindo componente gigante k={giant:,})",
              "Tamanho da componente (k vértices)", "Número de componentes")

    # --- escala log-log (todas as componentes) ---
    ax = axes[1]
    mask_pos = counts > 0
    ax.scatter(sizes[mask_pos], counts[mask_pos],
               color=PALETTE["green"], s=50, alpha=0.85, zorder=3)
    # destacar o gigante
    ax.scatter([giant], [size_count[giant]],
               color=PALETTE["red"], s=80, zorder=4, label=f"Gigante (k={giant:,})")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.legend(fontsize=9)
    _style_ax(ax, "Escala log-log (todas as componentes)",
              "Tamanho da componente k (log)", "Número de componentes (log)")
    ax.yaxis.set_major_formatter(ticker.ScalarFormatter())

    fig.tight_layout()
    path = os.path.join(FIGURES_DIR, "component_size_distribution.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Salvo: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("\n=== Coletando dados do Neo4j ===")
    counts, rels, avg_deg = fetch_graph_stats()
    degrees, num_nodes     = fetch_degree_distribution()
    components             = fetch_wcc()

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

    print("\n=== Gerando figuras ===")
    plot_degree_distribution(degrees, num_nodes)
    plot_component_sizes(components)

    print("\nConcluído. Figuras em:", FIGURES_DIR)
    _driver.close()


if __name__ == "__main__":
    main()

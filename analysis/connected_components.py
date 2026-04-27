"""
components.py

Loads the CITES graph from Neo4j and computes:
  - Weakly  connected components (WCC)
  - Strongly connected components (SCC)

For each component reports:
  - Size (number of nodes)
  - Number of Unicamp articles within it
"""

import os
from collections import Counter

import networkx as nx
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")),
)

# ---------------------------------------------------------------------------
# Load graph + unicamp flags
# ---------------------------------------------------------------------------

print("Fetching edges...")
with driver.session() as session:
    edges = session.run("""
        MATCH (a:Article)-[:CITES]->(b:Article)
        RETURN a.openalex_id AS src, b.openalex_id AS tgt
    """).data()

    print("Fetching Unicamp article IDs...")
    unicamp_ids = {
        r["id"] for r in session.run("""
            MATCH (a:Article)-[:AFFILIATED_WITH]->(i:Institution {openalex_id: "I181391015"})
            RETURN a.openalex_id AS id
        """).data()
    }

driver.close()

print(f"  {len(edges):,} edges  |  {len(unicamp_ids):,} Unicamp articles")

G = nx.DiGraph()
G.add_edges_from((r["src"], r["tgt"]) for r in edges)
print(f"  {G.number_of_nodes():,} nodes in graph")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def report(label: str, components):
    comps  = list(components)
    total  = len(comps)

    print(f"\n{'='*60}")
    print(f"{label}  —  {total:,} components")
    print(f"{'='*60}")
    print(f"  {'Rank':>6}  {'Size':>10}  {'Unicamp nodes':>15}")
    print(f"  {'-'*6}  {'-'*10}  {'-'*15}")

    sorted_comps = sorted(comps, key=len, reverse=True)
    for rank, comp in enumerate(sorted_comps, 1):
        n_uni = len(set(comp) & unicamp_ids)
        print(f"  {rank:>6}  {len(comp):>10,}  {n_uni:>15,}")

    all_sizes = [len(c) for c in comps]
    print(f"\n  Largest   : {max(all_sizes):,} nodes")
    print(f"  Median    : {sorted(all_sizes)[len(all_sizes)//2]:,} nodes")
    print(f"  Singletons: {sum(1 for s in all_sizes if s == 1):,}")
    print(f"  Unicamp nodes in largest: "
          f"{len(set(sorted_comps[0]) & unicamp_ids):,}")

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

cur_dir = os.path.dirname(os.path.abspath(__file__))
os.makedirs(f"{cur_dir}/reports", exist_ok=True)

with open(f"{cur_dir}/reports/components_report.txt", "w", encoding="utf-8") as f:
    os.sys.stdout = f  # redirect print to file
    report("WEAKLY  CONNECTED COMPONENTS",
        nx.weakly_connected_components(G))

    report("STRONGLY CONNECTED COMPONENTS",
        nx.strongly_connected_components(G))
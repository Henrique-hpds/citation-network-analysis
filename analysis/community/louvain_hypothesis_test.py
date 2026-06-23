import os
from collections import defaultdict

import community as community_louvain
import matplotlib
matplotlib.use("Agg")
import networkx as nx
import numpy as np
from dotenv import load_dotenv
from neo4j import GraphDatabase
import json 

load_dotenv()

UNICAMP_ID = "I181391015"

driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")),
)


def _run(cypher: str, params: dict = None):
    with driver.session() as s:
        return [r.data() for r in s.run(cypher, params or {})]


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

def run_louvain(U, seed=42):
    partition = community_louvain.best_partition(U, random_state=seed)
    modularity = community_louvain.modularity(partition, U)
    n_communities = len(set(partition.values()))
    return partition, modularity, n_communities

def summarize_seed_effect(modularities):
    values = np.array(list(modularities.values()))

    return {
        "mean": float(values.mean()),
        "std": float(values.std(ddof=1)),
        "min": float(values.min()),
        "max": float(values.max()),
        "range": float(values.max() - values.min()),
        "cv": float(values.std(ddof=1) / values.mean()),
    }

def monte_carlo_modularity_test(U, observed_Q, n_null=100, seed_offset=10_000):
    null_Qs = []

    for i in range(n_null):
        R = U.copy()
        nx.double_edge_swap(
            R,
            nswap=10 * R.number_of_edges(),
            max_tries=100 * R.number_of_edges(),
            seed=seed_offset + i,
        )

        if i == 0:
            deg_before = sorted(d for _, d in U.degree())
            deg_after = sorted(d for _, d in R.degree())
            assert deg_before == deg_after, "double_edge_swap changed the degrees"

            edges_changed = sum(1 for e in R.edges() if not U.has_edge(*e))
            print(f"[sanity] {edges_changed:,}/{R.number_of_edges():,} edges differ from orignial")

        _, q_null, _ = run_louvain(R, seed=seed_offset + i)
        null_Qs.append(q_null)
        print(f"Null {i + 1}/{n_null} done")

    null_Qs = np.array(null_Qs)
    p_value = (np.sum(null_Qs >= observed_Q) + 1) / (n_null + 1)

    return {
        "observed_Q": float(observed_Q),
        "null_mean": float(null_Qs.mean()),
        "null_std": float(null_Qs.std(ddof=1)),
        "p_value": float(p_value),
        "n_null": n_null,
        "null_Qs": null_Qs.tolist(),
    }

def main():
    G, _, _ = load_graph()

    U = G.to_undirected()

    largest_wcc = max(nx.connected_components(U), key=len)
    U_giant = U.subgraph(largest_wcc).copy()

    all_modularities = {}
    all_n_communities = {}

    partial_result_path = "analysis/reports/louvain_modularity_tests_partial.json"
    final_result_path = "analysis/reports/louvain_modularity_tests.json"

    if os.path.exists(partial_result_path):
        with open(partial_result_path, "r", encoding="utf-8") as fp:
            results = json.load(fp)
            all_modularities = results["seed_modularities"]
            all_n_communities = results["seed_n_communities"]
            str_seeds = [seed for seed in all_modularities.keys()]
            for seed in str_seeds:
                all_modularities[int(seed)] = all_modularities[seed]
                all_modularities.pop(seed)
                all_n_communities[int(seed)] = all_n_communities[seed]
                all_n_communities.pop(seed)
        print(f"Loaded {len(all_modularities)} partial results")

    print(all_modularities)
    print(all_n_communities)

    for seed in range(1000):
        if seed in all_modularities:
            continue

        print(f"Processing seed: {seed}")
        _, modularity, n_communities = run_louvain(U_giant, seed)

        all_modularities[seed] = modularity
        all_n_communities[seed] = n_communities

        results = {
            "seed_modularities": all_modularities,
            "seed_n_communities": all_n_communities,
        }

        with open(partial_result_path, "w+", encoding="utf-8") as fp:
            json.dump(results, fp)

    print("Summarization")
    seed_summary = summarize_seed_effect(all_modularities)

    print("Monte Carlo Test")
    significance_result = monte_carlo_modularity_test(
        U_giant,
        observed_Q=max(all_modularities.values()),
        n_null=100,
    )

    results = {
        "seed_modularities": all_modularities,
        "seed_n_communities": all_n_communities,
        "seed_summary": seed_summary,
        "significance_test": significance_result,
    }

    print("Writing results")
    with open(final_result_path, "w", encoding="utf-8") as fp:
        json.dump(results, fp, indent=2)

    print("Seed effect:")
    print(seed_summary)

    print("Significance test:")
    print(f"Observed Q: {significance_result['observed_Q']:.4f}")
    print(f"Null mean Q: {significance_result['null_mean']:.4f}")
    print(f"p-value: {significance_result['p_value']:.4f}")


if __name__ == "__main__":
    main()

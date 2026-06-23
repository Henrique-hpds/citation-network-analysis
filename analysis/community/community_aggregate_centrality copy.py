"""Calcula centralidades exatas e as agrega por comunidade."""

from collections import Counter
from statistics import correlation

import networkx as nx

from helper_functions import (
    TABLES_DIR,
    load_citation_graph,
    load_official_partition,
    pagerank_power_iteration,
    write_csv,
)


def compute_node_centralities(graph):
    print("Calculando PageRank exato...")
    pagerank = pagerank_power_iteration(graph)
    print("Calculando betweenness exata...")
    betweenness = nx.betweenness_centrality(graph, normalized=True)
    return pagerank, betweenness


def aggregate_centralities(graph, partition, pagerank, betweenness):
    sizes = Counter(partition.values())
    pagerank_sum = Counter()
    betweenness_sum = Counter()
    in_degree_sum = Counter()
    article_rows = []

    for node, community_id in partition.items():
        node_in_degree = graph.in_degree(node)
        pagerank_sum[community_id] += pagerank[node]
        betweenness_sum[community_id] += betweenness[node]
        in_degree_sum[community_id] += node_in_degree
        article_rows.append({
            "openalex_id": node,
            "community_id": community_id,
            "pagerank": pagerank[node],
            "betweenness": betweenness[node],
            "in_degree": node_in_degree,
        })

    community_rows = [
        {
            "community_id": community_id,
            "pagerank_sum": pagerank_sum[community_id],
            "betweenness_sum": betweenness_sum[community_id],
            "mean_in_degree": in_degree_sum[community_id] / size,
        }
        for community_id, size in sizes.items()
    ]
    community_rows.sort(key=lambda row: row["community_id"])
    article_rows.sort(key=lambda row: row["openalex_id"])
    return community_rows, article_rows


def evaluate_hypothesis(rows):
    centrality_correlation = correlation(
        [row["pagerank_sum"] for row in rows],
        [row["betweenness_sum"] for row in rows],
    )
    return [{
        "hypothesis": "aggregate_pagerank_correlates_with_aggregate_betweenness",
        "correlation_threshold": 0.7,
        "pearson_correlation": centrality_correlation,
        "hypothesis_supported": centrality_correlation > 0.7,
        "pagerank_total": sum(row["pagerank_sum"] for row in rows),
        "betweenness_total": sum(row["betweenness_sum"] for row in rows),
        "betweenness_method": "exact_directed_normalized",
    }]


def main():
    partition = load_official_partition()
    graph = load_citation_graph().subgraph(partition).copy()
    print(f"Grafo: {graph.number_of_nodes():,} nós, {graph.number_of_edges():,} arestas")

    pagerank, betweenness = compute_node_centralities(graph)
    community_rows, article_rows = aggregate_centralities(
        graph,
        partition,
        pagerank,
        betweenness,
    )
    summary = evaluate_hypothesis(community_rows)

    write_csv(
        TABLES_DIR / "article_centrality.csv",
        ["openalex_id", "community_id", "pagerank", "betweenness", "in_degree"],
        article_rows,
    )
    write_csv(
        TABLES_DIR / "community_aggregate_centrality.csv",
        ["community_id", "pagerank_sum", "betweenness_sum", "mean_in_degree"],
        community_rows,
    )
    write_csv(
        TABLES_DIR / "community_aggregate_centrality_summary.csv",
        list(summary[0]),
        summary,
    )

    print(f"Comunidades analisadas: {len(community_rows)}")
    print(f"Hipótese confirmada: {summary[0]['hypothesis_supported']}")
    print(f"Tabelas salvas em {TABLES_DIR}")


if __name__ == "__main__":
    main()

"""Calcula centralidades globais dos artigos e agrega por comunidade.

Objetivo:
    Medir a importância estrutural de cada comunidade a partir das
    centralidades dos artigos que a compõem. O PageRank captura prestígio no
    fluxo global de citações; a betweenness exata mede intermediação em
    caminhos mínimos. As duas métricas são somadas por comunidade para uso no
    ranking de influência.

Entradas:
    - ``network.graphml``: grafo de citações entre artigos.
    - ``analysis/community/tables/official_louvain_partition.csv``:
      comunidade oficial de cada artigo.

Saídas:
    - ``analysis/community/tables/community_aggregate_centrality.csv`` com:
      ``pagerank_sum``, ``betweenness_sum`` e grau de entrada médio por
      comunidade.

Observação:
    A betweenness é calculada de forma exata, dirigida e normalizada pelo
    NetworkX. Este script é computacionalmente caro em redes grandes.
"""

from collections import Counter

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

    for node, community_id in partition.items():
        node_in_degree = graph.in_degree(node)
        pagerank_sum[community_id] += pagerank[node]
        betweenness_sum[community_id] += betweenness[node]
        in_degree_sum[community_id] += node_in_degree

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
    return community_rows


def main():
    partition = load_official_partition()
    graph = load_citation_graph().subgraph(partition).copy()
    print(f"Grafo: {graph.number_of_nodes():,} nós, {graph.number_of_edges():,} arestas")

    pagerank, betweenness = compute_node_centralities(graph)
    community_rows = aggregate_centralities(
        graph,
        partition,
        pagerank,
        betweenness,
    )

    write_csv(
        TABLES_DIR / "community_aggregate_centrality.csv",
        ["community_id", "pagerank_sum", "betweenness_sum", "mean_in_degree"],
        community_rows,
    )

    print(f"Comunidades analisadas: {len(community_rows)}")
    print(f"Tabelas salvas em {TABLES_DIR}")


if __name__ == "__main__":
    main()

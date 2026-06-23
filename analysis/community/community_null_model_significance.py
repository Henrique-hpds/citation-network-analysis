"""Executa testes de significância por modelos nulos das comunidades.

Objetivo:
    Avaliar se a estrutura comunitária observada é mais forte do que seria
    esperado ao acaso. O script consolida dois testes usados no relatório:
    modularidade contra grafos aleatórios com preservação de grau e densidade
    interna contra embaralhamento dos rótulos de comunidade.

Entradas:
    - ``analysis/reports/louvain_modularity_tests.json``: contém o teste de
      modularidade contra o modelo nulo de preservação de grau.
    - ``network.graphml``: grafo de citações.
    - ``official_louvain_partition.csv``: partição oficial usada nas demais
      análises.

Saídas:
    - ``analysis/community/tables/community_null_model_summary.csv`` com os
      valores observados, médias nulas, desvios, z-scores, p-valores e
      estatísticas de sanidade do embaralhamento.

Metodologia:
    O segundo teste embaralha os rótulos de comunidade preservando os tamanhos
    dos grupos. Assim, compara-se a densidade média observada com a densidade
    esperada caso os artigos fossem distribuídos aleatoriamente em comunidades
    de mesmo tamanho.
"""

import json

import numpy as np

from helper_functions import (
    ROOT,
    TABLES_DIR,
    load_citation_graph,
    load_official_partition,
    write_csv,
)


LOUVAIN_RESULT_PATH = ROOT / "analysis" / "reports" / "louvain_modularity_tests.json"
PARTITION_PERMUTATIONS = 1000


def mean_internal_density(labels, edge_sources, edge_targets, community_count, denominators):
    internal_mask = labels[edge_sources] == labels[edge_targets]
    internal_edges = np.bincount(
        labels[edge_sources[internal_mask]],
        minlength=community_count,
    )
    densities = np.divide(
        internal_edges,
        denominators,
        out=np.zeros(community_count, dtype=float),
        where=denominators > 0,
    )
    return densities.mean()


def partition_shuffle_test(graph, partition, permutations=1000, seed=42):
    nodes = list(partition)
    node_index = {node: index for index, node in enumerate(nodes)}
    community_ids = sorted(set(partition.values()))
    community_index = {
        community_id: index
        for index, community_id in enumerate(community_ids)
    }
    labels = np.array([
        community_index[partition[node]]
        for node in nodes
    ])
    sizes = np.bincount(labels, minlength=len(community_ids))
    denominators = sizes * (sizes - 1)
    edges = [
        (node_index[source], node_index[target])
        for source, target in graph.edges
    ]
    edge_sources = np.fromiter((source for source, _ in edges), dtype=np.int64)
    edge_targets = np.fromiter((target for _, target in edges), dtype=np.int64)

    observed = mean_internal_density(
        labels,
        edge_sources,
        edge_targets,
        len(community_ids),
        denominators,
    )
    rng = np.random.default_rng(seed)
    null_values = []
    changed_counts = []
    for _ in range(permutations):
        shuffled = rng.permutation(labels)
        changed_counts.append(int(np.sum(shuffled != labels)))
        null_values.append(mean_internal_density(
            shuffled,
            edge_sources,
            edge_targets,
            len(community_ids),
            denominators,
        ))

    null_values = np.array(null_values)
    null_mean = null_values.mean()
    null_std = null_values.std(ddof=1)
    p_value = (np.sum(null_values >= observed) + 1) / (permutations + 1)
    z_score = (observed - null_mean) / null_std
    changed_counts = np.array(changed_counts)
    return {
        "observed": observed,
        "null_values": null_values,
        "null_mean": null_mean,
        "null_std": null_std,
        "p_value": p_value,
        "z_score": z_score,
        "changed_nodes_mean": changed_counts.mean(),
        "changed_nodes_min": changed_counts.min(),
        "changed_nodes_max": changed_counts.max(),
        "changed_nodes_fraction_mean": changed_counts.mean() / len(labels),
        "node_count": len(labels),
    }


def main():
    louvain_results = json.loads(LOUVAIN_RESULT_PATH.read_text(encoding="utf-8"))
    modularity_test = louvain_results["significance_test"]

    partition = load_official_partition()
    graph = load_citation_graph().subgraph(partition).copy()
    density_test = partition_shuffle_test(
        graph,
        partition,
        permutations=PARTITION_PERMUTATIONS,
    )

    modularity_z_score = (
        modularity_test["observed_Q"] - modularity_test["null_mean"]
    ) / modularity_test["null_std"]
    summary_rows = [
        {
            "test": "degree_preserving_rewiring",
            "metric": "louvain_modularity",
            "observed": modularity_test["observed_Q"],
            "null_mean": modularity_test["null_mean"],
            "null_std": modularity_test["null_std"],
            "z_score": modularity_z_score,
            "p_value": modularity_test["p_value"],
            "null_samples": modularity_test["n_null"],
            "hypothesis_supported": modularity_test["p_value"] < 0.05,
            "sanity_check": "",
            "sanity_changed_nodes_mean": "",
            "sanity_changed_nodes_min": "",
            "sanity_changed_nodes_max": "",
            "sanity_changed_nodes_fraction_mean": "",
            "sanity_node_count": "",
        },
        {
            "test": "partition_shuffle_preserving_community_sizes",
            "metric": "mean_internal_density",
            "observed": density_test["observed"],
            "null_mean": density_test["null_mean"],
            "null_std": density_test["null_std"],
            "z_score": density_test["z_score"],
            "p_value": density_test["p_value"],
            "null_samples": PARTITION_PERMUTATIONS,
            "hypothesis_supported": density_test["p_value"] < 0.05,
            "sanity_check": "shuffled_partition_changed_node_labels",
            "sanity_changed_nodes_mean": density_test["changed_nodes_mean"],
            "sanity_changed_nodes_min": density_test["changed_nodes_min"],
            "sanity_changed_nodes_max": density_test["changed_nodes_max"],
            "sanity_changed_nodes_fraction_mean": density_test["changed_nodes_fraction_mean"],
            "sanity_node_count": density_test["node_count"],
        },
    ]

    write_csv(
        TABLES_DIR / "community_null_model_summary.csv",
        list(summary_rows[0]),
        summary_rows,
    )

    for row in summary_rows:
        print(
            f"{row['metric']}: observado={row['observed']:.6f}, "
            f"p={row['p_value']:.6f}, z={row['z_score']:.2f}"
        )
    print(f"Tabelas salvas em {TABLES_DIR}")


if __name__ == "__main__":
    main()

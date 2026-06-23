"""Gera a partição oficial de comunidades por Louvain.

Objetivo:
    Executar Louvain em algumas sementes controladas, selecionar a partição
    oficial pela modularidade mediana e salvar o mapeamento artigo-comunidade
    usado por todas as análises posteriores.

Entradas:
    - ``network.graphml``: grafo de citações. O algoritmo é aplicado à maior
      componente fracamente conexa convertida para grafo não direcionado, que
      é a base comparável das análises comunitárias.

Saídas:
    - ``analysis/community/tables/official_louvain_partition.csv`` com
      ``openalex_id`` e ``community_id``.

Observação metodológica:
    O relatório final usa valores de robustez do Louvain calculados em testes
    mais amplos, mas este script preserva a geração da partição oficial usada
    pelas tabelas e figuras da versão 3.
"""

import community as community_louvain
import numpy as np

from helper_functions import TABLES_DIR, largest_weak_component, load_citation_graph, write_csv

SEEDS = (0, 42, 204)


def run_louvain(graph, seed):
    partition = community_louvain.best_partition(graph, random_state=seed)
    modularity = community_louvain.modularity(partition, graph)
    return partition, modularity, len(set(partition.values()))


def main():
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    graph = largest_weak_component(load_citation_graph()).to_undirected()
    print(f"Maior WCC: {graph.number_of_nodes():,} nós, {graph.number_of_edges():,} arestas")

    results = []
    partitions = {}
    for seed in SEEDS:
        partition, modularity, n_communities = run_louvain(graph, seed)
        results.append({
            "seed": seed,
            "modularity": modularity,
            "n_communities": n_communities,
        })
        partitions[seed] = partition
        print(f"Seed {seed}: Q={modularity:.6f}, comunidades={n_communities}")

    official_result = sorted(results, key=lambda row: row["modularity"])[len(results) // 2]
    official_seed = official_result["seed"]
    official_partition = partitions[official_seed]
    for row in results:
        row["is_official"] = row["seed"] == official_seed

    modularities = np.array([row["modularity"] for row in results])
    community_counts = np.array([row["n_communities"] for row in results])
    q_cv = modularities.std(ddof=1) / modularities.mean()
    community_range = int(community_counts.max() - community_counts.min())

    # Hipóteses prévias: CV de Q < 1% e intervalo de até 2 comunidades.
    q_robust = q_cv < 0.01
    community_count_robust = community_range <= 2
    summary = [{
        "n_seeds": len(SEEDS),
        "modularity_mean": modularities.mean(),
        "modularity_variance": modularities.var(ddof=1),
        "modularity_cv": q_cv,
        "n_communities_mean": community_counts.mean(),
        "n_communities_variance": community_counts.var(ddof=1),
        "n_communities_range": community_range,
        "q_cv_threshold": 0.01,
        "q_robust": q_robust,
        "n_communities_range_threshold": 2,
        "n_communities_robust": community_count_robust,
        "official_seed": official_seed,
        "official_modularity": official_result["modularity"],
        "selection_rule": "median_modularity",
    }]

    write_csv(
        TABLES_DIR / "official_louvain_partition.csv",
        ["openalex_id", "community_id"],
        (
            {"openalex_id": node, "community_id": community_id}
            for node, community_id in sorted(official_partition.items())
        ),
    )

    print(f"Hipótese de estabilidade de Q confirmada: {q_robust}")
    print(f"Hipótese de estabilidade do número de comunidades confirmada: {community_count_robust}")
    print(f"Partição oficial: seed {official_seed} (Q mediana={official_result['modularity']:.6f})")
    print(f"Tabelas salvas em {TABLES_DIR}")


if __name__ == "__main__":
    main()

"""Calcula o fluxo externo de citações entre comunidades.

Objetivo:
    Quantificar como as comunidades se conectam por citações que cruzam
    fronteiras comunitárias. Essas métricas sustentam a discussão sobre
    influência externa, alcance e relações intercomunidades.

Entradas:
    - ``network.graphml``: grafo dirigido de citações.
    - ``analysis/community/tables/official_louvain_partition.csv``:
      comunidade oficial de cada artigo.

Saídas:
    - ``analysis/community/tables/community_external_impact.csv`` contendo,
      para cada comunidade:
        * ``external_in``: citações recebidas de outras comunidades;
        * ``external_out``: citações feitas para outras comunidades;
        * ``communities_that_impact``: número de comunidades citantes;
        * ``communities_impacted``: número de comunidades citadas.

Interpretação:
    ``external_in`` mede reconhecimento externo recebido. Já
    ``communities_impacted`` é usado como alcance, pois conta quantas
    comunidades diferentes são citadas pela comunidade de origem.
"""

from collections import Counter, defaultdict

from helper_functions import (
    TABLES_DIR,
    load_citation_graph,
    load_official_partition,
    write_csv,
)


def compute_external_impact(graph, partition):
    community_ids = set(partition.values())
    external_in = Counter()
    external_out = Counter()
    source_communities = defaultdict(set)
    target_communities = defaultdict(set)

    for source, target in graph.edges:
        source_community = partition.get(source)
        target_community = partition.get(target)
        if (
            source_community is None
            or target_community is None
            or source_community == target_community
        ):
            continue

        external_out[source_community] += 1
        external_in[target_community] += 1
        source_communities[target_community].add(source_community)
        target_communities[source_community].add(target_community)

    rows = [
        {
            "community_id": community_id,
            "external_in": external_in[community_id],
            "external_out": external_out[community_id],
            "communities_that_impact": len(source_communities[community_id]),
            "communities_impacted": len(target_communities[community_id]),
        }
        for community_id in community_ids
    ]
    return sorted(rows, key=lambda row: (-row["external_in"], row["community_id"]))


def main():
    graph = load_citation_graph()
    partition = load_official_partition()
    rows = compute_external_impact(graph, partition)

    write_csv(
        TABLES_DIR / "community_external_impact.csv",
        [
            "community_id",
            "external_in",
            "external_out",
            "communities_that_impact",
            "communities_impacted",
        ],
        rows,
    )

    print(f"Comunidades analisadas: {len(rows)}")
    print(f"Citações externas: {sum(row['external_in'] for row in rows):,}")
    print(f"Tabelas salvas em {TABLES_DIR}")


if __name__ == "__main__":
    main()

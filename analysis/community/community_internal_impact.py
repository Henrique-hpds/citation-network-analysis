"""Calcula coesão interna e densidade das comunidades.

Objetivo:
    Medir o grau de fechamento interno de cada comunidade por meio do número
    de citações internas e da densidade dirigida. Também calcula uma densidade
    interna voltada à Unicamp, considerando citações internas cujo destino é
    um artigo classificado como ``uni``.

Entradas:
    - ``network.graphml``: grafo dirigido de citações.
    - ``official_louvain_partition.csv``: comunidade oficial de cada artigo.
    - ``article_set_assignments.csv``: classificação dos artigos em
      ``inter``, ``top tier``, ``uni`` ou ``inst``.

Saídas:
    - ``analysis/community/tables/community_internal_impact.csv`` com tamanho
      da comunidade, número de artigos Unicamp, citações internas, citações
      internas para artigos Unicamp, densidade interna e densidade interna da
      Unicamp.
    - ``analysis/community/tables/community_internal_impact_summary.csv`` com
      totais e estatísticas usadas no texto da seção.

Definição:
    A densidade interna é ``m_C / (|C|(|C|-1))``. A densidade interna da
    Unicamp usa como numerador apenas citações internas que têm artigo
    Unicamp como destino.
"""

from collections import Counter
from statistics import mean, median

from helper_functions import (
    TABLES_DIR,
    community_label,
    count_unicamp_articles_by_community,
    load_article_set_assignments,
    load_citation_graph,
    load_official_partition,
    write_csv,
)


def compute_internal_impact(graph, partition, article_sets):
    sizes = Counter(partition.values())
    unicamp_counts = count_unicamp_articles_by_community(partition, article_sets)
    internal_edges = Counter()
    internal_edges_to_unicamp = Counter()

    for source, target in graph.edges:
        source_community = partition.get(source)
        target_community = partition.get(target)
        if (
            source != target
            and source_community is not None
            and source_community == target_community
        ):
            internal_edges[source_community] += 1
            if article_sets.get(target) == "uni":
                internal_edges_to_unicamp[source_community] += 1

    rows = []
    for community_id, size in sizes.items():
        denominator = size * (size - 1)
        unicamp_count = unicamp_counts[community_id]
        unicamp_denominator = unicamp_count * (size - 1)
        unicamp_internal_edges = internal_edges_to_unicamp[community_id]
        rows.append({
            "community_id": community_id,
            "community_name": community_label(community_id),
            "size": size,
            "unicamp_article_count": unicamp_count,
            "unicamp_article_percent": 100 * unicamp_count / size,
            "internal_edges": internal_edges[community_id],
            "internal_edges_to_unicamp": unicamp_internal_edges,
            "density": internal_edges[community_id] / denominator if denominator else 0.0,
            "unicamp_internal_density": (
                unicamp_internal_edges / unicamp_denominator
                if unicamp_denominator else 0.0
            ),
        })

    return sorted(rows, key=lambda row: row["density"], reverse=True)


def evaluate_hypothesis(rows):
    quartile_size = max(1, len(rows) // 4)
    high_density_size_median = median(row["size"] for row in rows[:quartile_size])
    low_density_size_median = median(row["size"] for row in rows[-quartile_size:])

    return [{
        "hypothesis": "higher_density_communities_are_smaller",
        "quartile_size": quartile_size,
        "high_density_size_median": high_density_size_median,
        "low_density_size_median": low_density_size_median,
        "hypothesis_supported": high_density_size_median < low_density_size_median,
        "mean_density": mean(row["density"] for row in rows),
        "total_internal_edges": sum(row["internal_edges"] for row in rows),
    }]


def main():
    graph = load_citation_graph()
    partition = load_official_partition()
    article_sets = load_article_set_assignments()
    rows = compute_internal_impact(graph, partition, article_sets)
    hypothesis = evaluate_hypothesis(rows)

    write_csv(
        TABLES_DIR / "community_internal_impact.csv",
        [
            "community_id",
            "community_name",
            "size",
            "unicamp_article_count",
            "unicamp_article_percent",
            "internal_edges",
            "internal_edges_to_unicamp",
            "density",
            "unicamp_internal_density",
        ],
        rows,
    )
    write_csv(
        TABLES_DIR / "community_internal_impact_summary.csv",
        list(hypothesis[0]),
        hypothesis,
    )

    print(f"Comunidades analisadas: {len(rows)}")
    print(f"Arestas internas: {hypothesis[0]['total_internal_edges']:,}")
    print(f"Hipótese confirmada: {hypothesis[0]['hypothesis_supported']}")
    print(f"Tabelas salvas em {TABLES_DIR}")


if __name__ == "__main__":
    main()

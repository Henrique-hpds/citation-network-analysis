"""Calcula o ranking composto de influência das comunidades.

Objetivo:
    Combinar três dimensões de influência em um único escore: citações
    externas recebidas, alcance externo e PageRank agregado. Esse ranking é
    usado para identificar as comunidades estruturalmente mais importantes do
    grafo e para ordenar a tabela final da análise da Unicamp.

Entradas:
    - ``community_external_impact.csv``: fornece ``external_in`` e
      ``communities_impacted``.
    - ``community_aggregate_centrality.csv``: fornece ``pagerank_sum`` e
      ``betweenness_sum``.

Saídas:
    - ``analysis/community/tables/community_influence_scores.csv`` com o
      escore final, ranking e componentes normalizados por min-max.
    - ``analysis/community/tables/community_influence_sensitivity.csv`` com
      a verificação de robustez que substitui parte do peso por betweenness.

Definição:
    O escore principal usa pesos iguais para ``external_in``,
    ``communities_impacted`` e ``pagerank_sum``. A sensibilidade compara esse
    ranking a uma alternativa que inclui betweenness agregada.
"""

import csv
from statistics import correlation, mean

from helper_functions import TABLES_DIR, minmax_normalize, write_csv


def load_rows(filename):
    with (TABLES_DIR / filename).open(encoding="utf-8") as file:
        return {
            int(row["community_id"]): row
            for row in csv.DictReader(file)
        }


def rank_scores(scores):
    ordered = sorted(scores, key=lambda community_id: (-scores[community_id], community_id))
    return {
        community_id: rank
        for rank, community_id in enumerate(ordered, 1)
    }


def main():
    external_rows = load_rows("community_external_impact.csv")
    centrality_rows = load_rows("community_aggregate_centrality.csv")
    community_ids = sorted(external_rows)

    external_in = {
        community_id: float(external_rows[community_id]["external_in"])
        for community_id in community_ids
    }
    communities_impacted = {
        community_id: float(external_rows[community_id]["communities_impacted"])
        for community_id in community_ids
    }
    pagerank_sum = {
        community_id: float(centrality_rows[community_id]["pagerank_sum"])
        for community_id in community_ids
    }
    betweenness_sum = {
        community_id: float(centrality_rows[community_id]["betweenness_sum"])
        for community_id in community_ids
    }

    external_norm = minmax_normalize(external_in)
    impacted_norm = minmax_normalize(communities_impacted)
    pagerank_norm = minmax_normalize(pagerank_sum)
    betweenness_norm = minmax_normalize(betweenness_sum)

    baseline_scores = {
        community_id: (
            external_norm[community_id]
            + impacted_norm[community_id]
            + pagerank_norm[community_id]
        ) / 3
        for community_id in community_ids
    }
    alternative_scores = {
        community_id: (
            0.2 * external_norm[community_id]
            + 0.2 * impacted_norm[community_id]
            + 0.2 * pagerank_norm[community_id]
            + 0.4 * betweenness_norm[community_id]
        )
        for community_id in community_ids
    }
    baseline_ranks = rank_scores(baseline_scores)
    alternative_ranks = rank_scores(alternative_scores)

    spearman = correlation(
        [baseline_ranks[community_id] for community_id in community_ids],
        [alternative_ranks[community_id] for community_id in community_ids],
    )
    top_10_baseline = {
        community_id for community_id, rank in baseline_ranks.items() if rank <= 10
    }
    top_10_alternative = {
        community_id for community_id, rank in alternative_ranks.items() if rank <= 10
    }
    top_10_overlap = len(top_10_baseline & top_10_alternative)
    rank_shifts = {
        community_id: abs(
            baseline_ranks[community_id] - alternative_ranks[community_id]
        )
        for community_id in community_ids
    }
    hypothesis_supported = spearman > 0.8 and top_10_overlap >= 8

    rows = [
        {
            "community_id": community_id,
            "external_in_normalized": external_norm[community_id],
            "communities_impacted_normalized": impacted_norm[community_id],
            "pagerank_sum_normalized": pagerank_norm[community_id],
            "betweenness_sum_normalized": betweenness_norm[community_id],
            "baseline_score": baseline_scores[community_id],
            "baseline_rank": baseline_ranks[community_id],
            "betweenness_weighted_score": alternative_scores[community_id],
            "betweenness_weighted_rank": alternative_ranks[community_id],
            "absolute_rank_shift": rank_shifts[community_id],
            "final_influence_score": baseline_scores[community_id],
            "final_rank": baseline_ranks[community_id],
        }
        for community_id in community_ids
    ]
    rows.sort(key=lambda row: row["final_rank"])

    sensitivity = [{
        "comparison": "equal_weights_vs_betweenness_weighted",
        "spearman_correlation": spearman,
        "top_10_overlap": top_10_overlap,
        "mean_absolute_rank_shift": mean(rank_shifts.values()),
        "max_absolute_rank_shift": max(rank_shifts.values()),
        "hypothesis_spearman_threshold": 0.8,
        "hypothesis_top_10_overlap_threshold": 8,
        "hypothesis_supported": hypothesis_supported,
    }]
    write_csv(
        TABLES_DIR / "community_influence_scores.csv",
        list(rows[0]),
        rows,
    )
    write_csv(
        TABLES_DIR / "community_influence_sensitivity.csv",
        list(sensitivity[0]),
        sensitivity,
    )

    print(f"Spearman: {spearman:.4f}")
    print(f"Sobreposição top-10: {top_10_overlap}/10")
    print(f"Hipótese confirmada: {hypothesis_supported}")
    print(f"Comunidade líder: {rows[0]['community_id']}")
    print(f"Tabelas salvas em {TABLES_DIR}")


if __name__ == "__main__":
    main()

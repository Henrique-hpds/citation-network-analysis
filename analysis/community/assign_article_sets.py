"""Classifica os artigos do grafo em conjuntos analíticos.

Objetivo:
    Atribuir a cada artigo exatamente um valor de ``article_set`` entre
    ``inter``, ``top tier``, ``uni`` e ``inst``. Essa classificação é usada
    pelas análises de presença da Unicamp e pelas métricas internas das
    comunidades.

Entradas:
    - ``network.graphml``: grafo completo de citações, usado para obter os
      OpenAlex IDs dos artigos existentes no banco.
    - ``data/responses_1/_top_cited_cs``: artigos do conjunto top-tier.
    - ``data/responses_1/_unicamp_cs``: artigos da Unicamp.
    - diretórios institucionais listados em ``helper_functions.py``:
      artigos associados às instituições de referência.

Saídas:
    - ``analysis/community/tables/article_set_assignments.csv`` com a
      classificação final por artigo.
    - atributo ``article_set`` persistido nos nós ``Article`` do Neo4j.

Regra:
    Em caso de sobreposição, aplica-se a precedência
    ``uni > top tier > inst > inter``.
"""

from helper_functions import (
    TABLES_DIR,
    count_matching_article_sets,
    load_article_set_ids,
    load_citation_graph,
    persist_article_sets,
    write_csv,
)


def classify_articles(article_ids, institution_ids, top_tier_ids, unicamp_ids):
    assignments = {}
    for article_id in article_ids:
        if article_id in unicamp_ids:
            assignments[article_id] = "uni"
        elif article_id in top_tier_ids:
            assignments[article_id] = "top tier"
        elif article_id in institution_ids:
            assignments[article_id] = "inst"
        else:
            assignments[article_id] = "inter"
    return assignments


def main():
    graph = load_citation_graph()
    institution_ids, top_tier_ids, unicamp_ids = load_article_set_ids()
    assignments = classify_articles(
        graph.nodes,
        institution_ids,
        top_tier_ids,
        unicamp_ids,
    )

    write_csv(
        TABLES_DIR / "article_set_assignments.csv",
        ["openalex_id", "article_set"],
        (
            {"openalex_id": article_id, "article_set": assignments[article_id]}
            for article_id in sorted(assignments)
        ),
    )

    updated = persist_article_sets(assignments)
    matched = count_matching_article_sets(assignments)
    if updated != len(assignments) or matched != len(assignments):
        raise RuntimeError(
            f"Classificação incompleta: {updated} atualizados e {matched} validados "
            f"de {len(assignments)} artigos"
        )

    print(f"Artigos atualizados e validados: {matched:,}")


if __name__ == "__main__":
    main()

"""Identifica o subcampo dominante de cada comunidade.

Objetivo:
    Atribuir uma caracterização semântica simples às comunidades detectadas,
    contando os subfields OpenAlex associados aos artigos de cada comunidade.
    O resultado é usado para tornar a tabela final interpretável, substituindo
    uma leitura puramente numérica dos IDs de comunidade.

Entradas:
    - ``network.graphml``: contém as relações ``HAS_SUBFIELD`` entre artigos e
      subfields.
    - ``official_louvain_partition.csv``: comunidade oficial de cada artigo.

Saídas:
    - ``analysis/community/tables/community_theme_summary.csv`` com, para cada
      comunidade, o subcampo dominante, número de subcampos distintos, total
      de atribuições temáticas e fração de artigos no subcampo dominante.

Observação:
    Um artigo pode ter mais de um subfield; por isso o total de atribuições
    temáticas pode ser maior do que o número de artigos da comunidade.
"""

from collections import Counter, defaultdict

from helper_functions import (
    TABLES_DIR,
    load_article_subfields,
    load_official_partition,
    write_csv,
)


def main():
    partition = load_official_partition()
    article_subfields = load_article_subfields()

    community_articles = defaultdict(list)
    community_theme_counts = defaultdict(Counter)
    for article_id, community_id in partition.items():
        community_articles[community_id].append(article_id)
        for subfield in article_subfields.get(article_id, []):
            community_theme_counts[community_id][subfield] += 1

    summary_rows = []
    for community_id in sorted(community_articles):
        article_count = len(community_articles[community_id])
        theme_counts = community_theme_counts[community_id]
        total_theme_assignments = sum(theme_counts.values())
        dominant_theme, dominant_theme_count = max(
            theme_counts.items(),
            key=lambda item: (item[1], item[0]),
            default=("", 0),
        )

        summary_rows.append({
            "community_id": community_id,
            "community_article_count": article_count,
            "distinct_theme_count": len(theme_counts),
            "total_theme_assignments": total_theme_assignments,
            "mean_themes_per_article": total_theme_assignments / article_count,
            "dominant_theme": dominant_theme,
            "dominant_theme_article_count": dominant_theme_count,
            "dominant_theme_article_fraction": (
                dominant_theme_count / article_count
                if article_count
                else 0
            ),
        })

    write_csv(
        TABLES_DIR / "community_theme_summary.csv",
        list(summary_rows[0]),
        summary_rows,
    )

    print(f"Comunidades analisadas: {len(summary_rows)}")
    print(f"Tabelas salvas em {TABLES_DIR}")


if __name__ == "__main__":
    main()

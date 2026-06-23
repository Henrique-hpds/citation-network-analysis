"""Calcula a presença da Unicamp em cada comunidade.

Objetivo:
    Quantificar, para cada comunidade da partição oficial, quantos artigos são
    da Unicamp e qual fração da comunidade eles representam. Esses valores
    alimentam a discussão sobre concentração da produção da Unicamp em
    comunidades centrais ou periféricas.

Entradas:
    - ``official_louvain_partition.csv``: comunidade de cada artigo.
    - ``article_set_assignments.csv``: identifica os artigos com
      ``article_set = "uni"``.

Saídas:
    - ``analysis/community/tables/community_unicamp_presence.csv`` com tamanho da
      comunidade, contagem de artigos Unicamp e fração Unicamp por comunidade.
"""

from collections import Counter

from helper_functions import (
    TABLES_DIR,
    count_unicamp_articles_by_community,
    load_article_set_assignments,
    load_official_partition,
    write_csv,
)


def main():
    partition = load_official_partition()
    article_sets = load_article_set_assignments()
    sizes = Counter(partition.values())
    unicamp_counts = count_unicamp_articles_by_community(partition, article_sets)

    rows = [
        {
            "community_id": community_id,
            "size": size,
            "unicamp_count": unicamp_counts[community_id],
            "unicamp_fraction": unicamp_counts[community_id] / size,
        }
        for community_id, size in sizes.items()
    ]
    fields = list(rows[0])
    write_csv(
        TABLES_DIR / "community_unicamp_presence.csv",
        fields,
        sorted(rows, key=lambda row: row["community_id"]),
    )

    print(f"Artigos IC/Unicamp na partição: {sum(unicamp_counts.values()):,}")
    print(f"Tabelas salvas em {TABLES_DIR}")


if __name__ == "__main__":
    main()

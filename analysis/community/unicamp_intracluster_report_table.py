"""Gera a tabela final de posição intra-cluster da Unicamp.

Objetivo:
    Identificar, em cada comunidade, o artigo da Unicamp com melhor posição
    intra-cluster e produzir a tabela LaTeX usada no relatório final. A seleção
    do artigo combina PageRank local e coreness local normalizados por min-max
    apenas entre artigos da Unicamp daquela comunidade.

Entradas:
    - ``network.graphml``: grafo de citações e metadados dos artigos.
    - ``official_louvain_partition.csv``: comunidade oficial de cada artigo.
    - ``article_set_assignments.csv``: identifica artigos Unicamp
      (``article_set = "uni"``).
    - ``community_influence_scores.csv``: ranking e escore de influência das
      comunidades.
    - ``community_ic_presence.csv``: fração de artigos Unicamp por comunidade.
    - ``community_theme_summary.csv``: subcampo dominante de cada comunidade.

Saídas:
    - ``analysis/community/tables/unicamp_top_article_by_community.csv``:
      uma linha por comunidade com o artigo Unicamp selecionado e suas métricas.
    - ``analysis/community/reports/unicamp_intracluster_position_table.tex``:
      tabela LaTeX no formato usado por ``analises_comunidade_3.tex``.

Métricas:
    Para cada comunidade, o PageRank e o k-core são calculados no subgrafo
    induzido pela própria comunidade. A tabela reporta ``PR_rel`` e
    ``Core_rel`` em relação à média de todos os artigos da comunidade.
"""

import csv
from statistics import mean

from helper_functions import (
    REPORTS_DIR,
    TABLES_DIR,
    community_code,
    community_label,
    load_article_set_assignments,
    load_article_subfields,
    load_citation_graph,
    load_official_partition,
    local_core_numbers,
    pagerank_power_iteration,
    safe_ratio,
    write_csv,
)


def read_csv_rows(path):
    with path.open(encoding="utf-8") as file:
        return [
            {
                key.strip(): (value or "").strip()
                for key, value in row.items()
                if key is not None
            }
            for row in csv.DictReader(file)
        ]


def read_by_community(path):
    return {
        int(row["community_id"]): row
        for row in read_csv_rows(path)
    }


def latex_escape(value):
    text = str(value)
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(replacements.get(character, character) for character in text)


def format_float(value, digits=3):
    return f"{float(value):.{digits}f}".replace(".", ",")


def format_percent(value, digits=1):
    return f"{100 * float(value):.{digits}f}%".replace(".", ",")


def compute_article_positions(graph, partition, article_sets, article_subfields):
    rows = []
    for community_id in sorted(set(partition.values())):
        nodes = sorted(
            article_id
            for article_id, article_community in partition.items()
            if article_community == community_id
        )
        local_graph = graph.subgraph(nodes).copy()
        pagerank = pagerank_power_iteration(
            local_graph,
            tolerance=1e-10,
            max_iterations=500,
        )
        core_numbers = local_core_numbers(local_graph)
        pagerank_mean = mean(pagerank.values())
        coreness_mean = mean(core_numbers.values())
        unicamp_nodes = [
            node
            for node in nodes
            if article_sets.get(node) == "uni"
        ]
        pagerank_min = min(pagerank[node] for node in unicamp_nodes)
        pagerank_max = max(pagerank[node] for node in unicamp_nodes)
        coreness_min = min(core_numbers[node] for node in unicamp_nodes)
        coreness_max = max(core_numbers[node] for node in unicamp_nodes)

        for node in unicamp_nodes:
            data = graph.nodes[node]
            pagerank_minmax = (
                (pagerank[node] - pagerank_min) / (pagerank_max - pagerank_min)
                if pagerank_max != pagerank_min
                else 0.0
            )
            coreness_minmax = (
                (core_numbers[node] - coreness_min) / (coreness_max - coreness_min)
                if coreness_max != coreness_min
                else 0.0
            )
            rows.append({
                "community_id": community_id,
                "openalex_id": node,
                "title": data.get("title", ""),
                "publication_year": data.get("publication_year", ""),
                "cited_by_count": data.get("cited_by_count", ""),
                "internal_in_degree": local_graph.in_degree(node),
                "internal_out_degree": local_graph.out_degree(node),
                "pagerank_local": pagerank[node],
                "pagerank_relative": safe_ratio(pagerank[node], pagerank_mean),
                "coreness": core_numbers[node],
                "coreness_relative": safe_ratio(core_numbers[node], coreness_mean),
                "pagerank_minmax": pagerank_minmax,
                "coreness_minmax": coreness_minmax,
                "minmax_sum": pagerank_minmax + coreness_minmax,
                "article_subfields": "; ".join(article_subfields.get(node, [])),
            })
    return rows


def top_article_by_community(article_rows):
    top_rows = {}
    for row in article_rows:
        community_id = row["community_id"]
        current = top_rows.get(community_id)
        key = (
            row["minmax_sum"],
            row["pagerank_relative"],
            row["coreness_relative"],
            int(row["cited_by_count"] or 0),
            row["openalex_id"],
        )
        if current is None or key > current[0]:
            top_rows[community_id] = (key, row)
    return {
        community_id: row
        for community_id, (_, row) in top_rows.items()
    }


def dominant_subfield(row):
    return row.get("dominant_theme", "") or row.get("community_dominant_subfield", "")


def count_external_in_by_community(graph, partition):
    counts = {community_id: 0 for community_id in set(partition.values())}
    for source, target in graph.edges():
        source_community = partition.get(source)
        target_community = partition.get(target)
        if (
            source_community is not None
            and target_community is not None
            and source_community != target_community
        ):
            counts[target_community] += 1
    return counts


def build_community_rows(top_articles, external_in_by_community):
    influence_rows = read_by_community(TABLES_DIR / "community_influence_scores.csv")
    presence_rows = read_by_community(TABLES_DIR / "community_ic_presence.csv")
    theme_rows = read_by_community(TABLES_DIR / "community_theme_summary.csv")

    rows = []
    for community_id, influence in sorted(
        influence_rows.items(),
        key=lambda item: int(item[1]["final_rank"]),
    ):
        article = top_articles[community_id]
        article_subfields = article["article_subfields"].split("; ")
        rows.append({
            "rank": influence["final_rank"],
            "community_id": community_id,
            "community": community_code(community_id),
            "community_name": community_label(community_id),
            "main_subfield": dominant_subfield(theme_rows[community_id]),
            "size": presence_rows[community_id]["size"],
            "external_in": external_in_by_community[community_id],
            "influence": influence["final_influence_score"],
            "unicamp_fraction": presence_rows[community_id]["ic_fraction"],
            "top_article_id": article["openalex_id"],
            "top_article_title": article["title"],
            "publication_year": article["publication_year"],
            "article_subfields": article["article_subfields"],
            "pagerank_relative": article["pagerank_relative"],
            "coreness_relative": article["coreness_relative"],
            "internal_in_degree": article["internal_in_degree"],
            "internal_out_degree": article["internal_out_degree"],
            "minmax_sum": article["minmax_sum"],
        })
    return rows


def write_table_tex(rows):
    output_path = REPORTS_DIR / "unicamp_intracluster_position_table.tex"
    lines = [
        r"\clearpage",
        r"\onecolumn",
        "",
        r"\begin{table}[p]",
        r"\centering",
        r"\scriptsize",
        r"\setlength{\tabcolsep}{2pt}",
        r"\renewcommand{\arraystretch}{1.04}",
        "",
        r"\caption{Comunidades ordenadas por influência e artigo Unicamp de maior posição intra-cluster.}",
        r"\label{tab:unicamp_posicao_intracluster}",
        "",
        r"\begin{tabularx}{\textwidth}{",
        r"@{}",
        r"C{0.55cm} % Com.",
        r"L         % Subcampo",
        r"R{0.65cm} % Tam.",
        r"R{0.75cm} % Cit. ext.",
        r"R{0.65cm} % Infl.",
        r"R{0.65cm} % \% IC",
        r"L         % Artigo IC",
        r"R{0.55cm} % Ano",
        r"L         % Subcampos do artigo",
        r"R{0.65cm} % PR$_{rel}$",
        r"R{0.65cm} % Core$_{rel}$",
        r"@{}",
        r"}",
        r"\toprule",
        (
            r"Com. &"
            "\n"
            r"Subcampo &"
            "\n"
            r"Tam. &"
            "\n"
            r"Cit. ext. &"
            "\n"
            r"Infl. &"
            "\n"
            r"\% IC &"
            "\n"
            r"Artigo IC &"
            "\n"
            r"Ano &"
            "\n"
            r"Subcampos do artigo &"
            "\n"
            r"PR$_{rel}$ &"
            "\n"
            r"Core$_{rel}$ \\"
        ),
        r"\midrule",
        "",
    ]
    for row in rows:
        values = [
            row["community"],
            row["main_subfield"],
            row["size"],
            row["external_in"],
            format_float(row["influence"], 3),
            format_percent(row["unicamp_fraction"], 1),
            row["top_article_title"],
            row["publication_year"],
            row["article_subfields"],
            format_float(row["pagerank_relative"], 2),
            format_float(row["coreness_relative"], 2),
        ]
        lines.append(" & ".join(latex_escape(value) for value in values) + r" \\")
    lines.extend([
        r"\bottomrule",
        r"\end{tabularx}",
        r"\end{table}",
        "",
        r"\twocolumn",
        r"\clearpage",
        "",
    ])
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


def main():
    graph = load_citation_graph()
    partition = load_official_partition()
    article_sets = load_article_set_assignments()
    article_subfields = load_article_subfields()
    article_rows = compute_article_positions(
        graph,
        partition,
        article_sets,
        article_subfields,
    )
    top_articles = top_article_by_community(article_rows)
    external_in_by_community = count_external_in_by_community(graph, partition)
    community_rows = build_community_rows(top_articles, external_in_by_community)

    write_csv(
        TABLES_DIR / "unicamp_top_article_by_community.csv",
        list(community_rows[0]),
        community_rows,
    )
    table_path = write_table_tex(community_rows)

    print(f"Salvo: {TABLES_DIR / 'unicamp_top_article_by_community.csv'}")
    print(f"Salvo: {table_path}")


if __name__ == "__main__":
    main()

"""Funções e constantes compartilhadas pelas análises de comunidade.

Objetivo:
    Centralizar caminhos, carregamento de dados e funções matemáticas comuns
    aos scripts em ``analysis/community``. Este arquivo não deve gerar outputs
    diretamente; ele apenas fornece utilitários para os demais programas.

Entradas acessadas pelas funções:
    - ``network.graphml``: grafo principal, com nós de artigo e subfield.
    - ``analysis/community/tables/official_louvain_partition.csv``:
      partição oficial de comunidades.
    - ``analysis/community/tables/article_set_assignments.csv``:
      classificação ``article_set`` dos artigos.
    - diretórios em ``data/responses_1`` usados para construir os conjuntos
      ``inst``, ``top tier`` e ``uni``.

Saídas:
    - Nenhuma saída própria. A função ``write_csv`` é usada por outros scripts
      para escrever tabelas padronizadas em ``analysis/community/tables``.

Conteúdo:
    Inclui rótulos semânticos de comunidades, carregadores do grafo e da
    partição, PageRank por iteração de potência, k-core local, normalização
    min-max, persistência do ``article_set`` no Neo4j e escrita CSV com
    inclusão automática de ``community_name`` quando há ``community_id``.
"""

import csv
import os
from collections import Counter, defaultdict
from pathlib import Path

import networkx as nx
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[2]
PHASE_DIR = Path(__file__).resolve().parent
TABLES_DIR = PHASE_DIR / "tables"
FIGURES_DIR = PHASE_DIR / "figures"
REPORTS_DIR = PHASE_DIR / "reports"
GRAPH_PATH = ROOT / "network.graphml"
OFFICIAL_PARTITION_PATH = TABLES_DIR / "official_louvain_partition.csv"
RESPONSES_DIR = ROOT / "data" / "responses_1"
NOTEBOOK_INSTITUTION_DIRS = [
    RESPONSES_DIR / "by_institution" / "universidade_federal_rio_janei",
    RESPONSES_DIR / "by_institution" / "massachusetts_institute_techno",
    RESPONSES_DIR / "by_institution" / "universidade_federal_sao_carlo",
    RESPONSES_DIR / "by_institution" / "carnegie_mellon_university",
    RESPONSES_DIR / "by_institution" / "universidade_sao_paulo",
    RESPONSES_DIR / "by_institution" / "nanjing_university",
    RESPONSES_DIR / "by_institution" / "university_california_berkeley",
    RESPONSES_DIR / "by_institution" / "eindhoven_university_technolog",
    RESPONSES_DIR / "by_institution" / "national_university_singapore",
    RESPONSES_DIR / "by_institution" / "federal_university_minas_gerai",
    RESPONSES_DIR / "by_institution" / "harvard_university",
    RESPONSES_DIR / "by_institution" / "peking_university",
    RESPONSES_DIR / "by_institution" / "university_melbourne",
    RESPONSES_DIR / "by_institution" / "humboldt-universitat_berlin",
    RESPONSES_DIR / "by_institution" / "university_oxford",
    RESPONSES_DIR / "by_institution" / "university_pennsylvania",
    RESPONSES_DIR / "by_institution" / "stanford_university",
    RESPONSES_DIR / "by_institution" / "university_tokyo",
    RESPONSES_DIR / "by_institution" / "instituto_tecnologico_aeronaut",
    RESPONSES_DIR / "by_institution" / "technical_university_darmstadt",
    RESPONSES_DIR / "by_institution" / "institut_polytechnique_paris",
    RESPONSES_DIR / "by_institution" / "technical_university_munich",
    RESPONSES_DIR / "by_institution" / "vellore_institute_technology_v",
    RESPONSES_DIR / "by_institution" / "wuhan_university",
    RESPONSES_DIR / "by_institution" / "king_s_college_london",
    RESPONSES_DIR / "by_institution" / "tsinghua_university",
    RESPONSES_DIR / "by_institution" / "yale_university",
    RESPONSES_DIR / "by_institution" / "kth_royal_institute_technology",
    RESPONSES_DIR / "by_institution" / "unesp",
    RESPONSES_DIR / "by_institution" / "kyoto_university",
]

COMMUNITY_LABELS = {
    0: "Visão computacional, sinais e mídia",
    1: "Visão computacional e IA central",
    2: "Teoria computacional e biologia molecular",
    3: "Sistemas de informação e aplicações em saúde",
    4: "Sistemas de informação e software",
    5: "Inteligência artificial e teoria computacional",
    6: "Redes, comunicações e sistemas",
    7: "Processamento de sinais e mecânica computacional",
    8: "IA aplicada, mídia e ambiente",
    9: "Visão computacional e computação gráfica",
    10: "IA, visão computacional e biologia molecular",
    11: "Visão computacional, IA e sinais",
    12: "IA e física atômica/molecular",
    13: "Redes, sistemas e hardware",
    14: "IA, redes e sistemas de informação",
    15: "Teoria computacional e análise numérica",
    16: "Redes, IA e engenharia elétrica",
    17: "Redes, hardware e sinais",
    18: "Sinais, visão computacional e sistemas",
    19: "Redes, IA e materiais",
    20: "Visão computacional e mecânica",
    21: "IA, sinais e redes",
    22: "IA e teoria computacional",
    23: "Visão computacional, mídia e biofísica",
    24: "Visão computacional, IA e sinais aplicados",
    25: "Visão computacional e sinais",
    26: "Visão computacional e mídia",
    27: "Visão computacional, computação gráfica e controle",
    28: "Teoria computacional, redes e matemática discreta",
    29: "Redes, física não linear e engenharia biomédica",
    30: "Teoria computacional e mecânica computacional",
    31: "IA, estatística e sinais",
    32: "IA, geofísica e imagens radiológicas",
    33: "Interação humano-computador e aplicações",
    34: "Redes, teoria computacional e gestão de sistemas",
    35: "Teoria computacional e análise numérica aplicada",
    36: "IA e imagens biomédicas",
    37: "IA, engenharia elétrica e redes",
    38: "Teoria computacional, análise numérica e geometria",
    39: "Visão computacional, ciências planetárias e geologia",
    40: "IA, sustentabilidade e engenharia elétrica",
}


def community_label(community_id):
    return COMMUNITY_LABELS.get(
        int(community_id),
        f"Comunidade temática {community_id}",
    )


def community_code(community_id):
    return f"C{int(community_id)}"


def load_citation_graph():
    raw_graph = nx.read_graphml(GRAPH_PATH)
    article_ids = {
        node: data["openalex_id"]
        for node, data in raw_graph.nodes(data=True)
        if data.get("labels") == ":Article"
    }

    graph = nx.DiGraph()
    graph.add_nodes_from(
        (article_ids[node], data)
        for node, data in raw_graph.nodes(data=True)
        if node in article_ids
    )
    graph.add_edges_from(
        (article_ids[source], article_ids[target])
        for source, target, data in raw_graph.edges(data=True)
        if data.get("label") == "CITES"
    )
    return graph


def load_article_subfields():
    raw_graph = nx.read_graphml(GRAPH_PATH)
    article_ids = {
        node: data["openalex_id"]
        for node, data in raw_graph.nodes(data=True)
        if data.get("labels") == ":Article"
    }
    subfield_names = {
        node: data["display_name"]
        for node, data in raw_graph.nodes(data=True)
        if data.get("labels") == ":Subfield"
    }
    article_subfields = defaultdict(set)
    for source, target, data in raw_graph.edges(data=True):
        if data.get("label") == "HAS_SUBFIELD":
            article_subfields[article_ids[source]].add(subfield_names[target])
    return {
        article_id: sorted(subfields)
        for article_id, subfields in article_subfields.items()
    }


def load_article_set_ids():
    institution_ids = set()
    for directory in NOTEBOOK_INSTITUTION_DIRS:
        if not directory.is_dir():
            raise FileNotFoundError(directory)
        institution_ids.update(path.stem for path in directory.glob("W*.json"))

    top_tier_ids = {
        path.stem for path in (RESPONSES_DIR / "_top_cited_cs").glob("W*.json")
    }
    unicamp_ids = {
        path.stem for path in (RESPONSES_DIR / "_unicamp_cs").glob("W*.json")
    }
    return institution_ids, top_tier_ids, unicamp_ids


def largest_weak_component(graph):
    nodes = max(nx.weakly_connected_components(graph), key=len)
    return graph.subgraph(nodes).copy()


def pagerank_power_iteration(graph, alpha=0.85, tolerance=1e-12, max_iterations=100):
    node_count = graph.number_of_nodes()
    pagerank = {node: 1 / node_count for node in graph}
    out_degree = dict(graph.out_degree())

    for _ in range(max_iterations):
        dangling_sum = sum(
            pagerank[node]
            for node, degree in out_degree.items()
            if degree == 0
        )
        base_value = (1 - alpha + alpha * dangling_sum) / node_count
        updated = {node: base_value for node in graph}

        for source in graph:
            if out_degree[source] == 0:
                continue
            contribution = alpha * pagerank[source] / out_degree[source]
            for target in graph.successors(source):
                updated[target] += contribution

        error = sum(abs(updated[node] - pagerank[node]) for node in graph)
        pagerank = updated
        if error < node_count * tolerance:
            return pagerank

    raise RuntimeError("PageRank não convergiu")


def minmax_normalize(values):
    minimum = min(values.values())
    maximum = max(values.values())
    if maximum == minimum:
        return {key: 0.0 for key in values}
    return {
        key: (value - minimum) / (maximum - minimum)
        for key, value in values.items()
    }


def safe_ratio(numerator, denominator):
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def local_core_numbers(local_graph):
    undirected = local_graph.to_undirected()
    undirected.remove_edges_from(nx.selfloop_edges(undirected))
    if undirected.number_of_edges() == 0:
        return {node: 0 for node in undirected.nodes}
    return nx.core_number(undirected)


def load_official_partition():
    with OFFICIAL_PARTITION_PATH.open(encoding="utf-8") as file:
        return {
            row["openalex_id"]: int(row["community_id"])
            for row in csv.DictReader(file)
        }


def load_article_set_assignments():
    with (TABLES_DIR / "article_set_assignments.csv").open(encoding="utf-8") as file:
        return {
            row["openalex_id"]: row["article_set"]
            for row in csv.DictReader(file)
        }


def count_unicamp_articles_by_community(partition, article_sets):
    return Counter(
        community_id
        for article_id, community_id in partition.items()
        if article_sets.get(article_id) == "uni"
    )


def get_neo4j_driver():
    from neo4j import GraphDatabase

    load_dotenv(ROOT / ".env")
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")),
    )


def persist_article_sets(assignments, batch_size=1000):
    rows = [
        {"openalex_id": openalex_id, "article_set": article_set}
        for openalex_id, article_set in assignments.items()
    ]
    query = """
        UNWIND $rows AS row
        MATCH (article:Article {openalex_id: row.openalex_id})
        SET article.article_set = row.article_set
        RETURN count(article) AS updated
    """

    updated = 0
    driver = get_neo4j_driver()
    with driver.session(database=os.getenv("NEO4J_DATABASE", "neo4j")) as session:
        for start in range(0, len(rows), batch_size):
            result = session.run(query, rows=rows[start:start + batch_size]).single()
            updated += result["updated"]
    driver.close()
    return updated


def count_matching_article_sets(assignments, batch_size=1000):
    rows = [
        {"openalex_id": openalex_id, "article_set": article_set}
        for openalex_id, article_set in assignments.items()
    ]
    query = """
        UNWIND $rows AS row
        MATCH (article:Article {openalex_id: row.openalex_id})
        WHERE article.article_set = row.article_set
        RETURN count(article) AS matched
    """

    matched = 0
    driver = get_neo4j_driver()
    with driver.session(database=os.getenv("NEO4J_DATABASE", "neo4j")) as session:
        for start in range(0, len(rows), batch_size):
            result = session.run(query, rows=rows[start:start + batch_size]).single()
            matched += result["matched"]
    driver.close()
    return matched


def write_csv(path, fieldnames, rows):
    fieldnames = list(fieldnames)
    rows = list(rows)
    if "community_id" in fieldnames and "community_name" not in fieldnames:
        updated_fieldnames = []
        for fieldname in fieldnames:
            updated_fieldnames.append(fieldname)
            if fieldname == "community_id":
                updated_fieldnames.append("community_name")
        fieldnames = updated_fieldnames
        rows = [
            {
                **row,
                "community_name": community_label(row["community_id"]),
            }
            for row in rows
        ]

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

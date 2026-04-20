from Neo4jLocalClient import Neo4jLocalClient
import schema

from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv(Path(__file__).resolve().parent / "../../.env")

def create_schema(client: Neo4jLocalClient, include_enterprise: bool = False) -> None:
    for stmt in schema.statements:
        client.query(stmt)

    if include_enterprise:
        for stmt in schema.enterprise_only_statements:
            client.query(stmt)


def clear_database(client: Neo4jLocalClient):
    
    constraint_rows = client.query("SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name")

    for row in constraint_rows:
        client.query(f"DROP CONSTRAINT {row['name']} IF EXISTS")

    index_rows = client.query(
        "SHOW INDEXES YIELD name, type "
        "WHERE type <> 'LOOKUP' "
        "RETURN name ORDER BY name"
    )
    
    for row in index_rows:
        client.query(f"DROP INDEX {row['name']} IF EXISTS")

    client.query("MATCH (n) DETACH DELETE n")


def populate(client: Neo4jLocalClient) -> None:
    articles = [
        {
            "openalex_id": "https://openalex.org/W1001",
            "title": "Citation Networks for Research Discovery",
            "publication_year": 2022,
            "doi": "https://doi.org/10.1000/cna.2022.001",
            "is_from_ic": True,
            "is_high_impact": False,
        },
        {
            "openalex_id": "https://openalex.org/W1002",
            "title": "Graph Analytics in Scientific Literature",
            "publication_year": 2023,
            "doi": "https://doi.org/10.1000/cna.2023.002",
            "is_from_ic": True,
            "is_high_impact": True,
        },
        {
            "openalex_id": "https://openalex.org/W1003",
            "title": "Visual Exploration of Bibliometric Data",
            "publication_year": 2024,
            "doi": "https://doi.org/10.1000/cna.2024.003",
            "is_from_ic": False,
            "is_high_impact": True,
        },
    ]

    authors = [
        {
            "openalex_id": "https://openalex.org/A1001",
            "display_name": "Ana Souza",
            "is_ic_researcher": True,
        },
        {
            "openalex_id": "https://openalex.org/A1002",
            "display_name": "Bruno Lima",
            "is_ic_researcher": False,
        },
        {
            "openalex_id": "https://openalex.org/A1003",
            "display_name": "Carla Mendes",
            "is_ic_researcher": True,
        },
    ]

    institutions = [
        {
            "openalex_id": "https://openalex.org/I1001",
            "display_name": "Instituto de Computacao Inteligente",
        },
        {
            "openalex_id": "https://openalex.org/I1002",
            "display_name": "Centro de Estudos Bibliometricos",
        },
    ]

    venues = [
        {
            "openalex_id": "https://openalex.org/S1001",
            "display_name": "Journal of Citation Analysis",
        },
        {
            "openalex_id": "https://openalex.org/S1002",
            "display_name": "Graph Research Conference",
        },
    ]

    concepts = [
        {
            "openalex_id": "https://openalex.org/C1001",
            "display_name": "Bibliometrics",
        },
        {
            "openalex_id": "https://openalex.org/C1002",
            "display_name": "Graph Mining",
        },
        {
            "openalex_id": "https://openalex.org/C1003",
            "display_name": "Data Visualization",
        },
    ]

    authored_by = [
        {
            "article_id": "https://openalex.org/W1001",
            "author_id": "https://openalex.org/A1001",
        },
        {
            "article_id": "https://openalex.org/W1001",
            "author_id": "https://openalex.org/A1002",
        },
        {
            "article_id": "https://openalex.org/W1002",
            "author_id": "https://openalex.org/A1002",
        },
        {
            "article_id": "https://openalex.org/W1002",
            "author_id": "https://openalex.org/A1003",
        },
        {
            "article_id": "https://openalex.org/W1003",
            "author_id": "https://openalex.org/A1001",
        },
        {
            "article_id": "https://openalex.org/W1003",
            "author_id": "https://openalex.org/A1003",
        },
    ]

    author_affiliations = [
        {
            "author_id": "https://openalex.org/A1001",
            "institution_id": "https://openalex.org/I1001",
        },
        {
            "author_id": "https://openalex.org/A1002",
            "institution_id": "https://openalex.org/I1001",
        },
        {
            "author_id": "https://openalex.org/A1003",
            "institution_id": "https://openalex.org/I1002",
        },
    ]

    published_in = [
        {
            "article_id": "https://openalex.org/W1001",
            "venue_id": "https://openalex.org/S1001",
        },
        {
            "article_id": "https://openalex.org/W1002",
            "venue_id": "https://openalex.org/S1002",
        },
        {
            "article_id": "https://openalex.org/W1003",
            "venue_id": "https://openalex.org/S1001",
        },
    ]

    article_concepts = [
        {
            "article_id": "https://openalex.org/W1001",
            "concept_id": "https://openalex.org/C1001",
        },
        {
            "article_id": "https://openalex.org/W1001",
            "concept_id": "https://openalex.org/C1002",
        },
        {
            "article_id": "https://openalex.org/W1002",
            "concept_id": "https://openalex.org/C1002",
        },
        {
            "article_id": "https://openalex.org/W1003",
            "concept_id": "https://openalex.org/C1001",
        },
        {
            "article_id": "https://openalex.org/W1003",
            "concept_id": "https://openalex.org/C1003",
        },
    ]

    citations = [
        {
            "source_id": "https://openalex.org/W1002",
            "target_id": "https://openalex.org/W1001",
        },
        {
            "source_id": "https://openalex.org/W1003",
            "target_id": "https://openalex.org/W1001",
        },
        {
            "source_id": "https://openalex.org/W1003",
            "target_id": "https://openalex.org/W1002",
        },
    ]

    for article in articles:
        client.query(
            """
            MERGE (a:Article {openalex_id: $openalex_id})
            SET a.title = $title,
                a.publication_year = $publication_year,
                a.doi = $doi,
                a.is_from_ic = $is_from_ic,
                a.is_high_impact = $is_high_impact
            """,
            article,
        )

    for author in authors:
        client.query(
            """
            MERGE (a:Author {openalex_id: $openalex_id})
            SET a.display_name = $display_name,
                a.is_ic_researcher = $is_ic_researcher
            """,
            author,
        )

    for institution in institutions:
        client.query(
            """
            MERGE (i:Institution {openalex_id: $openalex_id})
            SET i.display_name = $display_name
            """,
            institution,
        )

    for venue in venues:
        client.query(
            """
            MERGE (v:Venue {openalex_id: $openalex_id})
            SET v.display_name = $display_name
            """,
            venue,
        )

    for concept in concepts:
        client.query(
            """
            MERGE (c:Concept {openalex_id: $openalex_id})
            SET c.display_name = $display_name
            """,
            concept,
        )

    for relation in authored_by:
        client.query(
            """
            MATCH (article:Article {openalex_id: $article_id})
            MATCH (author:Author {openalex_id: $author_id})
            MERGE (article)-[:AUTHORED_BY]->(author)
            """,
            relation,
        )

    for relation in author_affiliations:
        client.query(
            """
            MATCH (author:Author {openalex_id: $author_id})
            MATCH (institution:Institution {openalex_id: $institution_id})
            MERGE (author)-[:AFFILIATED_WITH]->(institution)
            """,
            relation,
        )

    for relation in published_in:
        client.query(
            """
            MATCH (article:Article {openalex_id: $article_id})
            MATCH (venue:Venue {openalex_id: $venue_id})
            MERGE (article)-[:PUBLISHED_IN]->(venue)
            """,
            relation,
        )

    for relation in article_concepts:
        client.query(
            """
            MATCH (article:Article {openalex_id: $article_id})
            MATCH (concept:Concept {openalex_id: $concept_id})
            MERGE (article)-[:HAS_CONCEPT]->(concept)
            """,
            relation,
        )

    for relation in citations:
        client.query(
            """
            MATCH (source:Article {openalex_id: $source_id})
            MATCH (target:Article {openalex_id: $target_id})
            MERGE (source)-[:CITES]->(target)
            """,
            relation,
        )

if __name__ == "__main__":
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE", "neo4j")

    client = Neo4jLocalClient(uri, username, password, database)
    clear_database(client)
    create_schema(client)
    #populate(client)
    
    client.close()

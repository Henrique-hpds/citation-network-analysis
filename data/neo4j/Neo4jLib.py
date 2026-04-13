from Neo4jLocalClient import Neo4jLocalClient
from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv(Path(__file__).resolve().parent / "../../.env")


def create_schema(client: Neo4jLocalClient) -> None:
    """
    - nó: Article {id, titulo, ano, venue, impacto, area}
    - nó: Author  {id, nome, afiliacao, autor_ic?}
    - aresta: CITES
    - aresta: AUTHORED_BY
    - opcional: :AFFILIATED_WITH
    """

    statements = [
        # Constraints de unicidade
        "CREATE CONSTRAINT article_id_unique IF NOT EXISTS "
        "FOR (a:Article) REQUIRE a.id IS UNIQUE",

        "CREATE CONSTRAINT author_id_unique IF NOT EXISTS "
        "FOR (a:Author) REQUIRE a.id IS UNIQUE",

        # Índices úteis para busca
        "CREATE INDEX article_title_idx IF NOT EXISTS "
        "FOR (a:Article) ON (a.titulo)",

        "CREATE INDEX article_year_idx IF NOT EXISTS "
        "FOR (a:Article) ON (a.ano)",

        "CREATE INDEX article_area_idx IF NOT EXISTS "
        "FOR (a:Article) ON (a.area)",

        "CREATE INDEX article_venue_idx IF NOT EXISTS "
        "FOR (a:Article) ON (a.venue)",

        "CREATE INDEX author_name_idx IF NOT EXISTS "
        "FOR (a:Author) ON (a.nome)",

        "CREATE INDEX author_affiliation_idx IF NOT EXISTS "
        "FOR (a:Author) ON (a.afiliacao)",
    ]

    for stmt in statements:
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

if __name__ == "__main__":
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE", "neo4j")

    client = Neo4jLocalClient(uri, username, password, database)
    clear_database(client)
    create_schema(client)
    
    client.close()

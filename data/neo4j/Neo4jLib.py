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

if __name__ == "__main__":
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE", "neo4j")

    client = Neo4jLocalClient(uri, username, password, database)
    clear_database(client)
    create_schema(client)
    
    client.close()

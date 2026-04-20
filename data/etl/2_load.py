"""
2_load.py

Reads the flattened batch JSON files produced by 1_flatten.py and
populates a Neo4j database using MERGE (idempotent — safe to re-run).

Usage:
    pip install neo4j

    python 2_load.py --input ./data/flat

Optional flags:
    --batch-size   Records per transaction (default: 500)
    --skip-rels    Skip relationship loading (useful to load nodes first)
"""

import argparse
import json
import sys
from pathlib import Path
from dotenv import load_dotenv
import os

from neo4j import GraphDatabase


# ---------------------------------------------------------------------------
# Cypher statements  (one per entity / relationship type)
# ---------------------------------------------------------------------------

MERGE_ARTICLE = """
UNWIND $rows AS r
MERGE (a:Article {openalex_id: r.openalex_id})
SET
  a.doi              = r.doi,
  a.title            = r.title,
  a.publication_year = r.publication_year,
  a.publication_date = r.publication_date,
  a.language         = r.language,
  a.type             = r.type,
  a.cited_by_count   = r.cited_by_count,
  a.is_retracted     = r.is_retracted,
  a.is_oa            = r.is_oa,
  a.oa_status        = r.oa_status
"""

MERGE_AUTHOR = """
UNWIND $rows AS r
MERGE (a:Author {openalex_id: r.openalex_id})
SET
  a.display_name = r.display_name,
  a.orcid        = r.orcid
"""

MERGE_INSTITUTION = """
UNWIND $rows AS r
MERGE (i:Institution {openalex_id: r.openalex_id})
SET
  i.display_name = r.display_name,
  i.ror          = r.ror,
  i.country_code = r.country_code,
  i.type         = r.type
"""

MERGE_VENUE = """
UNWIND $rows AS r
MERGE (v:Venue {openalex_id: r.openalex_id})
SET
  v.display_name = r.display_name,
  v.issn_l       = r.issn_l,
  v.type         = r.type,
  v.is_oa        = r.is_oa,
  v.is_in_doaj   = r.is_in_doaj
"""

MERGE_TOPIC = """
UNWIND $rows AS r
MERGE (t:Topic {openalex_id: r.openalex_id})
SET
  t.display_name  = r.display_name,
  t.subfield_id   = r.subfield_id,
  t.subfield_name = r.subfield_name,
  t.field_id      = r.field_id,
  t.field_name    = r.field_name,
  t.domain_id     = r.domain_id,
  t.domain_name   = r.domain_name
"""

MERGE_FUNDER = """
UNWIND $rows AS r
MERGE (f:Funder {openalex_id: r.openalex_id})
SET f.display_name = r.display_name
"""

# Relationships — each requires the nodes to already exist (run nodes first)

REL_CITES = """
UNWIND $rows AS r
MATCH (src:Article {openalex_id: r.article_id})
UNWIND r.cited_works AS cited_id
MATCH (dst:Article {openalex_id: cited_id})
MERGE (src)-[:CITES]->(dst)
"""

REL_AUTHORED_BY = """
UNWIND $rows AS r
MATCH (art:Article {openalex_id: r.article_id})
UNWIND r.authored_by AS ab
MATCH (auth:Author {openalex_id: ab.author_id})
MERGE (art)-[rel:AUTHORED_BY]->(auth)
SET
  rel.author_position  = ab.author_position,
  rel.is_corresponding = ab.is_corresponding,
  rel.countries        = ab.countries
WITH art, auth, ab
UNWIND ab.institution_ids AS iid
MATCH (inst:Institution {openalex_id: iid})
MERGE (auth)-[:WORKS_AT]->(inst)
"""

REL_PUBLISHED_IN = """
UNWIND $rows AS r
WITH r WHERE r.venue_id IS NOT NULL
MATCH (art:Article  {openalex_id: r.article_id})
MATCH (v:Venue      {openalex_id: r.venue_id})
MERGE (art)-[:PUBLISHED_IN]->(v)
"""

REL_HAS_TOPIC = """
UNWIND $rows AS r
MATCH (art:Article {openalex_id: r.article_id})
UNWIND r.topic_ids AS ti
MATCH (t:Topic {openalex_id: ti.topic_id})
MERGE (art)-[rel:HAS_TOPIC]->(t)
SET rel.is_primary = ti.is_primary
"""

REL_FUNDED_BY = """
UNWIND $rows AS r
MATCH (art:Article {openalex_id: r.article_id})
UNWIND r.funder_ids AS fid
MATCH (f:Funder {openalex_id: fid})
MERGE (art)-[:FUNDED_BY]->(f)
"""

REL_AFFILIATED_WITH = """
UNWIND $rows AS r
MATCH (art:Article {openalex_id: r.article_id})
UNWIND r.authored_by AS ab
MATCH (auth:Author {openalex_id: ab.author_id})
UNWIND ab.institution_ids AS iid
MATCH (inst:Institution {openalex_id: iid})
MERGE (art)-[:AFFILIATED_WITH]->(inst)
"""


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_batches(session, cypher: str, files: list[Path], tx_batch: int, label: str):
    """Stream records from a list of JSON files and execute cypher in batches."""
    total = 0
    buffer = []

    def flush():
        nonlocal total
        if not buffer:
            return
        session.run(cypher, rows=buffer)
        total += len(buffer)
        buffer.clear()

    for path in sorted(files):
        with open(path, encoding="utf-8") as f:
            records = json.load(f)
        for rec in records:
            buffer.append(rec)
            if len(buffer) >= tx_batch:
                flush()
        flush()  # tail of each file

    print(f"  {label:<20} {total:>10,} records")
    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Load flattened OpenAlex batches into Neo4j.")
    parser.add_argument("--input",      required=True, help="Directory produced by 1_flatten.py")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Records per Neo4j transaction (default: 500)")
    parser.add_argument("--skip-rels",  action="store_true",
                        help="Skip relationship loading (load nodes only)")
    args = parser.parse_args()

    flat = Path(args.input)

    load_dotenv()

    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")

    driver = GraphDatabase.driver(uri, auth=(user, password))

    print("=== Phase 1: Nodes ===")
    with driver.session() as session:
        load_batches(session, MERGE_ARTICLE,      list((flat / "articles").glob("*.json")),      args.batch_size, "Articles")
        load_batches(session, MERGE_AUTHOR,       list((flat / "authors").glob("*.json")),       args.batch_size, "Authors")
        load_batches(session, MERGE_INSTITUTION,  list((flat / "institutions").glob("*.json")),  args.batch_size, "Institutions")
        load_batches(session, MERGE_VENUE,        list((flat / "venues").glob("*.json")),        args.batch_size, "Venues")
        load_batches(session, MERGE_TOPIC,        list((flat / "topics").glob("*.json")),        args.batch_size, "Topics")
        load_batches(session, MERGE_FUNDER,       list((flat / "funders").glob("*.json")),       args.batch_size, "Funders")

    if args.skip_rels:
        print("\n--skip-rels set — skipping relationships.")
        driver.close()
        return

    print("\n=== Phase 2: Relationships ===")
    rel_files = list((flat / "relationships").glob("*.json"))

    with driver.session() as session:
        load_batches(session, REL_AUTHORED_BY,    rel_files, args.batch_size, "AUTHORED_BY")
        load_batches(session, REL_PUBLISHED_IN,   rel_files, args.batch_size, "PUBLISHED_IN")
        load_batches(session, REL_HAS_TOPIC,      rel_files, args.batch_size, "HAS_TOPIC")
        load_batches(session, REL_FUNDED_BY,      rel_files, args.batch_size, "FUNDED_BY")
        load_batches(session, REL_AFFILIATED_WITH,rel_files, args.batch_size, "AFFILIATED_WITH")

        print("\n  CITES edges (this will take a while on large datasets)...")
        load_batches(session, REL_CITES,          rel_files, args.batch_size, "CITES")

    print("\nDone.")
    driver.close()


if __name__ == "__main__":
    main()

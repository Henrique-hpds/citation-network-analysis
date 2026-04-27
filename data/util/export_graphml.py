"""
export_graphml.py

Exports a field-filtered subgraph from Neo4j to a single GraphML file.
Connects via the Python driver — no APOC required.

Exported nodes and fields:
    Article      : openalex_id, doi, title, publication_year, cited_by_count
    Author       : openalex_id
    Institution  : openalex_id, country_code, display_name, ror
    Subfield     : openalex_id, display_name, field_name
    Venue        : openalex_id, display_name, type

Exported relationships (no extra properties unless noted):
    AFFILIATED_WITH  Article → Institution
    AUTHORED_BY      Article → Author       (+author_position)
    CITES            Article → Article
    HAS_SUBFIELD     Article → Subfield
    PUBLISHED_IN     Article → Venue

Requires:
    pip install neo4j

Usage:
    python export_graphml.py \\
        --uri      bolt://localhost:7687 \\
        --user     neo4j \\
        --password secret \\
        --output   network.graphml
"""

import argparse
import sys
import xml.etree.ElementTree as ET
from xml.dom import minidom
from dotenv import load_dotenv
import os

try:
    from neo4j import GraphDatabase
except ImportError:
    print("[ERROR] neo4j driver not installed.  Run: pip install neo4j", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Cypher queries — project only the fields we want
# ---------------------------------------------------------------------------

QUERY_ARTICLES = """
MATCH (a:Article)
RETURN
    id(a)               AS neo_id,
    a.title             AS title,
    a.cited_by_count    AS cited_by_count,
    a.doi               AS doi,
    a.openalex_id       AS openalex_id,
    a.publication_year  AS publication_year
"""

QUERY_AUTHORS = """
MATCH (a:Article)-[:AUTHORED_BY]->(auth:Author)
RETURN DISTINCT
    id(auth)            AS neo_id,
    auth.openalex_id    AS openalex_id
"""

QUERY_INSTITUTIONS = """
MATCH (a:Article)-[:AFFILIATED_WITH]->(i:Institution)
RETURN DISTINCT
    id(i)               AS neo_id,
    i.display_name      AS display_name,
    i.country_code      AS country_code,
    i.openalex_id       AS openalex_id,
    i.ror               AS ror
"""

QUERY_SUBFIELDS = """
MATCH (a:Article)-[:HAS_SUBFIELD]->(s:Subfield)
RETURN DISTINCT
    id(s)               AS neo_id,
    s.display_name      AS display_name,
    s.openalex_id       AS openalex_id,
    s.field_name        AS field_name
"""

QUERY_VENUES = """
MATCH (a:Article)-[:PUBLISHED_IN]->(v:Venue)
RETURN DISTINCT
    id(v)               AS neo_id,
    v.openalex_id       AS openalex_id,
    v.display_name      AS display_name,
    v.type              AS type
"""

QUERY_REL_AFFILIATED_WITH = """
MATCH (a:Article)-[r:AFFILIATED_WITH]->(i:Institution)
RETURN id(r) AS rel_id, id(a) AS src, id(i) AS tgt
"""

QUERY_REL_AUTHORED_BY = """
MATCH (a:Article)-[r:AUTHORED_BY]->(auth:Author)
RETURN id(r) AS rel_id, id(a) AS src, id(auth) AS tgt, r.author_position AS author_position
"""

QUERY_REL_CITES = """
MATCH (a1:Article)-[r:CITES]->(a2:Article)
RETURN id(r) AS rel_id, id(a1) AS src, id(a2) AS tgt
"""

QUERY_REL_HAS_SUBFIELD = """
MATCH (a:Article)-[r:HAS_SUBFIELD]->(s:Subfield)
RETURN id(r) AS rel_id, id(a) AS src, id(s) AS tgt
"""

QUERY_REL_PUBLISHED_IN = """
MATCH (a:Article)-[r:PUBLISHED_IN]->(v:Venue)
RETURN id(r) AS rel_id, id(a) AS src, id(v) AS tgt
"""


# ---------------------------------------------------------------------------
# GraphML builder
# ---------------------------------------------------------------------------

# GraphML attr.type mapping
_ATTR_TYPE = {
    str:   "string",
    int:   "int",
    float: "double",
    bool:  "boolean",
}

def _graphml_type(value) -> str:
    return _ATTR_TYPE.get(type(value), "string")


class GraphMLWriter:
    """Builds a GraphML document incrementally and writes it to disk."""

    def __init__(self):
        self._keys:  dict[str, ET.Element] = {}   # key_id → <key> element
        self._nodes: list[ET.Element]       = []
        self._edges: list[ET.Element]       = []
        self._key_counter = 0

    def _get_key(self, name: str, for_: str, value) -> str:
        """Return (creating if necessary) a <key> id for this attribute."""
        key_id = f"{for_}_{name}"
        if key_id not in self._keys:
            k = ET.Element("key", {
                "id":        key_id,
                "for":       for_,
                "attr.name": name,
                "attr.type": _graphml_type(value),
            })
            self._keys[key_id] = k
        return key_id

    def add_node(self, neo_id: int, label: str, props: dict):
        node = ET.Element("node", {"id": f"n{neo_id}", "labels": label})
        # always add the label as a data element (Gephi / yEd convention)
        label_key = self._get_key("label", "node", "")
        d = ET.SubElement(node, "data", {"key": label_key})
        d.text = label
        for k, v in props.items():
            if v is None:
                continue
            key_id = self._get_key(k, "node", v)
            d = ET.SubElement(node, "data", {"key": key_id})
            d.text = str(v)
        self._nodes.append(node)

    def add_edge(self, rel_id: int, src: int, tgt: int, label: str, props: dict | None = None):
        edge = ET.Element("edge", {
            "id":     f"e{rel_id}",
            "source": f"n{src}",
            "target": f"n{tgt}",
            "label":  label,
        })
        for k, v in (props or {}).items():
            if v is None:
                continue
            key_id = self._get_key(k, "edge", v)
            d = ET.SubElement(edge, "data", {"key": key_id})
            d.text = str(v)
        self._edges.append(edge)

    def write(self, output_path: str):
        root = ET.Element("graphml", {
            "xmlns":              "http://graphml.graphdrawing.org/graphml",
            "xmlns:xsi":          "http://www.w3.org/2001/XMLSchema-instance",
            "xsi:schemaLocation": (
                "http://graphml.graphdrawing.org/graphml "
                "http://graphml.graphdrawing.org/graphml/graphml.xsd"
            ),
        })
        for key_el in self._keys.values():
            root.append(key_el)

        graph = ET.SubElement(root, "graph", {"id": "G", "edgedefault": "directed"})
        for n in self._nodes:
            graph.append(n)
        for e in self._edges:
            graph.append(e)

        with open(output_path, "wb") as f:
            f.write(b'<?xml version="1.0" encoding="utf-8"?>')
            ET.ElementTree(root).write(f, encoding="utf-8", xml_declaration=False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Export a field-filtered Neo4j subgraph to GraphML."
    )
    parser.add_argument("--output",   default="network.graphml",
                        help="Output GraphML file (default: network.graphml)")
    args = parser.parse_args()

    load_dotenv()

    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE", "neo4j")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    writer = GraphMLWriter()

    try:
        with driver.session(database=database) as session:

            # --- nodes ------------------------------------------------------

            print("Fetching Article nodes ...", flush=True)
            rows = session.run(QUERY_ARTICLES).data()
            for r in rows:
                writer.add_node(r["neo_id"], "Article", {
                    "openalex_id":      r["openalex_id"],
                    "doi":              r["doi"],
                    "title":            r["title"],
                    "publication_year": r["publication_year"],
                    "cited_by_count":   r["cited_by_count"],
                })
            print(f"  {len(rows):,} articles")

            # print("Fetching Author nodes ...", flush=True)
            # rows = session.run(QUERY_AUTHORS).data()
            # for r in rows:
            #     writer.add_node(r["neo_id"], "Author", {
            #         "openalex_id": r["openalex_id"],
            #     })
            # print(f"  {len(rows):,} authors")

            print("Fetching Institution nodes ...", flush=True)
            rows = session.run(QUERY_INSTITUTIONS).data()
            for r in rows:
                writer.add_node(r["neo_id"], "Institution", {
                    "openalex_id":  r["openalex_id"],
                    "display_name": r["display_name"],
                    "country_code": r["country_code"],
                    "ror":          r["ror"],
                })
            print(f"  {len(rows):,} institutions")

            print("Fetching Subfield nodes ...", flush=True)
            rows = session.run(QUERY_SUBFIELDS).data()
            for r in rows:
                writer.add_node(r["neo_id"], "Subfield", {
                    "openalex_id":  r["openalex_id"],
                    "display_name": r["display_name"],
                    "field_name":   r["field_name"],
                })
            print(f"  {len(rows):,} subfields")

            print("Fetching Venue nodes ...", flush=True)
            rows = session.run(QUERY_VENUES).data()
            for r in rows:
                writer.add_node(r["neo_id"], "Venue", {
                    "openalex_id":  r["openalex_id"],
                    "display_name": r["display_name"],
                    "type":         r["type"],
                })
            print(f"  {len(rows):,} venues")

            # --- relationships ----------------------------------------------

            print("Fetching AFFILIATED_WITH ...", flush=True)
            rows = session.run(QUERY_REL_AFFILIATED_WITH).data()
            for r in rows:
                writer.add_edge(r["rel_id"], r["src"], r["tgt"], "AFFILIATED_WITH")
            print(f"  {len(rows):,} edges")

            # print("Fetching AUTHORED_BY ...", flush=True)
            # rows = session.run(QUERY_REL_AUTHORED_BY).data()
            # for r in rows:
            #     writer.add_edge(r["rel_id"], r["src"], r["tgt"], "AUTHORED_BY")
            #                     # , {"author_position": r["author_position"]})
            # print(f"  {len(rows):,} edges")

            print("Fetching CITES ...", flush=True)
            rows = session.run(QUERY_REL_CITES).data()
            for r in rows:
                writer.add_edge(r["rel_id"], r["src"], r["tgt"], "CITES")
            print(f"  {len(rows):,} edges")

            print("Fetching HAS_SUBFIELD ...", flush=True)
            rows = session.run(QUERY_REL_HAS_SUBFIELD).data()
            for r in rows:
                writer.add_edge(r["rel_id"], r["src"], r["tgt"], "HAS_SUBFIELD")
            print(f"  {len(rows):,} edges")

            print("Fetching PUBLISHED_IN ...", flush=True)
            rows = session.run(QUERY_REL_PUBLISHED_IN).data()
            for r in rows:
                writer.add_edge(r["rel_id"], r["src"], r["tgt"], "PUBLISHED_IN")
            print(f"  {len(rows):,} edges")

    finally:
        driver.close()

    print(f"\nWriting {args.output} ...", flush=True)
    writer.write(args.output)
    print("Done.")


if __name__ == "__main__":
    main()
"""
Build network.graphml from flattened ETL batches.

This is a local fallback for analysis scripts that expect the GraphML export
created from Neo4j. It preserves the schema consumed by
analysis/agent_graph_analysis.py:

    (:Article)-[:CITES]->(:Article)
    (:Article)-[:AFFILIATED_WITH]->(:Institution)
    (:Article)-[:HAS_SUBFIELD]->(:Subfield)
    (:Article)-[:PUBLISHED_IN]->(:Venue)
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import networkx as nx


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = ROOT / "data" / "flat_subgraph"
DEFAULT_OUTPUT = ROOT / "network.graphml"


def load_batches(directory: Path) -> list[dict]:
    rows: list[dict] = []
    for path in sorted(directory.glob("*.json")):
        rows.extend(json.loads(path.read_text(encoding="utf-8")))
    return rows


def add_node_once(G: nx.DiGraph, node_id: str, **attrs: object) -> None:
    if node_id in G:
        for key, value in attrs.items():
            if value not in (None, "") and not G.nodes[node_id].get(key):
                G.nodes[node_id][key] = str(value)
        return
    G.add_node(
        node_id,
        **{key: str(value) for key, value in attrs.items() if value is not None},
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build GraphML from data/flat_subgraph batches.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    flat = args.input
    G = nx.DiGraph()

    articles = load_batches(flat / "articles")
    institutions = {row["openalex_id"]: row for row in load_batches(flat / "institutions")}
    subfields = {row["openalex_id"]: row for row in load_batches(flat / "subfields")}
    venues = {row["openalex_id"]: row for row in load_batches(flat / "venues")}
    relationships = load_batches(flat / "relationships")

    article_ids = {row["openalex_id"] for row in articles}

    for row in articles:
        add_node_once(
            G,
            row["openalex_id"],
            labels=":Article",
            openalex_id=row["openalex_id"],
            doi=row.get("doi") or "",
            title=row.get("title") or "",
            publication_year=row.get("publication_year") or "",
            cited_by_count=row.get("cited_by_count") or 0,
        )

    for row in institutions.values():
        add_node_once(
            G,
            row["openalex_id"],
            labels=":Institution",
            openalex_id=row["openalex_id"],
            display_name=row.get("display_name") or "",
            country_code=row.get("country_code") or "",
            ror=row.get("ror") or "",
        )

    for row in subfields.values():
        add_node_once(
            G,
            row["openalex_id"],
            labels=":Subfield",
            openalex_id=row["openalex_id"],
            display_name=row.get("display_name") or "",
            field_name=row.get("field_name") or "",
        )

    for row in venues.values():
        add_node_once(
            G,
            row["openalex_id"],
            labels=":Venue",
            openalex_id=row["openalex_id"],
            display_name=row.get("display_name") or "",
            type=row.get("type") or "",
        )

    edge_id = 0
    for row in relationships:
        article_id = row["article_id"]
        if article_id not in article_ids:
            continue

        for cited_id in row.get("cited_works", []):
            if cited_id in article_ids:
                G.add_edge(article_id, cited_id, key=f"e{edge_id}", label="CITES")
                edge_id += 1

        for authorship in row.get("authored_by", []):
            for institution_id in authorship.get("institution_ids", []):
                if institution_id in institutions:
                    G.add_edge(article_id, institution_id, key=f"e{edge_id}", label="AFFILIATED_WITH")
                    edge_id += 1

        for subfield_id in row.get("subfield_ids", []):
            if subfield_id in subfields:
                G.add_edge(article_id, subfield_id, key=f"e{edge_id}", label="HAS_SUBFIELD")
                edge_id += 1

        venue_id = row.get("venue_id")
        if venue_id and venue_id in venues:
            G.add_edge(article_id, venue_id, key=f"e{edge_id}", label="PUBLISHED_IN")
            edge_id += 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    nx.write_graphml(G, args.output)

    labels = {}
    for _node, attrs in G.nodes(data=True):
        labels[attrs.get("labels", "")] = labels.get(attrs.get("labels", ""), 0) + 1
    edge_labels = {}
    for _source, _target, attrs in G.edges(data=True):
        edge_labels[attrs.get("label", "")] = edge_labels.get(attrs.get("label", ""), 0) + 1

    print(f"Wrote {args.output}")
    print(f"Nodes: {G.number_of_nodes():,} {labels}")
    print(f"Edges: {G.number_of_edges():,} {edge_labels}")


if __name__ == "__main__":
    main()

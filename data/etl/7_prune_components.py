import os
import sys
from collections import defaultdict, deque

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

_driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")),
)

def query(cypher: str, params: dict = None):
    with _driver.session(database=os.getenv("NEO4J_DATABASE")) as s:
        result = s.run(cypher, params or {})
        return [r.data() for r in result]


# ---------------------------------------------------------------------------
# WCC computation (in Python, over CITES edges)
# ---------------------------------------------------------------------------

def fetch_wcc_par():
    nodes = query("MATCH (a:Article) RETURN a.openalex_id AS id")
    edges = query("""
        MATCH (a:Article)-[:CITES]->(b:Article)
        RETURN a.openalex_id AS src, b.openalex_id AS dst
    """)

    adj = defaultdict(set)
    all_nodes = {r["id"] for r in nodes}
    for e in edges:
        adj[e["src"]].add(e["dst"])
        adj[e["dst"]].add(e["src"])

    visited  = set()
    parent   = {node: None for node in all_nodes}

    for node in all_nodes:
        if node in visited:
            continue
        queue = deque([node])
        visited.add(node)
        while queue:
            cur = queue.popleft()
            for nb in adj[cur]:
                if nb not in visited:
                    parent[nb] = cur
                    visited.add(nb)
                    queue.append(nb)

    return parent


# ---------------------------------------------------------------------------
# Component classification
# ---------------------------------------------------------------------------

def get_components_unicamp(parent: dict) -> dict:
    """
    Returns {root_id: {"nodes": set, "has_unicamp": bool}}.

    Uses Union-Find root lookup (path compression) instead of manual
    traversal to avoid cycles and O(n²) behaviour.
    """
    unicamp_articles = query(
        "MATCH (a:Article)-[:AFFILIATED_WITH]->(i:Institution {openalex_id: 'I181391015'}) "
        "RETURN a.openalex_id AS id"
    )
    unicamp_ids = {r["id"] for r in unicamp_articles}

    # Find root with memoisation
    roots: dict[str, str] = {}

    def find_root(node: str) -> str:
        if node in roots:
            return roots[node]
        path = []
        cur  = node
        while parent.get(cur) is not None:
            path.append(cur)
            cur = parent[cur]
        # cur is now the root; compress path
        for n in path:
            roots[n] = cur
        roots[cur] = cur
        return cur

    components: dict[str, dict] = {}
    for node in parent:
        root = find_root(node)
        if root not in components:
            components[root] = {"nodes": set(), "has_unicamp": False}
        components[root]["nodes"].add(node)
        if node in unicamp_ids:
            components[root]["has_unicamp"] = True

    return components


def get_retracted_articles() -> set[str]:
    retracted = query(
        "MATCH (a:Article) WHERE a.is_retracted = true RETURN a.openalex_id AS id"
    )
    return {r["id"] for r in retracted}


# ---------------------------------------------------------------------------
# Deletion helpers
# ---------------------------------------------------------------------------

def _delete_articles_in_batches(ids: set[str], batch_size: int, dry_run: bool) -> int:
    """DETACH DELETE articles by openalex_id in batches. Returns total deleted."""
    if not ids:
        return 0
    id_list = list(ids)
    total   = 0
    for i in range(0, len(id_list), batch_size):
        chunk = id_list[i:i + batch_size]
        if dry_run:
            total += len(chunk)
        else:
            with _driver.session(database=os.getenv("NEO4J_DATABASE")) as s:
                result = s.run(
                    """
                    MATCH (a:Article)
                    WHERE a.openalex_id IN $ids
                    DETACH DELETE a
                    RETURN count(a) AS deleted
                    """,
                    ids=chunk,
                )
                total += result.single()["deleted"]
    return total


def _delete_orphans(dry_run: bool) -> dict[str, int]:
    """
    Remove related nodes that are no longer connected to any Article.
    Returns a dict with counts per label.
    """
    steps = [
        ("Author",      "MATCH (n:Author)      WHERE NOT (:Article)-[:AUTHORED_BY]->(n)"),
        ("Institution", "MATCH (n:Institution) WHERE NOT (:Article)-[:AFFILIATED_WITH]->(n) "
                        "AND NOT (:Author)-[:WORKS_AT]->(n)"),
        ("Venue",       "MATCH (n:Venue)       WHERE NOT (:Article)-[:PUBLISHED_IN]->(n)"),
        ("Subfield",    "MATCH (n:Subfield)    WHERE NOT (:Article)-[:HAS_SUBFIELD]->(n)"),
    ]
    counts = {}
    for label, match in steps:
        with _driver.session(database=os.getenv("NEO4J_DATABASE")) as s:
            count = s.run(f"{match} RETURN count(n) AS c").single()["c"]
        if not dry_run and count > 0:
            with _driver.session(database=os.getenv("NEO4J_DATABASE")) as s:
                s.run(f"{match} DETACH DELETE n")
        counts[label] = count
    return counts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    dry_run = "--dry-run" in sys.argv

    if dry_run:
        print("=== DRY RUN — no changes will be made ===\n")

    # --- compute WCCs -------------------------------------------------------
    print("Fetching graph from Neo4j ...", flush=True)
    wcc_par    = fetch_wcc_par()
    components = get_components_unicamp(wcc_par)

    sizes            = {root: len(c["nodes"]) for root, c in components.items()}
    biggest          = max(sizes, key=sizes.get) if sizes else None
    unicamp_count    = sum(1 for c in components.values() if c["has_unicamp"])

    print(f"Total components            : {len(components):,}")
    print(f"Biggest component           : {biggest} ({sizes[biggest]:,} nodes)")
    print(f"Components with Unicamp     : {unicamp_count:,}")

    # --- build prune list ---------------------------------------------------
    pruned_no_unicamp: set[str] = set()
    pruned_singletons: set[str] = set()

    for root, component in components.items():
        if not component["has_unicamp"]:
            pruned_no_unicamp.update(component["nodes"])
        if sizes[root] == 1:
            pruned_singletons.update(component["nodes"])

    prune_retracted = get_retracted_articles()

    prune_list = pruned_no_unicamp | pruned_singletons | prune_retracted

    print(f"\nArticles to prune:")
    print(f"  No-Unicamp components : {len(pruned_no_unicamp):,}")
    print(f"  Singleton components  : {len(pruned_singletons):,}")
    print(f"  Retracted             : {len(prune_retracted):,}")
    print(f"  Total (deduplicated)  : {len(prune_list):,}")

    if not prune_list:
        print("\nNothing to prune.")
        return

    # --- delete articles ----------------------------------------------------
    BATCH_SIZE = 5_000

    print(f"\n{'[DRY RUN] Would delete' if dry_run else 'Deleting'} "
          f"{len(prune_list):,} articles in batches of {BATCH_SIZE:,} ...")

    deleted_articles = _delete_articles_in_batches(prune_list, BATCH_SIZE, dry_run)
    print(f"  {'Would remove' if dry_run else 'Removed'}: {deleted_articles:,} articles")

    # --- delete orphaned related nodes --------------------------------------
    print(f"\n{'[DRY RUN] Counting' if dry_run else 'Removing'} orphaned related nodes ...")
    orphan_counts = _delete_orphans(dry_run)

    for label, count in orphan_counts.items():
        print(f"  {label:<15} : "
              f"{'would remove' if dry_run else 'removed'} {count:,}")

    print("\nDone.")


if __name__ == "__main__":
    main()
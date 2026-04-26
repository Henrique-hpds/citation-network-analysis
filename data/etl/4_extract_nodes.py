"""
Collect the set of article IDs to include in the graph, then expand via BFS
from the corpus seeds until --target-size is reached.

Sources (in order):
  1. Every node that appears in any path file (--paths)
  2. Every article ID found in the corpus directories (--corpus)
  3. BFS expansion from the corpus seeds through the citation index
     until len(nodes) >= --target-size

Usage:
    python data/etl/extract_nodes.py \
        --paths           ./data/paths_top_to_unicamp.json \
                          ./data/paths_institutions_to_top.json \
        --corpus          ./data/responses_1/_unicamp_cs \
                          ./data/responses_1/_top_cited_cs \
        --target-size     100000 \
        --citation-index  ./data/citation_index.json \
        --output          ./data/graph_nodes.json
"""

import argparse
import json
import sys
from collections import deque
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _strip(url: str) -> str:
    return url.replace("https://openalex.org/", "") if url else ""


def _iter_records(path: Path):
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  [WARN] {path}: {exc}", file=sys.stderr)
        return
    records = raw if isinstance(raw, list) else raw.get("results", [raw])
    for rec in records:
        if isinstance(rec, dict) and rec.get("id"):
            yield rec


def _load_paths(path_files: list[Path]) -> set[str]:
    """Return all node IDs that appear in any path across all paths files."""
    nodes: set[str] = set()
    for p in path_files:
        if not p.exists():
            print(f"  [WARN] Paths file not found, skipping: {p}", file=sys.stderr)
            continue
        raw = json.loads(p.read_text(encoding="utf-8"))
        # Support both plain list and checkpoint format from find_path.py
        paths = raw if isinstance(raw, list) else raw.get("paths", [])
        for path in paths:
            nodes.update(path)
        print(f"  {p}: {len(paths):,} paths, {len(nodes):,} unique nodes so far")
    return nodes


def _load_corpus_ids(corpus_dirs: list[Path]) -> set[str]:
    """Return all article IDs found in the corpus directories."""
    ids: set[str] = set()
    for directory in corpus_dirs:
        if not directory.exists():
            print(f"  [WARN] Corpus directory not found, skipping: {directory}", file=sys.stderr)
            continue
        before = len(ids)
        for path in sorted(directory.rglob("*.json")):
            for rec in _iter_records(path):
                wid = _strip(rec.get("id", ""))
                if wid:
                    ids.add(wid)
        print(f"  {directory}: +{len(ids) - before:,} articles ({len(ids):,} total)")
    return ids


def _get_cited_by_count(wid: str, citation_index: dict) -> int:
    return len(citation_index.get(wid, {}).get("reverse_index", []))


# ---------------------------------------------------------------------------
# BFS expansion
# ---------------------------------------------------------------------------

def expand_to_target(
    nodes: set[str],
    seeds: set[str],
    citation_index: dict[str, dict],
    target_size: int,
    report_every: int = 10_000,
) -> set[str]:
    """
    BFS outward from `seeds` through both forward (index) and reverse
    (reverse_index) edges, adding neighbours to `nodes` until
    len(nodes) >= target_size.

    Neighbours are prioritised by cited_by_count (highest first) so the most
    connected articles are absorbed first, keeping the subgraph dense.
    """
    if len(nodes) >= target_size:
        return nodes

    print(f"\n  Starting BFS expansion from {len(seeds):,} seed(s) "
          f"(need {target_size - len(nodes):,} more nodes) ...", flush=True)

    visited = set(nodes)       # don't re-add what we already have
    frontier = deque(seeds)

    def sorted_neighbours(wid: str) -> list[str]:
        entry = citation_index.get(wid, {})
        nbrs = entry.get("index", []) + entry.get("reverse_index", [])
        # deduplicate while preserving order, then sort by connectivity
        seen, unique = set(), []
        for n in nbrs:
            if n not in seen:
                seen.add(n)
                unique.append(n)
        unique.sort(key=lambda n: _get_cited_by_count(n, citation_index), reverse=True)
        return unique

    last_report = len(nodes)

    while frontier and len(nodes) < target_size:
        wid = frontier.popleft()
        for nbr in sorted_neighbours(wid):
            if nbr not in visited:
                visited.add(nbr)
                nodes.add(nbr)
                frontier.append(nbr)
                if len(nodes) >= target_size:
                    break

        if len(nodes) - last_report >= report_every:
            print(f"  ... {len(nodes):,} nodes collected "
                  f"(frontier={len(frontier):,})", flush=True)
            last_report = len(nodes)

    return nodes


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Collect graph node IDs from paths and corpora, expanding via BFS to reach a target size."
    )
    parser.add_argument(
        "--paths", nargs="+", metavar="FILE", default=[],
        help="Path files produced by find_path.py. All nodes in any path are included.",
    )
    parser.add_argument(
        "--corpus", nargs="+", metavar="DIR", default=[],
        help="Corpus directories whose article IDs are included as seeds.",
    )
    parser.add_argument(
        "--target-size", type=int, default=0,
        help="Minimum number of nodes in the output. If paths+corpus fall short, "
             "BFS expansion from the corpus seeds is used to reach this number.",
    )
    parser.add_argument(
        "--citation-index", required=True,
        help="Citation index built by build_citation_index.py (needed for BFS expansion).",
    )
    parser.add_argument(
        "--output", required=True,
        help="Output JSON file — a list of article IDs (e.g. ./data/graph_nodes.json).",
    )
    args = parser.parse_args()

    if not args.paths and not args.corpus:
        print("[ERROR] Provide at least one of --paths or --corpus.", file=sys.stderr)
        sys.exit(1)

    # --- 1. Collect nodes from path files -----------------------------------
    nodes: set[str] = set()
    if args.paths:
        print("=== Loading path files ===")
        nodes |= _load_paths([Path(p) for p in args.paths])
        print(f"  Total from paths : {len(nodes):,}")

    # --- 2. Collect IDs from corpus directories -----------------------------
    corpus_ids: set[str] = set()
    if args.corpus:
        print("\n=== Loading corpus directories ===")
        corpus_ids = _load_corpus_ids([Path(d) for d in args.corpus])
        before = len(nodes)
        nodes |= corpus_ids
        print(f"  Total from corpus: {len(corpus_ids):,}  "
              f"(+{len(nodes) - before:,} new, {len(nodes):,} total)")

    print(f"\n  Combined node set : {len(nodes):,}")

    # --- 3. BFS expansion if target not met ---------------------------------
    if args.target_size > 0 and len(nodes) < args.target_size:
        index_path = Path(args.citation_index)
        if not index_path.exists():
            print(f"[ERROR] Citation index not found: {index_path}", file=sys.stderr)
            print("  Run build_citation_index.py first.", file=sys.stderr)
            sys.exit(1)

        print(f"\n=== Loading citation index from {index_path} ===")
        citation_index = json.loads(index_path.read_text(encoding="utf-8"))
        print(f"  {len(citation_index):,} entries loaded.")

        # Expand from corpus seeds (or all nodes if no corpus given)
        seeds = corpus_ids if corpus_ids else nodes
        nodes = expand_to_target(nodes, seeds, citation_index, args.target_size)
        print(f"\n  After expansion  : {len(nodes):,} nodes")

    elif args.target_size > 0:
        print(f"\n  Target size {args.target_size:,} already met — no expansion needed.")

    # --- 4. Save ------------------------------------------------------------
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(sorted(nodes), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n  {len(nodes):,} node IDs saved to {output_path}")
    print("\nDone.")


if __name__ == "__main__":
    main()
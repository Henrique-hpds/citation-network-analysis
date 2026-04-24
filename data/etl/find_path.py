"""
Usage:
    python data/etl/4_3_find_path.py \
        --citation-index ./data/citation_index.json \
        --from-dir       ./data/responses_1/_top_cited_cs \
        --to-dir         ./data/responses_1/_unicamp_cs \
        --output-paths   ./data/paths.json \
        --max-depth      6 \
        --min-citations  10 \
        --top-k          30
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
    """Remove the OpenAlex base URL, keeping only the ID (e.g. 'W12345')."""
    return url.replace("https://openalex.org/", "") if url else ""


def _iter_records(path: Path):
    """Yield individual article dicts from a JSON file (list or single object)."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  [WARN] {path}: {exc}", file=sys.stderr)
        return
    records = raw if isinstance(raw, list) else raw.get("results", [raw])
    for rec in records:
        if isinstance(rec, dict) and rec.get("id"):
            yield rec


def _load_seed_dir(directory: Path) -> set[str]:
    """Return the set of article IDs found in a seed directory."""
    ids = set()
    for path in sorted(directory.rglob("*.json")):
        for rec in _iter_records(path):
            wid = _strip(rec.get("id", ""))
            if wid:
                ids.add(wid)
    return ids


# ---------------------------------------------------------------------------
# Bidirectional BFS
# ---------------------------------------------------------------------------

def bidirectional_bfs(
    seeds_from: set[str],
    seeds_to: set[str],
    citation_index: dict[str, dict],
    max_depth: int,
    min_citations: int,
    top_k: int,
) -> tuple[set[str], list[list[str]]]:
    """
    Expand two frontiers simultaneously using the pre-built citation index:

      frontier_from  (starts at `from` seeds, e.g. top-cited articles)
        → uses "index" (referenced_works): follows citations downward

      frontier_to    (starts at `to` seeds, e.g. unicamp articles)
        → uses "reverse_index" (cited_by):  follows citations upward

    A path is found when the two frontiers share a common node.
    The combined path reads: from_seed → ... → meeting_node → ... → to_seed
    """

    def get_cited_by_count(wid: str) -> int:
        return citation_index.get(wid, {}).get("cited_by_count", 0)

    def neighbors_from(wid: str) -> list[str]:
        """Articles that `wid` cites — follow references downward."""
        refs = citation_index.get(wid, {}).get("index", [])
        if min_citations > 0:
            refs = [r for r in refs if get_cited_by_count(r) >= min_citations]
        if top_k > 0:
            refs.sort(key=get_cited_by_count, reverse=True)
            refs = refs[:top_k]
        return refs

    def neighbors_to(wid: str) -> list[str]:
        """Articles that cite `wid` — follow reverse index upward."""
        citers = citation_index.get(wid, {}).get("reverse_index", [])
        if min_citations > 0:
            citers = [c for c in citers if get_cited_by_count(c) >= min_citations]
        if top_k > 0:
            citers.sort(key=get_cited_by_count, reverse=True)
            citers = citers[:top_k]
        return citers

    # parent map doubles as visited set (None = seed node)
    parent_from: dict[str, str | None] = {s: None for s in seeds_from}
    parent_to:   dict[str, str | None] = {s: None for s in seeds_to}

    frontier_from = deque(seeds_from)
    frontier_to   = deque(seeds_to)
    intersections: set[str] = set()
    half = max_depth // 2

    def expand(frontier, parent_this, parent_other, get_neighbors):
        next_f = deque()
        while frontier:
            node = frontier.popleft()
            for nbr in get_neighbors(node):
                if nbr not in parent_this:
                    parent_this[nbr] = node
                    next_f.append(nbr)
                    if nbr in parent_other:
                        intersections.add(nbr)
        return next_f

    try:
        for step in range(half):
            if not frontier_from and not frontier_to:
                break

            print(
                f"  [step {step+1}/{half}]  "
                f"frontier_from={len(frontier_from):,}  "
                f"frontier_to={len(frontier_to):,}  "
                f"visited_from={len(parent_from):,}  "
                f"visited_to={len(parent_to):,}  "
                f"intersections={len(intersections):,}",
                flush=True,
            )

            if len(frontier_from) <= len(frontier_to):
                frontier_from = expand(frontier_from, parent_from, parent_to,   neighbors_from)
            else:
                frontier_to   = expand(frontier_to,   parent_to,   parent_from, neighbors_to)

            intersections |= set(parent_from) & set(parent_to)

    except KeyboardInterrupt:
        print("\nStopping early...")

    finally:
        intersections |= set(parent_from) & set(parent_to)
        all_nodes = set(parent_from) | set(parent_to)
        print(
            f"\n  [BFS done]  visited={len(all_nodes):,}  "
            f"intersections={len(intersections):,}"
        )

    # --- path reconstruction ------------------------------------------------

    def trace(node, parent_map) -> list[str]:
        path, cur = [], node
        while cur is not None:
            path.append(cur)
            cur = parent_map.get(cur)
        return list(reversed(path))

    paths = []
    for mid in intersections:
        side_from = trace(mid, parent_from)          # [from_seed, ..., mid]
        side_to   = trace(mid, parent_to)            # [to_seed,   ..., mid]
        combined  = side_from + list(reversed(side_to))[1:]  # drop duplicate mid
        paths.append(combined)

    return all_nodes, paths


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Find citation paths between two sets of articles using a pre-built citation index."
    )
    parser.add_argument("--citation-index", required=True,
                        help="Path to the citation index built by build_citation_index.py")
    parser.add_argument("--from-dir", required=True,
                        help="Directory of start seed articles (e.g. top-cited)")
    parser.add_argument("--to-dir", required=True,
                        help="Directory of end seed articles (e.g. unicamp)")
    parser.add_argument("--output-paths", default="paths.json",
                        help="Where to save discovered paths (default: paths.json)")
    parser.add_argument("--max-depth", type=int, default=6,
                        help="Max BFS depth total, split evenly between both sides (default: 6)")
    parser.add_argument("--min-citations", type=int, default=10,
                        help="Min cited_by_count to follow a neighbour (default: 10)")
    parser.add_argument("--top-k", type=int, default=30,
                        help="Max neighbours per node, highest-cited first (default: 30)")
    args = parser.parse_args()

    # --- load citation index ------------------------------------------------
    index_path = Path(args.citation_index)
    if not index_path.exists():
        print(f"[ERROR] Citation index not found: {index_path}", file=sys.stderr)
        print("  Run build_citation_index.py first.", file=sys.stderr)
        sys.exit(1)

    print(f"=== Loading citation index from {index_path} ===")
    citation_index = json.loads(index_path.read_text(encoding="utf-8"))
    print(f"  {len(citation_index):,} entries loaded.")

    # --- load seed sets -----------------------------------------------------
    print("\n=== Loading seed directories ===")
    seeds_from = _load_seed_dir(Path(args.from_dir))
    print(f"  From : {len(seeds_from):,} articles  ({args.from_dir})")
    seeds_to = _load_seed_dir(Path(args.to_dir))
    print(f"  To   : {len(seeds_to):,} articles  ({args.to_dir})")

    if not seeds_from:
        print("[ERROR] --from-dir contains no recognisable articles.", file=sys.stderr)
        sys.exit(1)
    if not seeds_to:
        print("[ERROR] --to-dir contains no recognisable articles.", file=sys.stderr)
        sys.exit(1)

    # --- bidirectional BFS --------------------------------------------------
    print(
        f"\n=== Bidirectional BFS "
        f"(max_depth={args.max_depth}, min_citations={args.min_citations}, "
        f"top_k={args.top_k}) ==="
    )
    all_nodes, paths = bidirectional_bfs(
        seeds_from=seeds_from,
        seeds_to=seeds_to,
        citation_index=citation_index,
        max_depth=args.max_depth,
        min_citations=args.min_citations,
        top_k=args.top_k,
    )

    print(f"\n  Total nodes visited : {len(all_nodes):,}")
    print(f"  Total paths found   : {len(paths):,}")
    if paths:
        lens = sorted(len(p) for p in paths)
        print(f"  Avg path length     : {sum(lens)/len(lens):.1f}")
        print(f"  Min / Max           : {lens[0]} / {lens[-1]}")

    # --- save paths ---------------------------------------------------------
    paths_path = Path(args.output_paths)
    paths_path.parent.mkdir(parents=True, exist_ok=True)
    paths_path.write_text(json.dumps(paths, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n  Paths saved to {paths_path}")
    print("\nDone.")


if __name__ == "__main__":
    main()
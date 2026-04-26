"""
Usage:
    python data/etl/find_path.py \
        --citation-index ./data/citation_index.json \
        --from-dirs      ./data/responses_1/_top_cited_cs ./data/responses_1/_top_cited_other \
        --to-dirs        ./data/responses_1/_unicamp_cs \
        --output-paths   ./data/paths.json \
        --max-depth      6 \
        --min-citations  10 \
        --top-k          50
 
    # Resume a previous run:
    python data/etl/find_path.py \
        --citation-index ./data/citation_index.json \
        --from-dirs      ./data/responses_1/_top_cited_cs \
        --to-dirs        ./data/responses_1/_unicamp_cs \
        --output-paths   ./data/paths.json \
        --use-checkpoint \
        --max-depth      6
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
 
def _filename_strip(path: Path) -> str:
    """Remove the file extension from a Path, returning the stem (e.g. 'W12345')."""
    return path.stem if path.stem else ""
 
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
 
 
def _load_seed_dirs(directories: list[Path]) -> set[str]:
    """
    Return the set of distinct article IDs found across all seed directories.
    An article appearing in more than one directory is deduplicated by ID.
 
    For each directory:
      - Seed WIDs are derived from the filenames of *.json files (e.g.
        'W12345.json' -> 'W12345') without opening them.
      - If a .checkpoint.json exists at the directory root, its `ids_done`
        list is also added (these are the articles fetched for those seeds).
      - If no checkpoint exists, the JSON files are opened and parsed
        normally via _iter_records as a fallback.
    """
    ids: set[str] = set()
 
    for directory in directories:
        if not directory.exists():
            print(f"  [WARN] Seed directory not found, skipping: {directory}", file=sys.stderr)
            continue
 
        before     = len(ids)
        checkpoint = directory / ".checkpoint.json"
 
        if checkpoint.exists():
            # Derive seed WIDs from filenames alone — no article files opened
            for path in sorted(directory.rglob("*.json")):
                if path.name == ".checkpoint.json":
                    continue
                if path.stem:
                    ids.add(path.stem)  # 'W12345.json' -> 'W12345'
 
            # Add ids_done from the checkpoint
            try:
                data = json.loads(checkpoint.read_text(encoding="utf-8"))
                for wid in data.get("ids_done", []):
                    if wid:
                        ids.add(wid)
            except (json.JSONDecodeError, OSError) as exc:
                print(f"  [WARN] Could not read checkpoint {checkpoint}: {exc}", file=sys.stderr)
 
            print(f"  {directory}: filenames + checkpoint, "
                  f"+{len(ids) - before:,} IDs ({len(ids):,} total)")
 
        else:
            # No checkpoint — open each article file to extract the ID
            for path in sorted(directory.rglob("*.json")):
                wid = _filename_strip(path.stem)  # 'W12345.json' -> 'W12345'
                if wid:
                    ids.add(wid)
 
            print(f"  {directory}: scanned JSON files, "
                  f"+{len(ids) - before:,} IDs ({len(ids):,} total)")
 
    return ids
 
# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------
 
def _load_checkpoint(path: Path) -> tuple[
    dict[str, str | None],   # parent
    set[str],                # reached_to
]:
    """Load BFS state saved by a previous run. Returns (parent, reached_to)."""
    raw   = json.loads(path.read_text(encoding="utf-8"))
    state = raw.get("_bfs_state", {})
    parent     = {k: v for k, v in state.get("parent", {}).items()}
    reached_to = set(state.get("reached_to", []))
    return parent, reached_to
 
 
def _save_checkpoint(
    output_path: Path,
    paths:      list[list[str]],
    parent:     dict[str, str | None],
    reached_to: set[str],
) -> None:
    """Write discovered paths plus BFS state so the run can be resumed."""
    data = {
        "_bfs_state": {
            "parent":     parent,
            "reached_to": list(reached_to),
        },
        "paths": paths,
    }
    output_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


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
    checkpoint: tuple[dict, dict, set] | None = None,
) -> tuple[set[str], list[list[str]]]:
    """
    Expand two frontiers simultaneously using the pre-built citation index:

      frontier_from  (starts at `from` seeds, e.g. top-cited articles)
        → uses "index" (referenced_works): follows citations downward

      frontier_to    (starts at `to` seeds, e.g. unicamp articles)
        → uses "reverse_index" (cited_by):  follows citations upward

    A path is found when the two frontiers share a common node.
    The combined path reads: from_seed → ... → meeting_node → ... → to_seed

    If `checkpoint` is provided as (parent_from, parent_to, intersections),
    the BFS resumes from those parent maps rather than starting fresh.
    The frontier for each side is reconstructed as the set of nodes whose
    parent is known but whose neighbours have not yet been expanded — i.e.
    nodes present as values in the parent map but not yet as keys.
    """

    def get_cited_by_count(wid: str) -> int:
        return len(citation_index.get(wid, {}).get("reverse_index", 0))

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

    # --- initialise or restore state ----------------------------------------

    if checkpoint:
        parent_from, parent_to, intersections = checkpoint
        print(f"  Resuming from checkpoint: "
              f"visited_from={len(parent_from):,}  visited_to={len(parent_to):,}  "
              f"intersections={len(intersections):,}")

        # Frontier = nodes discovered (appear as values) but not yet expanded
        # (do not appear as keys) on each side.
        discovered_from = set(parent_from.values()) - {None}
        discovered_to   = set(parent_to.values())   - {None}
        frontier_from = deque(discovered_from - set(parent_from))
        frontier_to   = deque(discovered_to   - set(parent_to))
    else:
        # parent map doubles as visited set (None = seed node)
        parent_from: dict[str, str | None] = {s: None for s in seeds_from}
        parent_to:   dict[str, str | None] = {s: None for s in seeds_to}
        intersections: set[str] = set()
        frontier_from = deque(seeds_from)
        frontier_to   = deque(seeds_to)

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

    # --- main loop ----------------------------------------------------------

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
        side_from = trace(mid, parent_from)
        side_to   = trace(mid, parent_to)
        combined  = side_from + list(reversed(side_to))[1:]
        paths.append(combined)

    return all_nodes, paths, parent_from, parent_to, intersections

# ---------------------------------------------------------------------------
# Forward BFS
# ---------------------------------------------------------------------------
 
def forward_bfs(
    seeds_from: set[str],
    seeds_to:   set[str],
    citation_index: dict[str, dict],
    max_depth:     int,
    min_citations: int,
    top_k:         int,
    checkpoint: tuple[dict, set] | None = None,
) -> tuple[set[str], list[list[str]], dict, set]:
    """
    Standard BFS from `seeds_from`, expanding via "index" (referenced_works),
    stopping when a node in `seeds_to` is reached or `max_depth` is exhausted.
 
    Uses a (node, depth) frontier so nodes are never re-expanded beyond their
    discovery depth. All paths to every reachable `to` seed are reconstructed
    via parent pointers.
 
    If `checkpoint` is provided as (parent, reached_to), the BFS resumes from
    the previously saved state. The frontier is rebuilt from all nodes that were
    discovered but cannot be guaranteed expanded, so some edges may be
    re-checked — this is harmless since the parent guard prevents re-adding.
    """
 
    def get_cited_by_count(wid: str) -> int:
        return len(citation_index.get(wid, {}).get("reverse_index", []))
 
    def neighbors(wid: str) -> list[str]:
        """Articles that `wid` cites — follow forward references."""
        refs = list(citation_index.get(wid, {}).get("index", []))
        if min_citations > 0:
            refs = [r for r in refs if get_cited_by_count(r) >= min_citations]
        if top_k > 0:
            refs.sort(key=get_cited_by_count, reverse=True)
            refs = refs[:top_k]
        return refs
 
    # --- initialise or restore state ----------------------------------------
 
    if checkpoint:
        parent, reached_to = checkpoint
        reached_to = set(reached_to)
        print(f"  Resuming from checkpoint: "
              f"visited={len(parent):,}  reached_to={len(reached_to):,}")
        # Conservatively restart frontier from all discovered nodes;
        # the parent guard prevents any node from being re-added.
        frontier: deque[tuple[str, int]] = deque((n, 0) for n in parent)
    else:
        parent:     dict[str, str | None] = {s: None for s in seeds_from}
        reached_to: set[str]              = seeds_from & seeds_to  # direct overlap
        frontier:   deque[tuple[str, int]] = deque((s, 0) for s in seeds_from)
 
    # --- main loop ----------------------------------------------------------
 
    try:
        last_depth = -1
 
        while frontier:
            node, depth = frontier.popleft()
 
            if depth >= max_depth:
                continue
 
            if depth != last_depth:
                last_depth = depth
                print(
                    f"  [depth {depth+1}/{max_depth}]  "
                    f"frontier={len(frontier):,}  "
                    f"visited={len(parent):,}  "
                    f"reached_to={len(reached_to):,}",
                    flush=True,
                )
 
            for nbr in neighbors(node):
                if nbr not in parent:
                    parent[nbr] = node
                    if nbr in seeds_to:
                        reached_to.add(nbr)
                    frontier.append((nbr, depth + 1))
 
    except KeyboardInterrupt:
        print("\n  Stopping early (KeyboardInterrupt)...")
 
    finally:
        print(
            f"\n  [BFS done]  visited={len(parent):,}  "
            f"reached_to={len(reached_to):,}"
        )
 
    # --- path reconstruction ------------------------------------------------
 
    def trace(node: str) -> list[str]:
        path, cur = [], node
        while cur is not None:
            path.append(cur)
            cur = parent.get(cur)
        return list(reversed(path))
 
    paths = [trace(t) for t in reached_to]
 
    return set(parent.keys()), paths, parent, reached_to
 
def filter_paths_to_seeds(
    paths:      list[list[str]],
    seeds_from: set[str],
    seeds_to:   set[str],
) -> None:
    """
    Remove in-place any path that does not start in seeds_from
    and end in seeds_to.
    """
    paths[:] = [
        p for p in paths
        if p and p[0] in seeds_from and p[-1] in seeds_to
    ]
 
# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
 
def main():
    parser = argparse.ArgumentParser(
        description="Find citation paths between two sets of articles using a pre-built citation index."
    )
    parser.add_argument("--citation-index", required=True,
                        help="Path to the citation index built by build_citation_index.py")
    parser.add_argument("--from-dirs", required=True, nargs="+", metavar="DIR",
                        help="One or more directories of start seed articles (e.g. top-cited). "
                             "Duplicates across directories are deduplicated by article ID.")
    parser.add_argument("--to-dirs", required=True, nargs="+", metavar="DIR",
                        help="One or more directories of end seed articles (e.g. unicamp). "
                             "Duplicates across directories are deduplicated by article ID.")
    parser.add_argument("--output-paths", default="paths.json",
                        help="Where to save discovered paths (default: paths.json)")
    parser.add_argument("--max-depth", type=int, default=6,
                        help="Max BFS depth total, split evenly between both sides (default: 6)")
    parser.add_argument("--min-citations", type=int, default=10,
                        help="Min cited_by_count to follow a neighbour (default: 10)")
    parser.add_argument("--top-k", type=int, default=30,
                        help="Max neighbours per node, highest-cited first (default: 30)")
    parser.add_argument("--use-checkpoint", action="store_true",
                        help="Resume from the BFS state saved in --output-paths if it exists.")
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
    seeds_from = _load_seed_dirs([Path(d) for d in args.from_dirs])
    print(f"  From : {len(seeds_from):,} distinct articles across {len(args.from_dirs)} dir(s)")
    seeds_to = _load_seed_dirs([Path(d) for d in args.to_dirs])
    print(f"  To   : {len(seeds_to):,} distinct articles across {len(args.to_dirs)} dir(s)")
 
    if not seeds_from:
        print("[ERROR] --from-dirs contains no recognisable articles.", file=sys.stderr)
        sys.exit(1)
    if not seeds_to:
        print("[ERROR] --to-dirs contains no recognisable articles.", file=sys.stderr)
        sys.exit(1)
 
    # --- load checkpoint if requested ---------------------------------------
    checkpoint = None
    paths_path = Path(args.output_paths)
 
    if args.use_checkpoint:
        if paths_path.exists():
            print(f"\n=== Loading BFS checkpoint from {paths_path} ===")
            try:
                checkpoint = _load_checkpoint(paths_path)
                print(f"  Checkpoint loaded.")
            except (KeyError, json.JSONDecodeError) as exc:
                print(f"  [WARN] Could not parse checkpoint ({exc}), starting fresh.",
                      file=sys.stderr)
                checkpoint = None
        else:
            print(f"\n  [WARN] --use-checkpoint set but {paths_path} not found — starting fresh.",
                  file=sys.stderr)
 
    # --- forward BFS --------------------------------------------------------
    print(
        f"\n=== Forward BFS "
        f"(max_depth={args.max_depth}, min_citations={args.min_citations}, "
        f"top_k={args.top_k}) ==="
    )
    all_nodes, paths, parent, reached_to = forward_bfs(
        seeds_from=seeds_from,
        seeds_to=seeds_to,
        citation_index=citation_index,
        max_depth=args.max_depth,
        min_citations=args.min_citations,
        top_k=args.top_k,
        checkpoint=checkpoint,
    )
 
    print(f"\n  Total nodes visited : {len(all_nodes):,}")
    print(f"  Total paths found   : {len(paths):,}")
    if paths:
        lens = sorted(len(p) for p in paths)
        print(f"  Avg path length     : {sum(lens)/len(lens):.1f}")
        print(f"  Min / Max           : {lens[0]} / {lens[-1]}")

        filter_paths_to_seeds(paths, seeds_from, seeds_to)
        print(f"\n  Paths starting in seeds_from and ending in seeds_to: {len(paths):,}")
        if paths:
            lens = sorted(len(p) for p in paths)
            print(f"  Avg path length     : {sum(lens)/len(lens):.1f}")
            print(f"  Min / Max           : {lens[0]} / {lens[-1]}")
 
    # --- save paths + BFS state for future resumption -----------------------
    paths_path.parent.mkdir(parents=True, exist_ok=True)
    _save_checkpoint(paths_path, paths, parent, reached_to)
    print(f"\n  Paths + checkpoint saved to {paths_path}")
    print("\nDone.")
 
 
if __name__ == "__main__":
    main()

"""
4_build_graph.py

Builds a thematically-coherent citation subgraph connecting:
  - "seed_ic"        : articles from IC/Unicamp researchers (openalex_cs/)
  - "seed_top"       : high-impact CS articles (top_cited_cs/)

Strategy (as recommended in especif.md):
  1. Collect seed IDs from both corpora; tag each as IC or top-tier.
  2. Compute a shared concept vocabulary from both seed sets.
  3. Bidirectional BFS within the already-downloaded corpus first
     (no extra API calls), then optionally expand via OpenAlex for
     neighbours not yet on disk (requires 3_fetch_neighbors.py).
  4. Write the subgraph (nodes + edges) as flat JSON ready for 2_load.py,
     and also write Neo4j Cypher to mark seed articles with flags
     `is_from_ic` and `is_high_impact`.

Usage:
    python 4_build_graph.py \\
        --ic-responses   ../../data/responses/openalex_cs \\
        --top-responses  ../../data/responses/top_cited_cs \\
        --output         ../../data/flat_graph \\
        --max-depth      4 \\
        --min-citations  10 \\
        --top-k-neighbors 50

Output written to --output/:
    articles.json        — Article nodes
    authors.json         — Author nodes
    institutions.json    — Institution nodes
    venues.json          — Venue nodes
    subfields.json       — Subfield nodes
    relationships.json   — All relationships (CITES, AUTHORED_BY, …)
    seed_tags.cypher     — Cypher to apply is_from_ic / is_high_impact flags
"""

import argparse
import json
import sys
from collections import defaultdict, deque
from pathlib import Path

# ---------------------------------------------------------------------------
# Shared extraction helpers (mirror of 1_flatten.py)
# ---------------------------------------------------------------------------

def _strip(url: str) -> str:
    return url.replace("https://openalex.org/", "") if url else ""


def _article(raw: dict) -> dict:
    return {
        "openalex_id":      _strip(raw.get("id", "")),
        "doi":              raw.get("doi"),
        "title":            raw.get("display_name"),
        "publication_year": raw.get("publication_year"),
        "type":             raw.get("type"),
        "cited_by_count":   raw.get("cited_by_count", 0),
        "is_retracted":     raw.get("is_retracted", False),
    }


def _authors(raw: dict) -> list[dict]:
    out = []
    for a in raw.get("authorships", []):
        author = a.get("author") or {}
        aid = _strip(author.get("id", ""))
        if not aid:
            continue
        out.append({
            "openalex_id":  aid,
            "display_name": author.get("display_name"),
            "orcid":        author.get("orcid"),
        })
    return out


def _institutions(raw: dict) -> list[dict]:
    seen, out = set(), []
    for a in raw.get("authorships", []):
        for inst in a.get("institutions", []):
            iid = _strip(inst.get("id", ""))
            if not iid or iid in seen:
                continue
            seen.add(iid)
            out.append({
                "openalex_id":  iid,
                "display_name": inst.get("display_name"),
                "ror":          inst.get("ror"),
                "country_code": inst.get("country_code"),
                "type":         inst.get("type"),
            })
    return out


def _venue(raw: dict) -> dict | None:
    loc    = raw.get("primary_location") or {}
    source = loc.get("source") or {}
    sid    = _strip(source.get("id", ""))
    if not sid:
        return None
    return {
        "openalex_id":  sid,
        "display_name": source.get("display_name"),
        "issn_l":       source.get("issn_l"),
        "type":         source.get("type"),
    }


def _subfields(raw: dict) -> list[dict]:
    seen, out = set(), []
    for t in raw.get("topics", []):
        sf  = t.get("subfield") or {}
        sid = _strip(sf.get("id", ""))
        if not sid or sid in seen:
            continue
        seen.add(sid)
        field = t.get("field") or {}
        out.append({
            "openalex_id":  sid,
            "display_name": sf.get("display_name"),
            "field_id":     _strip(field.get("id", "")),
            "field_name":   field.get("display_name"),
        })
    return out


def _relationships(raw: dict) -> dict:
    article_id = _strip(raw.get("id", ""))

    authored_by = []
    for a in raw.get("authorships", []):
        author  = a.get("author") or {}
        aid     = _strip(author.get("id", ""))
        if not aid:
            continue
        inst_ids = [_strip(i.get("id", "")) for i in a.get("institutions", []) if i.get("id")]
        authored_by.append({
            "author_id":        aid,
            "author_position":  a.get("author_position"),
            "is_corresponding": a.get("is_corresponding", False),
            "institution_ids":  inst_ids,
            "countries":        a.get("countries", []),
        })

    loc     = raw.get("primary_location") or {}
    source  = loc.get("source") or {}
    venue_id = _strip(source.get("id", "")) or None

    seen_sf, subfield_ids = set(), []
    for t in raw.get("topics", []):
        sf  = t.get("subfield") or {}
        sid = _strip(sf.get("id", ""))
        if sid and sid not in seen_sf:
            seen_sf.add(sid)
            subfield_ids.append(sid)

    cited_works = [_strip(w) for w in raw.get("referenced_works", [])]

    return {
        "article_id":   article_id,
        "authored_by":  authored_by,
        "venue_id":     venue_id,
        "subfield_ids": subfield_ids,
        "cited_works":  cited_works,
    }


def _concept_ids(raw: dict) -> set[str]:
    """Return the set of subfield IDs (used as thematic fingerprint)."""
    ids = set()
    for t in raw.get("topics", []):
        sf  = t.get("subfield") or {}
        sid = _strip(sf.get("id", ""))
        if sid:
            ids.add(sid)
    return ids


def _institution_ids(raw: dict) -> set[str]:
    ids = set()
    for a in raw.get("authorships", []):
        for inst in a.get("institutions", []):
            iid = _strip(inst.get("id", ""))
            if iid:
                ids.add(iid)
    return ids


# ---------------------------------------------------------------------------
# Corpus loader
# ---------------------------------------------------------------------------

def load_corpus(directory: Path) -> dict[str, dict]:
    """
    Load all .json files from a directory (or nested dirs of .json lists).
    Returns {openalex_id: raw_record}.
    """
    corpus: dict[str, dict] = {}

    for path in sorted(directory.rglob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            print(f"  [WARN] Skipping {path}: {exc}", file=sys.stderr)
            continue

        # Files may be a single record or a list (year_XXXX.json style)
        records = data.get("results", [data]) if isinstance(data, dict) else data

        for rec in records:
            if not isinstance(rec, dict):
                continue
            wid = _strip(rec.get("id", ""))
            if wid:
                corpus[wid] = rec

    return corpus


# ---------------------------------------------------------------------------
# Thematic similarity
# ---------------------------------------------------------------------------

def jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


# ---------------------------------------------------------------------------
# BFS expansion
# ---------------------------------------------------------------------------

def bidirectional_bfs(
    seeds_a: set[str],
    seeds_b: set[str],
    neighbors_fn,           # callable: id -> list[str]
    max_depth: int,
    verbose: bool = True,
) -> set[str]:
    """
    Bidirectional BFS between two seed sets.
    Returns the set of all node IDs in the discovered subgraph.
    Expands the smaller frontier first.
    """
    visited_a: dict[str, int] = {s: 0 for s in seeds_a}  # id -> depth
    visited_b: dict[str, int] = {s: 0 for s in seeds_b}

    frontier_a: deque[str] = deque(seeds_a)
    frontier_b: deque[str] = deque(seeds_b)

    half = max_depth // 2

    def expand(frontier, visited, depth_limit) -> deque:
        next_frontier: deque[str] = deque()
        while frontier:
            node = frontier.popleft()
            current_depth = visited[node]
            if current_depth >= depth_limit:
                continue
            for nbr in neighbors_fn(node):
                if nbr not in visited:
                    visited[nbr] = current_depth + 1
                    next_frontier.append(nbr)
        return next_frontier

    for step in range(half):
        if not frontier_a and not frontier_b:
            break

        # Expand smaller frontier first
        if len(frontier_a) <= len(frontier_b):
            frontier_a = expand(frontier_a, visited_a, half)
        else:
            frontier_b = expand(frontier_b, visited_b, half)

        intersection = set(visited_a) & set(visited_b)
        if intersection and verbose:
            print(f"  [BFS step {step+1}] intersection size: {len(intersection):,}")

    all_nodes = set(visited_a) | set(visited_b)
    if verbose:
        print(f"  [BFS] total nodes in subgraph: {len(all_nodes):,}")
    return all_nodes


# ---------------------------------------------------------------------------
# Graph assembly
# ---------------------------------------------------------------------------

def build_subgraph(
    selected_ids: set[str],
    corpus: dict[str, dict],
) -> dict:
    """
    Given a set of selected article IDs and the full corpus, extract all
    node and relationship records.  Edges are kept only if *both* endpoints
    are in selected_ids.
    """
    articles      = {}
    authors       = {}
    institutions  = {}
    venues        = {}
    subfields_map = {}
    relationships = []

    for wid in selected_ids:
        raw = corpus.get(wid)
        if raw is None:
            continue

        art = _article(raw)
        articles[art["openalex_id"]] = art

        for au in _authors(raw):
            authors[au["openalex_id"]] = au

        for inst in _institutions(raw):
            institutions[inst["openalex_id"]] = inst

        v = _venue(raw)
        if v:
            venues[v["openalex_id"]] = v

        for sf in _subfields(raw):
            subfields_map[sf["openalex_id"]] = sf

        rel = _relationships(raw)
        # Restrict CITES to within-subgraph edges
        rel["cited_works"] = [cw for cw in rel["cited_works"] if cw in selected_ids]
        relationships.append(rel)

    return {
        "articles":      list(articles.values()),
        "authors":       list(authors.values()),
        "institutions":  list(institutions.values()),
        "venues":        list(venues.values()),
        "subfields":     list(subfields_map.values()),
        "relationships": relationships,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Bidirectional BFS graph builder for citation network.")
    parser.add_argument("--unicamp-responses", required=True, help="Dir of Unicamp raw OpenAlex JSONs (from 0_download_unicamp.py)")
    parser.add_argument("--top-responses",     required=True, help="Dir of top-cited raw OpenAlex JSONs")
    parser.add_argument("--output",            required=True, help="Output directory for flat JSON files")
    parser.add_argument("--max-depth",         type=int, default=4,
                        help="Max BFS depth from each seed set (default: 4)")
    parser.add_argument("--min-citations",     type=int, default=10,
                        help="Minimum cited_by_count to follow a neighbour (default: 10)")
    parser.add_argument("--top-k-neighbors",   type=int, default=50,
                        help="Max neighbours to follow per node (default: 50, highest cited first)")
    parser.add_argument("--min-jaccard",       type=float, default=0.0,
                        help="Minimum topic Jaccard similarity to seed vocabulary (default: 0.0 = disabled)")
    args = parser.parse_args()

    ic_dir  = Path(args.unicamp_responses)
    top_dir = Path(args.top_responses)
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- 1. Load corpora ---
    print("Loading IC corpus...")
    ic_corpus = load_corpus(ic_dir)
    print(f"  {len(ic_corpus):,} IC articles loaded.")

    print("Loading top-cited corpus...")
    top_corpus = load_corpus(top_dir)
    print(f"  {len(top_corpus):,} top-cited articles loaded.")

    corpus = {**ic_corpus, **top_corpus}
    print(f"  {len(corpus):,} total unique articles in combined corpus.")

    seeds_ic  = set(ic_corpus.keys())
    seeds_top = set(top_corpus.keys())

    # --- 2. Build shared concept vocabulary ---
    print("Computing shared concept vocabulary...")
    vocab: set[str] = set()
    for wid in seeds_ic | seeds_top:
        vocab |= _concept_ids(corpus[wid])
    print(f"  {len(vocab):,} unique subfield IDs in seed vocabulary.")

    # --- 3. Build citation index (referenced_works) ---
    # cited_by index: article_id -> list of articles that cite it
    citing_index: dict[str, list[str]] = defaultdict(list)
    for wid, raw in corpus.items():
        for ref in raw.get("referenced_works", []):
            ref_id = _strip(ref)
            if ref_id:
                citing_index[ref_id].append(wid)

    def neighbors(wid: str) -> list[str]:
        """
        Return candidate neighbours for BFS expansion:
        - articles cited by wid  (outgoing CITES)
        - articles that cite wid (incoming CITES)
        Filtered by min_citations and top-k, restricted to known corpus.
        """
        raw = corpus.get(wid)
        if raw is None:
            return []

        # Outgoing: what this article references
        outgoing = [
            _strip(r) for r in raw.get("referenced_works", [])
            if _strip(r) in corpus
        ]
        # Incoming: what cites this article (already in corpus)
        incoming = citing_index.get(wid, [])

        candidates = list(dict.fromkeys(outgoing + incoming))  # deduplicate, preserve order

        # Filter by citation count
        if args.min_citations > 0:
            candidates = [
                c for c in candidates
                if corpus.get(c, {}).get("cited_by_count", 0) >= args.min_citations
            ]

        # Filter by thematic overlap
        if args.min_jaccard > 0:
            candidates = [
                c for c in candidates
                if jaccard(_concept_ids(corpus.get(c, {})), vocab) >= args.min_jaccard
            ]

        # Keep top-k by citation count
        if args.top_k_neighbors > 0:
            candidates.sort(
                key=lambda c: corpus.get(c, {}).get("cited_by_count", 0),
                reverse=True,
            )
            candidates = candidates[:args.top_k_neighbors]

        return candidates

    # --- 4. Bidirectional BFS ---
    print(f"Running bidirectional BFS (max_depth={args.max_depth})...")
    subgraph_ids = bidirectional_bfs(
        seeds_a=seeds_ic,
        seeds_b=seeds_top,
        neighbors_fn=neighbors,
        max_depth=args.max_depth,
    )

    # --- 5. Assemble subgraph ---
    print("Assembling subgraph records...")
    graph = build_subgraph(subgraph_ids, corpus)

    for entity, records in graph.items():
        subdir = out_dir / entity
        subdir.mkdir(exist_ok=True)
        out_path = subdir / f"{entity}_0000.json"
        out_path.write_text(json.dumps(records, ensure_ascii=False, indent=None), encoding="utf-8")
        print(f"  {entity:<15} {len(records):>8,} records → {out_path.relative_to(out_dir)}")

    # --- 6. Write seed-tagging Cypher ---
    cypher_lines = [
        "// Apply seed flags to Article nodes",
        "// is_from_unicamp : article downloaded from Unicamp seed corpus",
        "// is_high_impact  : article was in the top-cited seed corpus",
        "",
        "// Mark Unicamp articles",
        "MATCH (a:Article)",
        "WHERE a.openalex_id IN [",
    ]
    cypher_lines += [f'  "{wid}",' for wid in sorted(seeds_ic)]
    cypher_lines[-1] = cypher_lines[-1].rstrip(",")  # remove trailing comma
    cypher_lines += [
        "]",
        "SET a.is_from_unicamp = true;",
        "",
        "// Mark high-impact articles",
        "MATCH (a:Article)",
        "WHERE a.openalex_id IN [",
    ]
    cypher_lines += [f'  "{wid}",' for wid in sorted(seeds_top)]
    cypher_lines[-1] = cypher_lines[-1].rstrip(",")
    cypher_lines += [
        "]",
        "SET a.is_high_impact = true;",
    ]

    cypher_path = out_dir / "seed_tags.cypher"
    cypher_path.write_text("\n".join(cypher_lines), encoding="utf-8")
    print(f"  seed_tags.cypher written ({len(seeds_ic):,} Unicamp + {len(seeds_top):,} top seeds)")

    print("\nDone. Load with:")
    print(f"  python 2_load.py --input {out_dir}")
    print(f"  Then apply: cypher-shell < {cypher_path}")


if __name__ == "__main__":
    main()

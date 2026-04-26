"""
1_flatten_subgraph.py

Like 1_flatten.py, but only processes articles whose IDs appear in a
nodes file (produced by extract_nodes.py).  Files are located via the
"path" field stored in the citation index, so the full corpus does not
need to be scanned — only the relevant files are read.

Usage:
    python ./data/etl/1_flatten_subgraph.py \
        --nodes          ./data/graph_nodes.json \
        --citation-index ./data/citation_index.json \
        --input          ./data/responses_1 \
        --output         ./data/flat_subgraph \
        --batch-size     5000
"""

import argparse
import json
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Extraction helpers  (identical to 1_flatten.py)
# ---------------------------------------------------------------------------

def _strip(url: str) -> str:
    return url.replace("https://openalex.org/", "") if url else ""


def extract_article(raw: dict) -> dict:
    return {
        "openalex_id":      _strip(raw.get("id", "")),
        "doi":              raw.get("doi"),
        "title":            raw.get("display_name"),
        "publication_year": raw.get("publication_year"),
        "type":             raw.get("type"),
        "cited_by_count":   raw.get("cited_by_count", 0),
        "is_retracted":     raw.get("is_retracted", False),
    }


def extract_authors(raw: dict) -> list[dict]:
    authors = []
    for a in raw.get("authorships", []):
        author = a.get("author") or {}
        aid = author.get("id", "")
        if not aid:
            continue
        authors.append({
            "openalex_id":  _strip(aid),
            "display_name": author.get("display_name"),
            "orcid":        author.get("orcid"),
        })
    return authors


def extract_institutions(raw: dict) -> list[dict]:
    seen, institutions = set(), []
    for a in raw.get("authorships", []):
        for inst in a.get("institutions", []):
            iid = inst.get("id", "")
            if not iid or iid in seen:
                continue
            seen.add(iid)
            institutions.append({
                "openalex_id":  _strip(iid),
                "display_name": inst.get("display_name"),
                "ror":          inst.get("ror"),
                "country_code": inst.get("country_code"),
                "type":         inst.get("type"),
            })
    return institutions


def extract_venue(raw: dict) -> dict | None:
    loc    = raw.get("primary_location") or {}
    source = loc.get("source") or {}
    sid    = source.get("id", "")
    if not sid:
        return None
    return {
        "openalex_id":  _strip(sid),
        "display_name": source.get("display_name"),
        "issn_l":       source.get("issn_l"),
        "type":         source.get("type"),
    }


def extract_subfields(raw: dict) -> list[dict]:
    seen, subfields = set(), []
    for t in raw.get("topics", []):
        subfield = t.get("subfield") or {}
        sid = subfield.get("id", "")
        if not sid or sid in seen:
            continue
        seen.add(sid)
        field = t.get("field") or {}
        subfields.append({
            "openalex_id":  _strip(sid),
            "display_name": subfield.get("display_name"),
            "field_id":     _strip(field.get("id", "")),
            "field_name":   field.get("display_name"),
        })
    return subfields


def extract_relationships(raw: dict, wanted_ids: set[str]) -> dict:
    article_id = _strip(raw.get("id", ""))

    authored_by = []
    for a in raw.get("authorships", []):
        author = a.get("author") or {}
        aid = author.get("id", "")
        if not aid:
            continue
        inst_ids = [_strip(i.get("id", "")) for i in a.get("institutions", []) if i.get("id")]
        authored_by.append({
            "author_id":        _strip(aid),
            "author_position":  a.get("author_position"),
            "is_corresponding": a.get("is_corresponding", False),
            "institution_ids":  inst_ids,
            "countries":        a.get("countries", []),
        })

    loc      = raw.get("primary_location") or {}
    source   = loc.get("source") or {}
    venue_id = _strip(source.get("id", "")) or None

    seen_sf, subfield_ids = set(), []
    for t in raw.get("topics", []):
        sf  = t.get("subfield") or {}
        sid = sf.get("id", "")
        if sid and sid not in seen_sf:
            seen_sf.add(sid)
            subfield_ids.append(_strip(sid))

    # Only keep CITES edges where the target is also in the subgraph
    cited_works = [
        _strip(w) for w in raw.get("referenced_works", [])
        if _strip(w) in wanted_ids
    ]

    return {
        "article_id":   article_id,
        "authored_by":  authored_by,
        "venue_id":     venue_id,
        "subfield_ids": subfield_ids,
        "cited_works":  cited_works,
    }


# ---------------------------------------------------------------------------
# Batch writer  (identical to 1_flatten.py)
# ---------------------------------------------------------------------------

class BatchWriter:
    def __init__(self, output_dir: Path, name: str, batch_size: int):
        self.output_dir = output_dir
        self.name       = name
        self.batch_size = batch_size
        self._buffer: list = []
        self._file_idx     = 0
        self._total        = 0

    def add(self, record):
        if record is None:
            return
        if isinstance(record, list):
            self._buffer.extend(record)
            self._total += len(record)
        else:
            self._buffer.append(record)
            self._total += 1
        if len(self._buffer) >= self.batch_size:
            self._flush()

    def _flush(self):
        if not self._buffer:
            return
        path = self.output_dir / f"{self.name}_{self._file_idx:04d}.json"
        path.write_text(json.dumps(self._buffer, ensure_ascii=False), encoding="utf-8")
        self._buffer   = []
        self._file_idx += 1

    def close(self) -> int:
        self._flush()
        return self._total


# ---------------------------------------------------------------------------
# File resolution
# ---------------------------------------------------------------------------

def _resolve_paths(
    wanted_ids: set[str],
    citation_index: dict[str, dict],
    input_dir: Path,
) -> dict[str, Path]:
    """
    Return {wid: absolute_path} for every wanted ID we can locate.

    Resolution order:
      1. "path" field in the citation index  (fast, avoids scanning)
      2. Fallback: derive path from ID structure  W1000057649 → W10/00/W1000057649.json
         (handles articles referenced but never directly scanned)

    Articles with no resolvable path are reported as missing.
    """
    resolved: dict[str, Path] = {}
    missing: list[str] = []
    misses = 0

    for wid in wanted_ids:
        entry = citation_index.get(wid, {})
        stored = entry.get("path")

        if stored:
            p = Path(stored)
            if not p.is_absolute():
                p = input_dir / p
            if p.exists():
                resolved[wid] = p
                continue
        else:
            misses += 1

        # Fallback: derive from ID  (W + digits → W<d0d1>/<d2d3>/W....json)
        digits = wid[1:]
        sub1   = "W" + digits[:2]
        sub2   = digits[2:4].zfill(2) if len(digits) >= 4 else "00"
        derived = input_dir / sub1 / sub2 / f"{wid}.json"
        if derived.exists():
            resolved[wid] = derived
            continue

        missing.append(wid)

    if missing:
        print(f"  [WARN] {len(missing):,} node(s) could not be located on disk "
              f"(referenced but never downloaded).", file=sys.stderr)
    
    print(misses, "IDs missing 'path' in citation index (may be normal if many are referenced-only).")

    return resolved


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Flatten a subgraph: only process articles listed in a nodes file."
    )
    parser.add_argument("--nodes",          required=True,
                        help="JSON file produced by extract_nodes.py (list of article IDs).")
    parser.add_argument("--citation-index", required=True,
                        help="Citation index from build_citation_index.py (used to locate files).")
    parser.add_argument("--input",          required=True,
                        help="Root corpus directory (used as base for relative paths and fallback lookup).")
    parser.add_argument("--output",         required=True,
                        help="Directory to write flattened batch files.")
    parser.add_argument("--batch-size",     type=int, default=5000,
                        help="Records per output file (default: 5000).")
    args = parser.parse_args()

    input_dir  = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    for subdir in ("articles", "authors", "institutions", "venues", "subfields", "relationships"):
        (output_dir / subdir).mkdir(exist_ok=True)

    # --- load node list -----------------------------------------------------
    print(f"=== Loading node list from {args.nodes} ===")
    wanted_ids: set[str] = set(json.loads(Path(args.nodes).read_text(encoding="utf-8")))
    print(f"  {len(wanted_ids):,} nodes to process.")

    # --- load citation index (for file paths) -------------------------------
    print(f"\n=== Loading citation index from {args.citation_index} ===")
    citation_index = json.loads(Path(args.citation_index).read_text(encoding="utf-8"))
    print(f"  {len(citation_index):,} entries loaded.")

    # --- resolve file paths -------------------------------------------------
    print(f"\n=== Resolving file paths ===")
    id_to_path = _resolve_paths(wanted_ids, citation_index, input_dir)
    print(f"  {len(id_to_path):,} / {len(wanted_ids):,} nodes located on disk.")

    # --- process files ------------------------------------------------------
    writers = {
        "articles":      BatchWriter(output_dir / "articles",      "articles",      args.batch_size),
        "authors":       BatchWriter(output_dir / "authors",        "authors",       args.batch_size),
        "institutions":  BatchWriter(output_dir / "institutions",   "institutions",  args.batch_size),
        "venues":        BatchWriter(output_dir / "venues",         "venues",        args.batch_size),
        "subfields":     BatchWriter(output_dir / "subfields",      "subfields",     args.batch_size),
        "relationships": BatchWriter(output_dir / "relationships",  "relationships", args.batch_size),
    }

    print(f"\n=== Flattening {len(id_to_path):,} articles ===")
    for i, (wid, path) in enumerate(id_to_path.items(), 1):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            print(f"  [WARN] Skipping {path}: {exc}", file=sys.stderr)
            continue

        raw = data[0] if isinstance(data, list) else data

        writers["articles"].add(extract_article(raw))
        writers["authors"].add(extract_authors(raw))
        writers["institutions"].add(extract_institutions(raw))
        writers["venues"].add(extract_venue(raw))
        writers["subfields"].add(extract_subfields(raw))
        writers["relationships"].add(extract_relationships(raw, wanted_ids))

        if i % 10_000 == 0:
            print(f"  {i:,} / {len(id_to_path):,} articles processed ...", flush=True)

    totals = {name: w.close() for name, w in writers.items()}

    print("\nDone.")
    for name, count in totals.items():
        print(f"  {name:<15} {count:>10,} records")


if __name__ == "__main__":
    main()
"""
1_flatten.py

Reads individual OpenAlex work JSON files from an input directory,
extracts only the fields needed for the Neo4j schema, and writes
compact batched JSON files to an output directory.

Usage:
    python 1_flatten.py --input ./responses/openalex_cs --output ./flat --batch-size 5000
"""

import argparse
import json
import os
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def extract_article(raw: dict) -> dict:
    """Return a flat dict with only the Article node fields."""
    return {
        "openalex_id":      raw.get("id", "").replace("https://openalex.org/", ""),
        "doi":              raw.get("doi"),
        "title":            raw.get("display_name"),
        "publication_year": raw.get("publication_year"),
        "type":             raw.get("type"),
        "cited_by_count":   raw.get("cited_by_count", 0),
        "is_retracted":     raw.get("is_retracted", False),
    }


def extract_authors(raw: dict) -> list[dict]:
    """Return a list of Author dicts found in authorships."""
    authors = []
    for a in raw.get("authorships", []):
        author = a.get("author") or {}
        aid = author.get("id", "")
        if not aid:
            continue
        authors.append({
            "openalex_id":  aid.replace("https://openalex.org/", ""),
            "display_name": author.get("display_name"),
            "orcid":        author.get("orcid"),
        })
    return authors


def extract_institutions(raw: dict) -> list[dict]:
    """Return a list of Institution dicts found in authorships."""
    seen = set()
    institutions = []
    for a in raw.get("authorships", []):
        for inst in a.get("institutions", []):
            iid = inst.get("id", "")
            if not iid or iid in seen:
                continue
            seen.add(iid)
            institutions.append({
                "openalex_id":  iid.replace("https://openalex.org/", ""),
                "display_name": inst.get("display_name"),
                "ror":          inst.get("ror"),
                "country_code": inst.get("country_code"),
                "type":         inst.get("type"),
            })
    return institutions


def extract_venue(raw: dict) -> dict | None:
    """Return a Venue dict from primary_location.source, or None."""
    loc    = raw.get("primary_location") or {}
    source = loc.get("source") or {}
    sid    = source.get("id", "")
    if not sid:
        return None
    return {
        "openalex_id":  sid.replace("https://openalex.org/", ""),
        "display_name": source.get("display_name"),
        "issn_l":       source.get("issn_l"),
        "type":         source.get("type"),
    }


def extract_subfields(raw: dict) -> list[dict]:
    """
    Return a deduplicated list of Subfield dicts from topics[].
    Each subfield carries its parent field for hierarchy queries.

    Example output:
        {
            "openalex_id":  "subfields/1707",
            "display_name": "Computer Vision and Pattern Recognition",
            "field_id":     "fields/17",
            "field_name":   "Computer Science",
        }
    """
    seen = set()
    subfields = []
    for t in raw.get("topics", []):
        subfield = t.get("subfield") or {}
        sid = subfield.get("id", "")
        if not sid or sid in seen:
            continue
        seen.add(sid)
        field = t.get("field") or {}
        subfields.append({
            "openalex_id":  sid.replace("https://openalex.org/", ""),
            "display_name": subfield.get("display_name"),
            "field_id":     field.get("id", "").replace("https://openalex.org/", ""),
            "field_name":   field.get("display_name"),
        })
    return subfields


def extract_relationships(raw: dict) -> dict:
    """
    Return relationship records that link this article to other nodes.
    cited_works and related_works store short IDs only (no base URL).
    """
    article_id = raw.get("id", "").replace("https://openalex.org/", "")

    authored_by = []
    for a in raw.get("authorships", []):
        author = a.get("author") or {}
        aid    = author.get("id", "")
        if not aid:
            continue
        inst_ids = [
            i.get("id", "").replace("https://openalex.org/", "")
            for i in a.get("institutions", [])
            if i.get("id")
        ]
        authored_by.append({
            "author_id":        aid.replace("https://openalex.org/", ""),
            "author_position":  a.get("author_position"),
            "is_corresponding": a.get("is_corresponding", False),
            "institution_ids":  inst_ids,
            "countries":        a.get("countries", []),
        })

    loc    = raw.get("primary_location") or {}
    source = loc.get("source") or {}
    venue_id = source.get("id", "").replace("https://openalex.org/", "") or None

    # Collect unique subfield IDs referenced by this article's topics
    seen_sf = set()
    subfield_ids = []
    for t in raw.get("topics", []):
        sf = t.get("subfield") or {}
        sid = sf.get("id", "")
        if sid and sid not in seen_sf:
            seen_sf.add(sid)
            subfield_ids.append(sid.replace("https://openalex.org/", ""))

    cited_works = [
        w.replace("https://openalex.org/", "")
        for w in raw.get("referenced_works", [])
    ]

    return {
        "article_id":    article_id,
        "authored_by":   authored_by,
        "venue_id":      venue_id,
        "subfield_ids":  subfield_ids,
        "cited_works":   cited_works,
    }


# ---------------------------------------------------------------------------
# Batch writer
# ---------------------------------------------------------------------------

class BatchWriter:
    """Accumulates records and flushes to numbered JSON files."""

    def __init__(self, output_dir: Path, name: str, batch_size: int):
        self.output_dir = output_dir
        self.name       = name
        self.batch_size = batch_size
        self._buffer: list = []
        self._file_idx      = 0
        self._total         = 0

    def add(self, record):
        if record is None:
            return
        if isinstance(record, list):
            self._buffer.extend(record)
        else:
            self._buffer.append(record)
        self._total += 1 if not isinstance(record, list) else len(record)
        if len(self._buffer) >= self.batch_size:
            self._flush()

    def _flush(self):
        if not self._buffer:
            return
        path = self.output_dir / f"{self.name}_{self._file_idx:04d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self._buffer, f, ensure_ascii=False)
        self._buffer  = []
        self._file_idx += 1

    def close(self):
        self._flush()
        return self._total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Flatten OpenAlex JSON files for Neo4j ingestion.")
    parser.add_argument("--input",      required=True, help="Directory of raw OpenAlex .json files")
    parser.add_argument("--output",     required=True, help="Directory to write flattened batch files")
    parser.add_argument("--batch-size", type=int, default=5000, help="Records per output file (default: 5000)")
    args = parser.parse_args()

    input_dir  = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # One sub-directory per entity type keeps things tidy for the loader
    for subdir in ("articles", "authors", "institutions", "venues", "subfields", "relationships"):
        (output_dir / subdir).mkdir(exist_ok=True)

    writers = {
        "articles":      BatchWriter(output_dir / "articles",      "articles",      args.batch_size),
        "authors":       BatchWriter(output_dir / "authors",        "authors",       args.batch_size),
        "institutions":  BatchWriter(output_dir / "institutions",   "institutions",  args.batch_size),
        "venues":        BatchWriter(output_dir / "venues",         "venues",        args.batch_size),
        "subfields":     BatchWriter(output_dir / "subfields",      "subfields",     args.batch_size),
        "relationships": BatchWriter(output_dir / "relationships",  "relationships", args.batch_size),
    }

    files = sorted(input_dir.glob("*.json"))
    total_files = len(files)
    if total_files == 0:
        print(f"No .json files found in {input_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Processing {total_files:,} files...")

    for i, path in enumerate(files, 1):
        try:
            with open(path, encoding="utf-8") as f:
                raw = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"  [WARN] Skipping {path.name}: {e}", file=sys.stderr)
            continue

        writers["articles"].add(extract_article(raw))
        writers["authors"].add(extract_authors(raw))
        writers["institutions"].add(extract_institutions(raw))
        writers["venues"].add(extract_venue(raw))
        writers["subfields"].add(extract_subfields(raw))
        writers["relationships"].add(extract_relationships(raw))

        if i % 10_000 == 0:
            print(f"  {i:,} / {total_files:,} files processed...")

    totals = {name: w.close() for name, w in writers.items()}

    print("\nDone.")
    for name, count in totals.items():
        print(f"  {name:<15} {count:>10,} records")


if __name__ == "__main__":
    main()
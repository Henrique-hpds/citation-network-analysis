[
    "w0010...": {
        "index": ["w1312321", ...],
        "reverse_index": ["w93219", ...]
        "path": "_top_cited_cs/w0010..." (ou outra pasta) OU "W00/10/w0010..."
    },
    ...
]


"""
Build a reverse citation index (cited_by) from one or more corpus directories.
Directories may be flat or nested (e.g. data/responses_1/W10/00/W1000057649.json).

Usage:
    python build_reverse_index.py \
        --corpus-dirs data/responses_1 data/responses/unicamp_cs data/responses/top_cited_cs \
        --output-index data/cited_by_index.json
"""

import argparse
import json
import sys
from collections import defaultdict
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


def _ref_ids(rec: dict) -> list[str]:
    """Return the list of article IDs that `rec` directly cites."""
    return [_strip(r) for r in rec.get("referenced_works", []) if r]


# ---------------------------------------------------------------------------
# Build reverse citation index
# ---------------------------------------------------------------------------

def build_reverse_index(
    corpus_dirs: list[Path],
    output_path: Path,
    report_every: int = 50_000,
) -> dict[str, list[str]]:
    """
    Scan every JSON file under each directory in `corpus_dirs` and build:

        cited_by[article_id] = [ids of articles that cite article_id]

    This is the inverse of `referenced_works`. Both flat and nested directory
    layouts are supported — scanning uses rglob so depth doesn't matter.

    The index is saved incrementally to `output_path` every `report_every`
    articles so progress is never lost if the run is interrupted.
    """
    cited_by: dict[str, list[str]] = defaultdict(list)
    total = 0

    print("=== Building reverse citation index ===")
    for d in corpus_dirs:
        print(f"  + {d}")

    for corpus_dir in corpus_dirs:
        print(f"\n  Scanning: {corpus_dir}")
        for path in corpus_dir.rglob("*.json"):
            for rec in _iter_records(path):
                src = _strip(rec.get("id", ""))
                if not src:
                    continue
                for ref in _ref_ids(rec):
                    cited_by[ref].append(src)
                total += 1
                if total % report_every == 0:
                    print(f"  ... {total:,} articles scanned, "
                          f"{len(cited_by):,} unique cited articles so far — "
                          f"saving checkpoint ...",
                          flush=True)
                    _save(cited_by, output_path, dedup=False)

    # Final dedup pass (avoids per-article overhead during the hot loop)
    for key in cited_by:
        cited_by[key] = list(dict.fromkeys(cited_by[key]))

    result = dict(cited_by)
    _save(result, output_path, dedup=False)  # already deduped above

    print(f"\n  Done. {total:,} articles scanned, "
          f"{len(result):,} entries in reverse index.")
    return result


def _save(cited_by: dict, path: Path, dedup: bool = True) -> None:
    data = cited_by
    if dedup:
        data = {k: list(dict.fromkeys(v)) for k, v in cited_by.items()}
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Build a reverse citation index from one or more corpus directories."
    )
    parser.add_argument(
        "--corpus-dirs", required=True, nargs="+", metavar="DIR",
        help="One or more corpus directories to scan (flat or nested layouts both work).",
    )
    parser.add_argument(
        "--output-index", default="cited_by_index.json",
        help="Where to save the reverse citation index (default: cited_by_index.json).",
    )
    parser.add_argument(
        "--skip-if-exists", action="store_true",
        help="If the output file already exists, load and return it instead of rebuilding.",
    )
    args = parser.parse_args()

    output_path = Path(args.output_index)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.skip_if_exists and output_path.exists():
        print(f"=== Loading existing index from {output_path} ===")
        cited_by = json.loads(output_path.read_text(encoding="utf-8"))
        print(f"  {len(cited_by):,} entries loaded.")
        return cited_by

    corpus_dirs = [Path(d) for d in args.corpus_dirs]
    missing = [d for d in corpus_dirs if not d.exists()]
    if missing:
        for d in missing:
            print(f"  [ERROR] Directory not found: {d}", file=sys.stderr)
        sys.exit(1)

    build_reverse_index(corpus_dirs, output_path)


if __name__ == "__main__":
    main()
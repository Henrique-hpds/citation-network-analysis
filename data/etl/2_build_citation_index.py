"""
Builds a combined citation index from one or more corpus directories in a
single pass. Each article gets an entry with:

    {
        "index":         ["W123", ...],   # articles this one cites (forward)
        "reverse_index": ["W456", ...],   # articles that cite this one (reverse)
        "path":          "responses_1/W10/00/W1000057649.json"
    }

Articles that are referenced but have no JSON file on disk still receive a
reverse_index entry (populated by whoever cites them), but their "index" and
"path" will be absent until their file is scanned.

Usage:
    python build_citation_index.py \
        --corpus-dirs data/responses_1 data/responses/unicamp_cs data/responses/top_cited_cs \
        --output-index data/citation_index.json
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
# Build combined citation index
# ---------------------------------------------------------------------------

def build_citation_index(
    corpus_dirs: list[Path],
    output_path: Path,
    checkpoint: dict[str, dict] | None = None,
    report_every: int = 50_000,
) -> dict[str, dict]:
    """
    Single-pass scan of all JSON files under `corpus_dirs`. Builds:

        index[wid] = {
            "index":         [...],   # referenced_works (forward)
            "reverse_index": [...],   # articles that cite wid (reverse)
            "path":          "..."    # relative path to the source JSON
        }

    Both forward and reverse data are collected in the same loop:
      - When we scan article A which cites [B, C]:
          index[A]["index"]         = [B, C]       ← forward, read directly
          index[B]["reverse_index"] += [A]          ← reverse, accumulated
          index[C]["reverse_index"] += [A]

    Articles only referenced but never found on disk will have no "index"
    or "path" key, only a "reverse_index".

    If `checkpoint` is provided, any file whose path is already recorded in
    the checkpoint is skipped — the scan resumes from where it left off.

    Checkpoints are written to disk every `report_every` articles.
    """

    index: dict[str, dict] = defaultdict(lambda: {"reverse_index": []})
    total = 0

    if checkpoint:
        # Seed the index from the checkpoint so accumulated data is preserved
        for wid, entry in checkpoint.items():
            index[wid] = dict(entry)
        # Collect the set of file paths already fully scanned
        already_scanned = {
            entry["path"] for entry in checkpoint.values() if "path" in entry
        }
        print(f"  Resuming from checkpoint: {len(checkpoint):,} entries, "
              f"{len(already_scanned):,} files already scanned.")
    else:
        already_scanned = set()

    print("=== Building citation index ===")
    for d in corpus_dirs:
        print(f"  + {d}")

    for corpus_dir in corpus_dirs:
        print(f"\n  Scanning: {corpus_dir}", flush=True)
        for path in corpus_dir.rglob("*.json"):
            if str(path) in already_scanned:
                continue

            for rec in _iter_records(path):
                src = _strip(rec.get("id", ""))
                if not src:
                    continue

                refs = _ref_ids(rec)

                # Forward index, citation count, and file path for this article
                entry = index[src]
                entry["index"] = refs
                entry["cited_by_count"] = rec.get("cited_by_count", 0)
                entry["path"] = str(path)

                # Reverse index: register src as a citer of each ref
                for ref in refs:
                    index[ref]["reverse_index"].append(src)

                total += 1
                if total % report_every == 0:
                    print(f"  ... {total:,} new articles scanned, "
                          f"{len(index):,} unique articles in index so far — "
                          f"saving checkpoint ...",
                          flush=True)
                    _save(index, output_path)

    # Final dedup of reverse_index lists accumulated during the scan
    for entry in index.values():
        entry["reverse_index"] = list(dict.fromkeys(entry["reverse_index"]))

    result = dict(index)
    _save(result, output_path)

    print(f"\n  Done. {total:,} new articles scanned, "
          f"{len(result):,} unique articles in index.")
    return result


def _save(index: dict, path: Path) -> None:
    path.write_text(json.dumps(index, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Build a combined forward+reverse citation index from one or more corpus directories."
    )
    parser.add_argument(
        "--corpus-dirs", required=True, nargs="+", metavar="DIR",
        help="One or more corpus directories to scan (flat or nested layouts both work).",
    )
    parser.add_argument(
        "--output-index", default="citation_index.json",
        help="Where to save the citation index (default: citation_index.json).",
    )
    parser.add_argument(
        "--use-checkpoint", action="store_true",
        help="Load the existing output file as a checkpoint and resume scanning "
             "from where it left off, skipping files already recorded in the index.",
    )
    args = parser.parse_args()

    output_path = Path(args.output_index)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    corpus_dirs = [Path(d) for d in args.corpus_dirs]
    missing = [d for d in corpus_dirs if not d.exists()]
    if missing:
        for d in missing:
            print(f"  [ERROR] Directory not found: {d}", file=sys.stderr)
        sys.exit(1)

    checkpoint = None
    if args.use_checkpoint:
        if output_path.exists():
            print(f"=== Loading checkpoint from {output_path} ===")
            checkpoint = json.loads(output_path.read_text(encoding="utf-8"))
            print(f"  {len(checkpoint):,} entries loaded.")
        else:
            print(f"  [WARN] --use-checkpoint set but {output_path} not found — starting fresh.")

    build_citation_index(corpus_dirs, output_path, checkpoint=checkpoint)


if __name__ == "__main__":
    main()
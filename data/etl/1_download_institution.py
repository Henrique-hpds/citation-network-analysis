"""
Usage (from repository root):
    python ./data/etl/0_download_institution.py \
        --input-csv     ./data/request_params/institutions/final_filtered_cs_institutions.csv \
        --output-dir     ./data/data/responses/by_institution/ \
        --min-citations     10 \
        --field-id          17
"""

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
import csv

import aiohttp
from dotenv import load_dotenv
import os
import unicodedata
import re

load_dotenv(Path(__file__).resolve().parent / "../../.env")

API_KEY  = os.getenv("OPENALEX_API_KEY")
BASE_URL = "https://api.openalex.org/works"

UNICAMP_ID = "I181391015"

FIELDS_SELECT = ",".join([
    "id", "doi", "display_name", "title", "publication_year",
    "publication_date", "language", "type", "primary_location",
    "open_access", "authorships", "primary_topic", "topics",
    "keywords", "concepts", "cited_by_count", "is_retracted",
    "referenced_works", "related_works", "counts_by_year",
    "fwci", "citation_normalized_percentile",
])

RETRY_ATTEMPTS = 4
RETRY_DELAY    = 3.0


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _make_session() -> aiohttp.ClientSession:
    headers   = {"User-Agent": "citation-network-analysis/1.0 (mailto:henriquehpds95@gmail.com)"}
    connector = aiohttp.TCPConnector(limit=1)
    return aiohttp.ClientSession(connector=connector, headers=headers)


def _add_key(params: dict) -> dict:
    if API_KEY:
        params["api_key"] = API_KEY
    return params


async def fetch_page(session: aiohttp.ClientSession, params: dict) -> dict:
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            async with session.get(
                BASE_URL, params=params,
                timeout=aiohttp.ClientTimeout(total=60)
            ) as resp:
                if resp.status == 429 or resp.status >= 500:
                    await asyncio.sleep(RETRY_DELAY * attempt)
                    continue
                resp.raise_for_status()
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt == RETRY_ATTEMPTS:
                raise
            await asyncio.sleep(RETRY_DELAY * attempt)
            print(f"  [retry {attempt}] {exc}", file=sys.stderr)
    raise RuntimeError("All retry attempts exhausted")


def _save_record(rec: dict, out_dir: Path, done_ids: set) -> bool:
    wid = rec.get("id", "").replace("https://openalex.org/", "")
    if not wid or wid in done_ids:
        return False
    out_path = out_dir / f"{wid}.json"
    if not out_path.exists():
        out_path.write_text(json.dumps(rec, ensure_ascii=False), encoding="utf-8")
    done_ids.add(wid)
    return True


def _load_checkpoint(out_dir: Path) -> dict:
    path = out_dir / ".checkpoint.json"
    return json.loads(path.read_text()) if path.exists() else {}


def _save_checkpoint(out_dir: Path, data: dict):
    (out_dir / ".checkpoint.json").write_text(json.dumps(data, indent=2))


REMOVED_WORDS = {
    "da", "de", "do", "dos", "das", "e", "a", "o", "os", "as",
    "of", "da", "del", "di", "du", "des", "le", "la", "el", "los", "las",
    "zu", "der", "die", "das", "und", "et", "al", "the", "and",
}

def _safe_institution_dir_name(name: str) -> str:
    name = name.lower()
    # Remove words with word boundaries
    pattern = r'\b(' + '|'.join(re.escape(word) for word in REMOVED_WORDS) + r')\b'
    name = re.sub(pattern, '', name)
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    # Clean up extra spaces and special characters
    name = re.sub(r'\s+', '_', name.strip())
    return "".join(
            c if (c.isalnum() or c in "_-") else "_"
            for c in name
        )[:30].strip("_-")

# ---------------------------------------------------------------------------
# Institution articles
# ---------------------------------------------------------------------------

async def parse_institutions_csv(csv_path: Path) -> list[tuple[str, str]]:
    institutions = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            institutions.append((row["openalex_id"], row["institution"]))
    return institutions

async def download_institution(
        out_dir: Path,
        institution_id: str = UNICAMP_ID,
        institution_name: str = "Unicamp",
        field_id: int = 17,
        min_citations: int = 10,
    ):
    out_institution_dir = out_dir / _safe_institution_dir_name(institution_name)
    out_institution_dir.mkdir(parents=True, exist_ok=True)
    cp          = _load_checkpoint(out_institution_dir)
    done_ids    = set(cp.get("ids_done", []))
    cursor      = cp.get("cursor", "*") or "*"
    pages_done  = cp.get("pages_done", 0)
    total_saved = len(done_ids)
    t0 = time.time()

    all_filters = [
        (
            f"primary_topic.field.id:{field_id}"
            f",cited_by_count:>{min_citations}"
            f",type:article"
            f",authorships.institutions.id:{institution_id}"
        ),
        (
            f"primary_topic.field.id:{field_id}"
            f",cited_by_count:>{min_citations}"
            f",type:article"
            f",institutions.id:{institution_id}"
        )
    ]

    for filter_str in all_filters:
        async with _make_session() as session:
            if pages_done == 0:
                data  = await fetch_page(session, _add_key(
                    {"filter": filter_str, "per_page": 200, "cursor": "*", "select": FIELDS_SELECT}
                ))
                total = data["meta"]["count"]
                print(f"  {institution_name} CS total: {total:,} articles")
            else:
                data  = None
                total = None
                print(f"  Resuming from page {pages_done + 1} — already saved {total_saved:,}")

            while cursor:
                if data is None:
                    data = await fetch_page(session, _add_key(
                        {"filter": filter_str, "per_page": 200, "cursor": cursor, "select": FIELDS_SELECT}
                    ))
                for rec in data.get("results", []):
                    if _save_record(rec, out_institution_dir, done_ids):
                        total_saved += 1

                pages_done += 1
                cursor = data["meta"].get("next_cursor") or ""
                _save_checkpoint(out_institution_dir,
                                {"cursor": cursor, "pages_done": pages_done, "ids_done": list(done_ids)})

                total_str = f"/{total:,}" if total else ""
                print(f"    page {pages_done:>3}  saved {total_saved:,}{total_str}  ({time.time()-t0:.0f}s)")
                data = None
                if not cursor:
                    break
                await asyncio.sleep(0.15)

    print(f"  {institution_name} done — {total_saved:,} articles → {out_institution_dir}\n")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run(args):
    institutions = await parse_institutions_csv(Path(args.input_csv))

    for i, (institution_id, institution_name) in enumerate(institutions):
        print(f"=== Starting download - {institution_name} ({i+1}/{len(institutions)}) ===")
        await download_institution(
            Path(args.output_dir),
            institution_id=institution_id,
            institution_name=institution_name,
            min_citations=args.min_citations
        )


def main():
    parser = argparse.ArgumentParser(
        description="Download Unicamp CS and top-cited CS articles from OpenAlex."
    )
    parser.add_argument("--input-csv",      required=True,
                        help="Input CSV file with institution IDs")
    parser.add_argument("--output-dir",     default="data/data/responses/",
                        help="Output directory for downloaded articles (default: data/data/responses/)")
    parser.add_argument("--min-citations", type=int, default=10,
                        help="Minimum number of citations for inclusion (default: 10)")
    parser.add_argument("--field-id", type=int, default=17,
                        help="Field ID for filtering (default: 17)")
    
    args = parser.parse_args()

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
"""
0_download.py

Downloads both corpora needed for the citation graph, one JSON file per work:

  1. Unicamp CS
       All Unicamp articles in Computer Science (field 17) with cited_by_count > 10.
       Uses cursor-based pagination (safe to interrupt and resume).

  2. Top-cited CS per year
       Top-N most-cited CS articles for each publication year in a given range.
       Sorted by cited_by_count descending, page-based pagination.

Usage:
    python 0_download.py \\
        --unicamp-output  ./responses/unicamp_cs \\
        --top-output      ./responses/top_cited_cs \\
        --top-per-year    10 \\
        --year-start      1980 \\
        --year-end        2024

Env:
    OPENALEX_API_KEY  (optional but raises rate limit)
"""

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

import aiohttp
from dotenv import load_dotenv
import os

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


# ---------------------------------------------------------------------------
# Corpus 1 — Unicamp
# ---------------------------------------------------------------------------

async def download_unicamp(out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    cp          = _load_checkpoint(out_dir)
    done_ids    = set(cp.get("ids_done", []))
    cursor      = cp.get("cursor", "*") or "*"
    pages_done  = cp.get("pages_done", 0)
    total_saved = len(done_ids)
    t0 = time.time()

    filter_str = (
        f"primary_topic.field.id:17,cited_by_count:>10,"
        f"type:article,authorships.institutions.id:{UNICAMP_ID}"
    )

    async with _make_session() as session:
        if pages_done == 0:
            data  = await fetch_page(session, _add_key(
                {"filter": filter_str, "per_page": 200, "cursor": "*", "select": FIELDS_SELECT}
            ))
            total = data["meta"]["count"]
            print(f"  Unicamp CS total: {total:,} articles")
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
                if _save_record(rec, out_dir, done_ids):
                    total_saved += 1

            pages_done += 1
            cursor = data["meta"].get("next_cursor") or ""
            _save_checkpoint(out_dir,
                             {"cursor": cursor, "pages_done": pages_done, "ids_done": list(done_ids)})

            total_str = f"/{total:,}" if total else ""
            print(f"    page {pages_done:>3}  saved {total_saved:,}{total_str}  ({time.time()-t0:.0f}s)")
            data = None
            if not cursor:
                break
            await asyncio.sleep(0.15)

    print(f"  Unicamp done — {total_saved:,} articles → {out_dir}\n")


# ---------------------------------------------------------------------------
# Corpus 2 — Top-cited per year
# ---------------------------------------------------------------------------

async def download_top_cited(out_dir: Path, top_per_year: int, year_start: int, year_end: int):
    out_dir.mkdir(parents=True, exist_ok=True)
    cp          = _load_checkpoint(out_dir)
    done_ids    = set(cp.get("ids_done", []))
    years_done  = set(cp.get("years_done", []))
    total_saved = len(done_ids)
    t0 = time.time()

    years = [y for y in range(year_start, year_end + 1) if y not in years_done]
    print(f"  Top-{top_per_year}/year, {year_start}–{year_end} "
          f"({len(years_done)} years already done, {len(years)} remaining)")

    per_page = min(top_per_year, 200)

    async with _make_session() as session:
        for year in years:
            year_saved = 0
            remaining  = top_per_year
            page       = 1
            filter_str = f"primary_topic.field.id:17,type:article,publication_year:{year}"

            while remaining > 0:
                fetch_n = min(remaining, per_page)
                data    = await fetch_page(session, _add_key({
                    "filter":   filter_str,
                    "per_page": fetch_n,
                    "page":     page,
                    "sort":     "cited_by_count:desc",
                    "select":   FIELDS_SELECT,
                }))
                results = data.get("results", [])
                if not results:
                    break

                for rec in results:
                    if _save_record(rec, out_dir, done_ids):
                        total_saved += 1
                        year_saved  += 1

                remaining -= len(results)
                if len(results) < fetch_n:
                    break
                page += 1
                await asyncio.sleep(0.1)

            years_done.add(year)
            _save_checkpoint(out_dir,
                             {"ids_done": list(done_ids), "years_done": list(years_done)})
            print(f"    {year}  +{year_saved:>4} articles  (total {total_saved:,})  ({time.time()-t0:.0f}s)")

    print(f"  Top-cited done — {total_saved:,} articles → {out_dir}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run(args):
    print("=== Corpus 1: Unicamp CS ===")
    await download_unicamp(Path(args.unicamp_output))

    print("=== Corpus 2: Top-cited CS per year ===")
    await download_top_cited(
        Path(args.top_output),
        args.top_per_year,
        args.year_start,
        args.year_end,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Download Unicamp CS and top-cited CS articles from OpenAlex."
    )
    parser.add_argument("--unicamp-output", default="data/data/responses/unicamp_cs",
                        help="Output dir for Unicamp corpus (default: data/data/responses/unicamp_cs)")
    parser.add_argument("--top-output",     default="data/data/responses/top_cited_cs",
                        help="Output dir for top-cited corpus (default: data/data/responses/top_cited_cs)")
    parser.add_argument("--top-per-year",   type=int, default=10,
                        help="Top-N articles per year for the top-cited corpus (default: 10)")
    parser.add_argument("--year-start",     type=int, default=1980,
                        help="First publication year (default: 1980)")
    parser.add_argument("--year-end",       type=int, default=2024,
                        help="Last publication year (default: 2024)")
    args = parser.parse_args()

    asyncio.run(run(args))


if __name__ == "__main__":
    main()

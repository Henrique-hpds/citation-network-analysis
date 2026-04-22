"""
3_fetch_neighbors.py

Given a list of OpenAlex Work IDs, fetches each work's full record from the
OpenAlex API and writes them to an output directory (same format as the raw
responses in responses/openalex_cs/).

Used by 4_build_graph.py during BFS expansion to pull neighbors on-demand
without downloading the full dataset up-front.

Usage:
    python 3_fetch_neighbors.py --ids W12345 W67890 --output ./responses/expansion
    python 3_fetch_neighbors.py --ids-file ids.txt   --output ./responses/expansion

Env:
    OPENALEX_API_KEY  (optional — raises rate limit from 10 to 100 req/s)
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / "../../.env")

OPENALEX_BASE = "https://api.openalex.org/works"
API_KEY = os.getenv("OPENALEX_API_KEY")

# Keep well under the polite-pool limit
MAX_CONCURRENT = 8
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2.0  # seconds


async def fetch_work(session: aiohttp.ClientSession, work_id: str) -> dict | None:
    """Fetch a single work record from OpenAlex. Returns None on failure."""
    url = f"{OPENALEX_BASE}/{work_id}"
    params = {}
    if API_KEY:
        params["api_key"] = API_KEY

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 404:
                    return None
                if resp.status == 429 or resp.status >= 500:
                    await asyncio.sleep(RETRY_DELAY * attempt)
                    continue
                resp.raise_for_status()
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt == RETRY_ATTEMPTS:
                print(f"  [WARN] Failed to fetch {work_id} after {RETRY_ATTEMPTS} attempts: {exc}", file=sys.stderr)
                return None
            await asyncio.sleep(RETRY_DELAY * attempt)

    return None


async def fetch_all(work_ids: list[str], output_dir: Path) -> tuple[int, int]:
    """Fetch all work IDs concurrently, skip already-downloaded files."""
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    fetched = skipped = 0

    async def bounded_fetch(session, wid):
        nonlocal fetched, skipped
        out_path = output_dir / f"{wid}.json"
        if out_path.exists():
            skipped += 1
            return

        async with sem:
            data = await fetch_work(session, wid)

        if data is not None:
            out_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            fetched += 1

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)
    headers = {"User-Agent": "citation-network-analysis/1.0 (mailto:henriquehpds95@gmail.com)"}

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        tasks = [bounded_fetch(session, wid) for wid in work_ids]
        total = len(tasks)
        done = 0
        for coro in asyncio.as_completed(tasks):
            await coro
            done += 1
            if done % 100 == 0 or done == total:
                print(f"  {done}/{total} processed (fetched={fetched}, skipped={skipped})")

    return fetched, skipped


def main():
    parser = argparse.ArgumentParser(description="Fetch OpenAlex work records by ID.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--ids",      nargs="+", metavar="ID",  help="Work IDs (e.g. W12345)")
    group.add_argument("--ids-file", metavar="FILE", help="Text file with one Work ID per line")
    parser.add_argument("--output",  required=True, help="Directory to write JSON files")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.ids:
        work_ids = args.ids
    else:
        work_ids = Path(args.ids_file).read_text().splitlines()
        work_ids = [w.strip() for w in work_ids if w.strip()]

    print(f"Fetching {len(work_ids):,} work IDs → {output_dir}")
    fetched, skipped = asyncio.run(fetch_all(work_ids, output_dir))
    print(f"Done. fetched={fetched:,}  skipped(already existed)={skipped:,}")


if __name__ == "__main__":
    main()

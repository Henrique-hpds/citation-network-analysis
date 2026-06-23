"""
Comparative diffusion-distance analysis by university.

This script extends the idea from analysis/distances.py:
- In the citation digraph, A -> B means "A cites B".
- Diffusion is measured by shortest directed-path length from source set H to target paper P
  in the original digraph (multi-source BFS on original graph).

Here, target papers come from a JSON file of OpenAlex IDs, then are grouped by
university using :AFFILIATED_WITH relationships. The report compares each university.

Examples:
    python analysis/distances_by_university.py --article-ids-file pedro.json
    python analysis/distances_by_university.py --article-ids-file pedro.json --min-citations 1000
    python analysis/distances_by_university.py --article-ids-file pedro.json --sources-article-ids-file message.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from collections import defaultdict, deque
from typing import Dict, Iterable, List, Set, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

FIGURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "figures")
REPORTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
os.makedirs(FIGURES_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

PALETTE = {
    "blue": "#2563EB",
    "orange": "#EA580C",
    "gray": "#6B7280",
    "green": "#16A34A",
}

CONTINENT_ORDER = ["Africa", "Asia", "Europe", "North America", "South America"]
CONTINENT_COLORS = {
    "Africa": "#F97316",
    "Asia": "#2563EB",
    "Europe": "#7C3AED",
    "North America": "#16A34A",
    "South America": "#D97706",
}

AFRICA = {
    "AO", "BF", "BI", "BJ", "BW", "CD", "CF", "CG", "CI", "CM", "CV", "DJ", "DZ", "EG", "ER",
    "ET", "GA", "GH", "GM", "GN", "GQ", "GW", "KE", "KM", "LR", "LS", "LY", "MA", "MG", "ML",
    "MR", "MU", "MW", "MZ", "NA", "NE", "NG", "RE", "RW", "SC", "SD", "SL", "SN", "SO", "SS",
    "ST", "SZ", "TD", "TG", "TN", "TZ", "UG", "ZA", "ZM", "ZW",
}
ASIA = {
    "AE", "AF", "AM", "AZ", "BD", "BH", "BN", "BT", "CN", "CY", "GE", "HK", "ID", "IL", "IN",
    "IQ", "IR", "JO", "JP", "KG", "KH", "KR", "KW", "KZ", "LA", "LB", "LK", "MM", "MN", "MO",
    "MV", "MY", "NP", "OM", "PH", "PK", "QA", "SA", "SG", "SY", "TH", "TJ", "TL", "TM", "TR",
    "TW", "UZ", "VN", "YE",
}
EUROPE = {
    "AL", "AD", "AT", "BA", "BE", "BG", "BY", "CH", "CZ", "DE", "DK", "EE", "ES", "FI", "FR",
    "GB", "GR", "HR", "HU", "IE", "IS", "IT", "LI", "LT", "LU", "LV", "MC", "MD", "ME", "MK",
    "MT", "NL", "NO", "PL", "PT", "RO", "RS", "SE", "SI", "SK", "SM", "UA", "VA", "XK",
}
NORTH_AMERICA = {
    "AG", "AI", "AW", "BB", "BL", "BM", "BS", "BZ", "CA", "CR", "CU", "CW", "DM", "DO", "GD",
    "GL", "GP", "GT", "HN", "HT", "JM", "KN", "KY", "LC", "MF", "MQ", "MS", "MX", "NI", "PA",
    "PM", "PR", "SV", "SX", "TC", "TT", "US", "VC", "VG", "VI",
}
SOUTH_AMERICA = {"AR", "BO", "BR", "CL", "CO", "EC", "FK", "GF", "GY", "PE", "PY", "SR", "UY", "VE"}
OCEANIA = {"AS", "AU", "CK", "FJ", "FM", "GU", "KI", "MH", "MP", "NC", "NF", "NR", "NU", "NZ", "PF", "PG", "PN", "PW", "SB", "TK", "TO", "TV", "VU", "WS"}


def continent_from_country_code(country_code: str | None) -> str:
    code = (country_code or "").strip().upper()
    if not code:
        return "Other"
    if code in AFRICA:
        return "Africa"
    if code in ASIA:
        return "Asia"
    if code in EUROPE:
        return "Europe"
    if code in NORTH_AMERICA:
        return "North America"
    if code in SOUTH_AMERICA:
        return "South America"
    if code in OCEANIA:
        return "Oceania"
    if code == "AQ":
        return "Antarctica"
    return "Other"


def chunked(seq: List[str], size: int = 5000) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def load_id_list(path: str) -> Set[str]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"Arquivo {path} deve conter uma lista JSON de IDs")
    return {str(x) for x in data if str(x).strip()}


def multisource_bfs_original(G: nx.DiGraph, sources: Set[str]) -> Dict[str, int]:
    dist: Dict[str, int] = {}
    q: deque[str] = deque()

    for s in sources:
        if s in G:
            dist[s] = 0
            q.append(s)

    while q:
        u = q.popleft()
        for v in G.successors(u):
            if v not in dist:
                dist[v] = dist[u] + 1
                q.append(v)

    return dist


def short_name(name: str, max_len: int = 40) -> str:
    text = (name or "").strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "..."


def is_unicamp_name(name: str) -> bool:
    text = (name or "").lower()
    return "unicamp" in text or "campinas" in text


def normalize_continent(continent: str | None) -> str | None:
    value = (continent or "").strip()
    if value in CONTINENT_ORDER:
        return value
    return None


def is_unicamp_institution(row: dict) -> bool:
    return row.get("university_id") == "I181391015" or is_unicamp_name(row.get("university_name", ""))


def compute_university_groups(session, article_ids: Set[str]) -> Dict[str, dict]:
    """
    Return mapping:
      inst_openalex_id -> {"name": str, "article_ids": set[str]}

    One article can belong to multiple institutions.
    Articles with no institution are grouped under synthetic key NO_INSTITUTION.
    """
    groups: Dict[str, dict] = {}
    orphan_key = "NO_INSTITUTION"
    groups[orphan_key] = {"name": "No institution linked", "country_code": None, "continent": "Other", "article_ids": set()}

    ids_list = list(article_ids)
    for batch in chunked(ids_list, 4000):
        rows = session.run(
            """
            MATCH (a:Article)
            WHERE a.openalex_id IN $ids
            OPTIONAL MATCH (a)-[:AFFILIATED_WITH]->(i:Institution)
            RETURN a.openalex_id AS article_id,
                     collect(DISTINCT {id: i.openalex_id, name: coalesce(i.display_name, i.openalex_id), country_code: i.country_code}) AS institutions
            """,
            {"ids": batch},
        )

        for row in rows:
            article_id = row["article_id"]
            institutions = [x for x in row["institutions"] if x and x.get("id")]
            if not institutions:
                groups[orphan_key]["article_ids"].add(article_id)
                continue

            for inst in institutions:
                inst_id = inst["id"]
                inst_name = inst.get("name") or inst_id
                if inst_id not in groups:
                    country_code = inst.get("country_code")
                    groups[inst_id] = {
                        "name": inst_name,
                        "country_code": country_code,
                        "continent": continent_from_country_code(country_code),
                        "article_ids": set(),
                    }
                groups[inst_id]["article_ids"].add(article_id)

    # Drop empty synthetic bucket
    if not groups[orphan_key]["article_ids"]:
        groups.pop(orphan_key)

    return groups


def build_comparative_report(
    min_citations: int,
    source_count: int,
    giant_size: int,
    total_input_ids: int,
    grouped_ids_total: int,
    results: List[dict],
    excluded_targets_from_sources: bool,
) -> str:
    lines = []
    lines.append("Model: A->B means A cites B; diffusion distance uses shortest path from source set H to P")
    lines.append("       in the original citation digraph (multi-source BFS).")
    lines.append("")
    lines.append(f"Input IDs in file                        : {total_input_ids:,}")
    lines.append(f"IDs found in graph and grouped           : {grouped_ids_total:,}")
    lines.append(f"Giant weakly-connected component size    : {giant_size:,}")
    lines.append(f"High-impact source threshold             : >= {min_citations:,} citations")
    lines.append(f"High-impact sources in giant WCC         : {source_count:,}")
    lines.append(f"Exclude target IDs from source set       : {excluded_targets_from_sources}")
    lines.append("")

    lines.append(
        "Continent | University                                | Articles in giant | Reached | Reach% | Mean  | Median | Min | Max"
    )
    lines.append("-" * 110)
    for r in results:
        mean_txt = f"{r['mean_dist']:.2f}" if r["mean_dist"] is not None else "-"
        med_txt = f"{r['median_dist']:.1f}" if r["median_dist"] is not None else "-"
        min_txt = f"{r['min_dist']}" if r["min_dist"] is not None else "-"
        max_txt = f"{r['max_dist']}" if r["max_dist"] is not None else "-"

        lines.append(
            f"{short_name(r['continent'], 9):9} |"
            f" {r['university_name']} |"
            f" {r['articles_in_giant']:16,} |"
            f" {r['reached']:7,} |"
            f" {r['reach_pct']:5.1f}% |"
            f" {mean_txt:>5} |"
            f" {med_txt:>6} |"
            f" {min_txt:>3} |"
            f" {max_txt:>3}"
        )

    lines.append("")
    lines.append("Notes:")
    lines.append("- One paper may contribute to multiple universities if it has multiple affiliations.")
    lines.append("- Distances are computed only for nodes inside the giant WCC.")
    lines.append("- Reach% is over that university's target-paper subset inside the giant WCC.")
    return "\n".join(lines)


def plot_comparison(results: List[dict], top_n: int):
    if not results:
        return

    # Keep a balanced sample while grouping bars by continent
    selected = _select_grouped_by_continent(results, top_n)
    labels = [r["university_name"] for r in selected]
    x = np.arange(len(selected), dtype=float)
    continent_colors = [CONTINENT_COLORS.get(r["continent"], PALETTE["gray"]) for r in selected]

    reach = np.array([r["reach_pct"] for r in selected], dtype=float)
    mean_vals = np.array([r["mean_dist"] if r["mean_dist"] is not None else np.nan for r in selected], dtype=float)

    fig, ax = plt.subplots(figsize=(max(10, len(selected) * 0.7), 6.2))
    ax.bar(x, reach, color=continent_colors, alpha=0.95)
    ax.set_ylabel("Reached papers (%)")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=9)
    ax.set_ylim(0, 100)
    _decorate_continent_groups(ax, selected)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    out1 = os.path.join(FIGURES_DIR, "distances_by_university_reach.png")
    fig.savefig(out1, dpi=150, bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(max(10, len(selected) * 0.7), 6.2))
    ax.bar(x, mean_vals, color=continent_colors, alpha=0.95)
    ax.set_ylabel("Mean distance (hops)")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=9)
    _decorate_continent_groups(ax, selected)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    out2 = os.path.join(FIGURES_DIR, "distances_by_university_mean.png")
    fig.savefig(out2, dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"  Saved: {out1}")
    print(f"  Saved: {out2}")


def _select_grouped_by_continent(results: List[dict], max_total: int) -> List[dict]:
    if not results:
        return []

    filtered = [r for r in results if normalize_continent(r.get("continent"))]
    by_continent: dict[str, list[dict]] = {continent: [] for continent in CONTINENT_ORDER}
    for result in filtered:
        continent = normalize_continent(result.get("continent"))
        if continent is None:
            continue
        by_continent.setdefault(continent, []).append(result)

    # Keep up to max_total total, distributed across continents, while preserving the continent grouping.
    remaining = max_total
    selected: list[dict] = []
    active_continents = [c for c in CONTINENT_ORDER if by_continent.get(c)]
    if not active_continents:
        return []

    per_continent = max(1, max_total // len(active_continents))
    leftovers: list[dict] = []
    for continent in active_continents:
        continent_rows = sorted(
            by_continent[continent],
            key=lambda r: (
                r["mean_dist"] if r["mean_dist"] is not None else float("inf"),
                r["reach_pct"],
                -r["articles_in_giant"],
                r["university_name"],
            ),
        )
        take = min(per_continent, len(continent_rows), remaining)
        selected.extend(continent_rows[:take])
        leftovers.extend(continent_rows[take:])
        remaining -= take

    if remaining > 0:
        leftovers = sorted(
            leftovers,
            key=lambda r: (
                r["mean_dist"] if r["mean_dist"] is not None else float("inf"),
                r["reach_pct"],
                -r["articles_in_giant"],
                r["university_name"],
            ),
        )
        selected.extend(leftovers[:remaining])

    # Force inclusion of UNICAMP if present in the data and not already selected.
    unicamp_candidates = [r for r in filtered if is_unicamp_institution(r)]
    if unicamp_candidates:
        unicamp_row = sorted(
            unicamp_candidates,
            key=lambda r: (
                r["mean_dist"] if r["mean_dist"] is not None else float("inf"),
                r["reach_pct"],
                -r["articles_in_giant"],
                r["university_name"],
            ),
        )[0]
        if all(r["university_id"] != unicamp_row["university_id"] for r in selected):
            unicamp_continent = unicamp_row["continent"]
            same_continent_idxs = [idx for idx, row in enumerate(selected) if row["continent"] == unicamp_continent]
            if same_continent_idxs:
                worst_idx = max(
                    same_continent_idxs,
                    key=lambda idx: (
                        selected[idx]["mean_dist"] if selected[idx]["mean_dist"] is not None else float("inf"),
                        selected[idx]["reach_pct"],
                        selected[idx]["articles_in_giant"],
                        selected[idx]["university_name"],
                    ),
                )
                selected[worst_idx] = unicamp_row
            elif len(selected) < max_total:
                selected.append(unicamp_row)

    selected.sort(
        key=lambda r: (
            CONTINENT_ORDER.index(r.get("continent")) if r.get("continent") in CONTINENT_ORDER else len(CONTINENT_ORDER),
            r["mean_dist"] if r["mean_dist"] is not None else float("inf"),
            r["reach_pct"],
            r["university_name"],
        )
    )
    return selected[:max_total]


def _filter_allowed_institutions(results: List[dict], allowed_ids: Set[str] | None) -> List[dict]:
    if not allowed_ids:
        return results

    filtered = [r for r in results if r["university_id"] in allowed_ids]
    unicamp_rows = [r for r in results if is_unicamp_institution(r)]
    if unicamp_rows and all(r["university_id"] != "I181391015" for r in filtered):
        filtered.extend(unicamp_rows[:1])

    seen: set[str] = set()
    unique: list[dict] = []
    for row in filtered:
        if row["university_id"] in seen:
            continue
        seen.add(row["university_id"])
        unique.append(row)
    return unique


def _decorate_continent_groups(ax, results: List[dict]):
    if not results:
        return

    positions_by_continent: dict[str, list[int]] = {}
    for idx, row in enumerate(results):
        continent = normalize_continent(row.get("continent"))
        if not continent:
            continue
        positions_by_continent.setdefault(continent, []).append(idx)

    for continent in CONTINENT_ORDER:
        positions = positions_by_continent.get(continent)
        if not positions:
            continue
        center = (positions[0] + positions[-1]) / 2
        ax.text(center, 1.03, continent, transform=ax.get_xaxis_transform(), ha="center", va="bottom", fontsize=9, fontweight="bold", color=CONTINENT_COLORS.get(continent, PALETTE["gray"]))
        ax.axvline(positions[-1] + 0.5, color="#D1D5DB", linewidth=0.8, linestyle="--", alpha=0.8)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--article-ids-file", required=True, help="JSON list of target article IDs")
    parser.add_argument(
        "--min-citations",
        type=int,
        default=500,
        help="High-impact source threshold (default: 500)",
    )
    parser.add_argument(
        "--sources-article-ids-file",
        help="Optional JSON list of source article IDs (overrides --min-citations)",
    )
    parser.add_argument(
        "--exclude-target-ids-from-sources",
        action="store_true",
        default=False,
        help="Remove target IDs from source set to avoid trivial distance=0 overlap",
    )
    parser.add_argument(
        "--min-articles-per-university",
        type=int,
        default=20,
        help="Filter out very small university groups (default: 20)",
    )
    parser.add_argument(
        "--top-n-plot",
        type=int,
        default=20,
        help="Number of largest university groups in plots (default: 20)",
    )
    parser.add_argument(
        "--institution-ids-file",
        help="Optional JSON list of institution OpenAlex IDs to keep in the comparison",
    )
    args = parser.parse_args()

    target_ids = load_id_list(args.article_ids_file)
    print(f"Loaded target IDs: {len(target_ids):,}")

    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    pwd = os.getenv("NEO4J_PASSWORD")
    if not all([uri, user, pwd]):
        raise RuntimeError("Missing Neo4j env vars (NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)")

    driver = GraphDatabase.driver(uri, auth=(user, pwd))

    try:
        with driver.session() as s:
            print("Loading graph edges...")
            edges = [
                (r["src"], r["tgt"])
                for r in s.run(
                    """
                    MATCH (a:Article)-[:CITES]->(b:Article)
                    RETURN a.openalex_id AS src, b.openalex_id AS tgt
                    """
                )
            ]

            print("Loading article metadata...")
            meta_rows = list(
                s.run(
                    """
                    MATCH (a:Article)
                    RETURN a.openalex_id AS id,
                           coalesce(a.cited_by_count, 0) AS cited_by_count,
                           coalesce(a.title, '') AS title,
                           a.publication_year AS year
                    """
                )
            )
            meta = {
                r["id"]: {
                    "cited_by_count": int(r["cited_by_count"] or 0),
                    "title": r["title"],
                    "year": r["year"],
                }
                for r in meta_rows
            }

            print("Grouping target IDs by university...")
            groups = compute_university_groups(s, target_ids)

    finally:
        driver.close()

    # Determine source set
    if args.sources_article_ids_file:
        source_ids = load_id_list(args.sources_article_ids_file)
        print(f"Using custom source IDs: {len(source_ids):,}")
    else:
        source_ids = {nid for nid, m in meta.items() if m["cited_by_count"] >= args.min_citations}
        print(f"High-impact sources by threshold >= {args.min_citations:,}: {len(source_ids):,}")

    if args.exclude_target_ids_from_sources:
        before = len(source_ids)
        source_ids = source_ids - target_ids
        print(f"Removed overlaps with target IDs: {before - len(source_ids):,}")

    allowed_institution_ids = None
    if args.institution_ids_file:
        allowed_institution_ids = load_id_list(args.institution_ids_file)
        allowed_institution_ids.add("I181391015")
        print(f"Allowed institutions loaded: {len(allowed_institution_ids):,}")

    # Build graph and giant WCC
    G = nx.DiGraph()
    G.add_nodes_from(meta.keys())
    G.add_edges_from(edges)

    print("Computing giant WCC...")
    largest_wcc = max(nx.weakly_connected_components(G), key=len)
    G_giant = G.subgraph(largest_wcc).copy()
    giant_nodes = set(G_giant.nodes)
    print(f"Giant WCC nodes: {len(giant_nodes):,}")

    sources_giant = source_ids & giant_nodes
    print(f"Sources in giant WCC: {len(sources_giant):,}")

    print("Running multi-source BFS...")
    dist_all = multisource_bfs_original(G_giant, sources_giant)
    print(f"Reached nodes: {len(dist_all):,} / {len(giant_nodes):,}")

    # Aggregate stats by university
    results = []
    grouped_ids_union = set()

    for inst_id, payload in groups.items():
        inst_name = payload["name"]
        ids = set(payload["article_ids"])
        ids_in_giant = ids & giant_nodes
        grouped_ids_union.update(ids_in_giant)

        if len(ids_in_giant) < args.min_articles_per_university:
            continue

        reached_ids = [nid for nid in ids_in_giant if nid in dist_all]
        dists = [dist_all[nid] for nid in reached_ids]

        if dists:
            arr = np.array(dists)
            mean_dist = float(arr.mean())
            median_dist = float(np.median(arr))
            min_dist = int(arr.min())
            max_dist = int(arr.max())
        else:
            mean_dist = None
            median_dist = None
            min_dist = None
            max_dist = None

        total = len(ids_in_giant)
        reached = len(reached_ids)
        reach_pct = (100.0 * reached / total) if total else 0.0

        results.append(
            {
                "university_id": inst_id,
                "university_name": inst_name,
                "continent": payload.get("continent", continent_from_country_code(payload.get("country_code"))),
                "articles_in_giant": total,
                "reached": reached,
                "reach_pct": reach_pct,
                "mean_dist": mean_dist,
                "median_dist": median_dist,
                "min_dist": min_dist,
                "max_dist": max_dist,
            }
        )

    results = [r for r in results if normalize_continent(r.get("continent"))]
    results = _filter_allowed_institutions(results, allowed_institution_ids)
    results.sort(
        key=lambda r: (
            CONTINENT_ORDER.index(r["continent"]),
            r["mean_dist"] if r["mean_dist"] is not None else float("inf"),
            r["reach_pct"],
            -r["articles_in_giant"],
            r["university_name"],
        )
    )

    report = build_comparative_report(
        min_citations=args.min_citations,
        source_count=len(sources_giant),
        giant_size=len(giant_nodes),
        total_input_ids=len(target_ids),
        grouped_ids_total=len(grouped_ids_union),
        results=results,
        excluded_targets_from_sources=args.exclude_target_ids_from_sources,
    )

    report_path = os.path.join(REPORTS_DIR, "distances_by_university_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"Saved: {report_path}")

    csv_path = os.path.join(REPORTS_DIR, "distances_by_university.csv")
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "university_id",
                "university_name",
                "continent",
                "articles_in_giant",
                "reached",
                "reach_pct",
                "mean_dist",
                "median_dist",
                "min_dist",
                "max_dist",
            ],
        )
        writer.writeheader()
        writer.writerows(results)
    print(f"Saved: {csv_path}")

    print("Generating comparison figures...")
    plot_comparison(results, top_n=args.top_n_plot)

    print("\nDone.")


if __name__ == "__main__":
    main()

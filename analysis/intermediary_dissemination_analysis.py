"""
Intermediary-set analysis focused on dissemination.

Materializes A_inter from path outputs, restricting to nodes present in the
final GraphML cache, then computes bridge-oriented metrics on the path-relevant
subgraph.
"""

from __future__ import annotations

import csv
import json
import math
import os
import random
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-citation-network")

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx

from agent_graph_analysis import ROOT, load_graph, table


REPORT_DATE = date.today().isoformat()
METRICS_DIR = ROOT / "metrics"
REPORTS_DIR = ROOT / "reports"
CAMINHOS_DIR = REPORTS_DIR / "caminhos"
FIGS_DIR = CAMINHOS_DIR / "figs"

PATH_FILES = {
    "top_tier_to_unicamp": ROOT / "data" / "output" / "paths_top_to_unicamp.json",
    "institutions_to_unicamp": ROOT / "data" / "output" / "paths_institutions_to_unicamp.json",
}

BETWEENNESS_K = 64
HARMONIC_SAMPLE_SIZE = 128
HARMONIC_SEED = 42


def read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def load_paths(path: Path) -> list[list[str]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return raw["paths"] if isinstance(raw, dict) else raw


def to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def percentiles(values: list[int]) -> dict[str, float]:
    if not values:
        return {"n": 0, "min": 0, "p25": 0, "median": 0, "p75": 0, "p90": 0, "p99": 0, "max": 0, "mean": 0.0}
    xs = sorted(values)

    def q(p: float) -> float:
        return xs[min(len(xs) - 1, max(0, round((len(xs) - 1) * p)))]

    return {
        "n": len(xs),
        "min": xs[0],
        "p25": q(0.25),
        "median": q(0.50),
        "p75": q(0.75),
        "p90": q(0.90),
        "p99": q(0.99),
        "max": xs[-1],
        "mean": sum(xs) / len(xs),
    }


def distribution_row(label: str, values: list[int]) -> list[object]:
    stats = percentiles(values)
    return [
        label,
        stats["n"],
        stats["min"],
        stats["p25"],
        stats["median"],
        stats["p75"],
        stats["p90"],
        stats["p99"],
        stats["max"],
        f"{stats['mean']:.2f}",
    ]


def quantile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    return xs[min(len(xs) - 1, max(0, round((len(xs) - 1) * p)))]


def harmonic_sampled(G: nx.Graph, seeds: set[str], sample_size: int, rng_seed: int) -> dict[str, float]:
    seeds = [node for node in seeds if node in G]
    if not seeds:
        return {node: 0.0 for node in G.nodes()}
    rng = random.Random(rng_seed)
    if len(seeds) > sample_size:
        seeds = rng.sample(seeds, sample_size)
    scores = {node: 0.0 for node in G.nodes()}
    for seed in seeds:
        lengths = nx.single_source_shortest_path_length(G, seed)
        for node, dist in lengths.items():
            if dist > 0:
                scores[node] += 1 / dist
    scale = len(seeds)
    return {node: value / scale for node, value in scores.items()}


def normalize(values: dict[str, float]) -> dict[str, float]:
    if not values:
        return {}
    xs = list(values.values())
    lo = min(xs)
    hi = max(xs)
    if hi <= lo:
        return {key: 0.0 for key in values}
    return {key: (value - lo) / (hi - lo) for key, value in values.items()}


def title(row: dict | None, limit: int = 88) -> str:
    if not row:
        return "fora do cache do GraphML"
    text = str(row.get("title") or row.get("representative_title") or "")
    return text if len(text) <= limit else text[: limit - 1] + "..."


def plot_path_cdf(lengths_by_source: dict[str, list[int]], path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.5, 5))
    colors = {
        "top_tier_to_unicamp": "#2563EB",
        "institutions_to_unicamp": "#16A34A",
    }
    labels = {
        "top_tier_to_unicamp": "top-tier -> Unicamp",
        "institutions_to_unicamp": "instituicoes -> Unicamp",
    }
    for key, lengths in lengths_by_source.items():
        xs = sorted(lengths)
        ys = [(i + 1) / len(xs) for i in range(len(xs))]
        ax.step(xs, ys, where="post", linewidth=2, color=colors[key], label=labels[key])
    ax.set_title("CDF dos comprimentos de caminhos ate a Unicamp", fontsize=12, fontweight="bold")
    ax.set_xlabel("Comprimento do caminho (# arestas)")
    ax.set_ylabel("Fração acumulada")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_flow_heatmap(matrix: list[list[int]], labels: list[str], path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.8, 7.2))
    vmax = max(max(row) for row in matrix) if matrix else 0
    im = ax.imshow(matrix, cmap="Blues", vmin=0, vmax=vmax if vmax > 0 else 1)
    ax.set_xticks(range(len(labels)))
    ax.set_yticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_yticklabels(labels)
    ax.set_xlabel("Cluster citado")
    ax.set_ylabel("Cluster citante")
    ax.set_title("Fluxo de citacoes entre clusters intermediarios dominantes", fontsize=12, fontweight="bold")
    for i in range(len(labels)):
        for j in range(len(labels)):
            value = matrix[i][j]
            if value:
                ax.text(j, i, str(value), ha="center", va="center", fontsize=8, color="#111827")
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def update_index(report_path: Path) -> None:
    index_path = REPORTS_DIR / "index.md"
    line = (
        f"| {REPORT_DATE} | caminhos | A_inter e disseminacao | "
        f"[A_inter](caminhos/{report_path.name}) | "
        "Materializacao de intermediarios, metricas de ponte e fluxo entre clusters. |"
    )
    text = index_path.read_text(encoding="utf-8") if index_path.exists() else (
        "# Índice de relatórios\n\n"
        "| Data | Tipo | Identificador | Relatório | Resumo |\n"
        "| --- | --- | --- | --- | --- |\n"
    )
    if line not in text:
        if "\n## Pendências" in text:
            text = text.replace("\n## Pendências", f"\n{line}\n\n## Pendências", 1)
        else:
            text = text.rstrip() + "\n" + line + "\n"
    index_path.write_text(text, encoding="utf-8")


def main() -> None:
    CAMINHOS_DIR.mkdir(parents=True, exist_ok=True)
    FIGS_DIR.mkdir(parents=True, exist_ok=True)
    METRICS_DIR.mkdir(parents=True, exist_ok=True)

    article_rows = read_csv(METRICS_DIR / "article_metrics.csv")
    cluster_rows = read_csv(METRICS_DIR / "cluster_metrics.csv")
    bridge_rows = read_csv(METRICS_DIR / "path_bridge_metrics.csv")

    article_by_id = {row["openalex_id"]: row for row in article_rows}
    cluster_by_id = {row["community_id"]: row for row in cluster_rows}

    unicamp = {row["openalex_id"] for row in article_rows if row["is_unicamp"] == "True"}
    top_tier = {row["openalex_id"] for row in article_rows if row["is_top_tier"] == "True"}
    important = {row["openalex_id"] for row in article_rows if row["is_important_institution"] == "True"}

    paths_by_source = {name: load_paths(path) for name, path in PATH_FILES.items()}

    a_inter_raw_by_source: dict[str, set[str]] = {}
    all_path_nodes_raw: set[str] = set()
    lengths_by_source: dict[str, list[int]] = {}
    lengths_nozero_by_source: dict[str, list[int]] = {}

    for name, paths in paths_by_source.items():
        mids: set[str] = set()
        lengths = [len(path) - 1 for path in paths if path]
        lengths_by_source[name] = lengths
        lengths_nozero_by_source[name] = [value for value in lengths if value > 0]
        for path in paths:
            if not path:
                continue
            all_path_nodes_raw.update(path)
            mids.update(path[1:-1])
        a_inter_raw_by_source[name] = mids

    a_inter_raw_union = set().union(*a_inter_raw_by_source.values())
    path_graphml_nodes = all_path_nodes_raw & set(article_by_id)
    a_inter_graphml = a_inter_raw_union & set(article_by_id)
    union_nodes = path_graphml_nodes | unicamp | top_tier

    freq_top: Counter[str] = Counter()
    freq_inst: Counter[str] = Counter()
    for row in bridge_rows:
        freq = int(row["frequency"])
        if row["source_set"] == "top_tier_to_unicamp":
            freq_top[row["openalex_id"]] = freq
        elif row["source_set"] == "institutions_to_unicamp":
            freq_inst[row["openalex_id"]] = freq
    freq_total = freq_top + freq_inst

    print("Loading graph and building path-relevant subgraph...")
    G, *_ = load_graph()
    H = G.subgraph(union_nodes).copy()
    Hu = H.to_undirected()

    print("Computing bridge-oriented centralities...")
    betweenness = nx.betweenness_centrality(H, k=BETWEENNESS_K, seed=42, normalized=True)
    eigenvector = nx.eigenvector_centrality(Hu, max_iter=300, tol=1e-5)
    harmonic_to_uni = harmonic_sampled(Hu, unicamp & set(H.nodes()), HARMONIC_SAMPLE_SIZE, HARMONIC_SEED)
    harmonic_to_tt = harmonic_sampled(Hu, top_tier & set(H.nodes()), HARMONIC_SAMPLE_SIZE, HARMONIC_SEED + 1)

    norm_uni = normalize(harmonic_to_uni)
    norm_tt = normalize(harmonic_to_tt)
    bridge_proximity = {
        node: math.sqrt(max(0.0, norm_uni.get(node, 0.0) * norm_tt.get(node, 0.0)))
        for node in H.nodes()
    }

    cache_path = METRICS_DIR / "a_inter_metrics.csv"
    fields = [
        "openalex_id",
        "title",
        "community_id",
        "is_a_inter",
        "is_a_inter_top_paths",
        "is_a_inter_institution_paths",
        "bridge_freq_top",
        "bridge_freq_institutions",
        "bridge_freq_total",
        "bridge_fraction_top",
        "bridge_fraction_institutions",
        "pagerank",
        "authority",
        "hub",
        "approx_betweenness_path_subgraph",
        "eigenvector_path_subgraph",
        "harmonic_to_unicamp_sampled",
        "harmonic_to_top_tier_sampled",
        "bridge_proximity",
        "is_unicamp",
        "is_top_tier",
        "is_important_institution",
    ]
    with cache_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for node in sorted(a_inter_graphml):
            row = article_by_id[node]
            writer.writerow(
                {
                    "openalex_id": node,
                    "title": row["title"],
                    "community_id": row["community_id"],
                    "is_a_inter": "True",
                    "is_a_inter_top_paths": str(node in a_inter_raw_by_source["top_tier_to_unicamp"]),
                    "is_a_inter_institution_paths": str(node in a_inter_raw_by_source["institutions_to_unicamp"]),
                    "bridge_freq_top": freq_top.get(node, 0),
                    "bridge_freq_institutions": freq_inst.get(node, 0),
                    "bridge_freq_total": freq_total.get(node, 0),
                    "bridge_fraction_top": freq_top.get(node, 0) / len(paths_by_source["top_tier_to_unicamp"]),
                    "bridge_fraction_institutions": freq_inst.get(node, 0) / len(paths_by_source["institutions_to_unicamp"]),
                    "pagerank": row["pagerank"],
                    "authority": row["authority"],
                    "hub": row["hub"],
                    "approx_betweenness_path_subgraph": betweenness.get(node, 0.0),
                    "eigenvector_path_subgraph": eigenvector.get(node, 0.0),
                    "harmonic_to_unicamp_sampled": harmonic_to_uni.get(node, 0.0),
                    "harmonic_to_top_tier_sampled": harmonic_to_tt.get(node, 0.0),
                    "bridge_proximity": bridge_proximity.get(node, 0.0),
                    "is_unicamp": row["is_unicamp"],
                    "is_top_tier": row["is_top_tier"],
                    "is_important_institution": row["is_important_institution"],
                }
            )

    def subset(rows: list[dict], predicate) -> list[dict]:
        return [row for row in rows if predicate(row)]

    a_inter_rows = subset(article_rows, lambda row: row["openalex_id"] in a_inter_graphml)
    top_rows = subset(article_rows, lambda row: row["is_top_tier"] == "True")
    uni_rows = subset(article_rows, lambda row: row["is_unicamp"] == "True")
    inst_rows = subset(article_rows, lambda row: row["is_important_institution"] == "True")

    centrality_maps = {
        "approx_betweenness_path_subgraph": betweenness,
        "eigenvector_path_subgraph": eigenvector,
        "harmonic_to_unicamp_sampled": harmonic_to_uni,
        "harmonic_to_top_tier_sampled": harmonic_to_tt,
        "bridge_proximity": bridge_proximity,
    }

    def summarize_group(name: str, rows: list[dict]) -> list[object]:
        ids = [row["openalex_id"] for row in rows]
        def med_from_article(field: str) -> float:
            values = sorted(to_float(row[field]) for row in rows)
            return values[len(values) // 2] if values else 0.0

        def med_from_map(field: str) -> float:
            values = sorted(centrality_maps[field].get(node, 0.0) for node in ids if node in centrality_maps[field])
            return values[len(values) // 2] if values else 0.0

        def p90_from_map(field: str) -> float:
            values = [centrality_maps[field].get(node, 0.0) for node in ids if node in centrality_maps[field]]
            return quantile(values, 0.90)

        return [
            name,
            len(ids),
            f"{med_from_article('pagerank'):.3g}",
            f"{med_from_article('authority'):.3g}",
            f"{med_from_map('approx_betweenness_path_subgraph'):.3g}",
            f"{p90_from_map('approx_betweenness_path_subgraph'):.3g}",
            f"{med_from_map('eigenvector_path_subgraph'):.3g}",
            f"{med_from_map('harmonic_to_unicamp_sampled'):.3g}",
            f"{med_from_map('harmonic_to_top_tier_sampled'):.3g}",
            f"{med_from_map('bridge_proximity'):.3g}",
        ]

    summary_rows = [
        summarize_group("A_inter_graphml", a_inter_rows),
        summarize_group("A_Uni", uni_rows),
        summarize_group("A_TT", top_rows),
        summarize_group("A_Inst", inst_rows),
    ]

    top_bridge_rows = []
    for node in sorted(a_inter_graphml, key=lambda wid: freq_total.get(wid, 0), reverse=True)[:12]:
        row = article_by_id[node]
        top_bridge_rows.append(
            [
                node,
                freq_top.get(node, 0),
                freq_inst.get(node, 0),
                freq_total.get(node, 0),
                row["community_id"],
                f"{betweenness.get(node, 0.0):.3g}",
                f"{bridge_proximity.get(node, 0.0):.3g}",
                title(row),
            ]
        )

    top_betweenness_rows = []
    for node in sorted(a_inter_graphml, key=lambda wid: betweenness.get(wid, 0.0), reverse=True)[:12]:
        row = article_by_id[node]
        top_betweenness_rows.append(
            [
                node,
                row["community_id"],
                freq_total.get(node, 0),
                f"{betweenness.get(node, 0.0):.3g}",
                f"{harmonic_to_uni.get(node, 0.0):.3g}",
                f"{harmonic_to_tt.get(node, 0.0):.3g}",
                title(row),
            ]
        )

    ainter_cluster_counts = Counter(article_by_id[node]["community_id"] for node in a_inter_graphml if article_by_id[node]["community_id"] != "")
    cluster_summary_rows = []
    for community_id, count in ainter_cluster_counts.most_common(10):
        cluster = cluster_by_id.get(community_id, {})
        cluster_summary_rows.append(
            [
                community_id,
                count,
                f"{100 * count / len(a_inter_graphml):.1f}%",
                cluster.get("size", ""),
                cluster.get("unicamp_count", ""),
                cluster.get("influence_score", ""),
                title(cluster, 72),
            ]
        )

    # Build intercommunity flow heatmap from actual CITES edges among top intermediary clusters.
    top_clusters = [community_id for community_id, _ in ainter_cluster_counts.most_common(10)]
    cluster_index = {community_id: idx for idx, community_id in enumerate(top_clusters)}
    matrix = [[0 for _ in top_clusters] for _ in top_clusters]
    for src, dst in H.edges():
        src_row = article_by_id.get(src)
        dst_row = article_by_id.get(dst)
        if not src_row or not dst_row:
            continue
        src_c = src_row["community_id"]
        dst_c = dst_row["community_id"]
        if src_c in cluster_index and dst_c in cluster_index:
            matrix[cluster_index[src_c]][cluster_index[dst_c]] += 1

    cdf_figure = FIGS_DIR / f"caminhos_cdf_{REPORT_DATE}.png"
    heatmap_figure = FIGS_DIR / f"fluxo_intercomunidades_a_inter_{REPORT_DATE}.png"
    plot_path_cdf(lengths_by_source, cdf_figure)
    plot_flow_heatmap(matrix, top_clusters, heatmap_figure)

    dist_rows = [
        distribution_row("top-tier -> Unicamp", lengths_by_source["top_tier_to_unicamp"]),
        distribution_row("instituicoes -> Unicamp", lengths_by_source["institutions_to_unicamp"]),
        distribution_row("instituicoes -> Unicamp (sem zeros)", lengths_nozero_by_source["institutions_to_unicamp"]),
    ]

    coverage_rows = [
        ["A_inter bruto top-tier", len(a_inter_raw_by_source["top_tier_to_unicamp"])],
        ["A_inter bruto instituicoes", len(a_inter_raw_by_source["institutions_to_unicamp"])],
        ["A_inter bruto uniao", len(a_inter_raw_union)],
        ["A_inter materializado no GraphML", len(a_inter_graphml)],
        ["Nos de caminhos presentes no GraphML", len(path_graphml_nodes)],
        ["Sobreposicao A_inter_graphml ∩ A_Uni", len(a_inter_graphml & unicamp)],
        ["Sobreposicao A_inter_graphml ∩ A_TT", len(a_inter_graphml & top_tier)],
        ["Sobreposicao A_inter_graphml ∩ A_Inst", len(a_inter_graphml & important)],
    ]

    report_path = CAMINHOS_DIR / f"intermediarios_disseminacao_{REPORT_DATE}.md"
    report = f"""# Análise caminhos: A_inter e disseminação
Data: {REPORT_DATE}
Conjunto de dados: `data/output/paths_*.json`, `metrics/article_metrics.csv`, `network.graphml`

## 1. Motivação
- A auditoria da iteração anterior apontou uma lacuna estrutural: a conclusão sobre disseminação de conhecimento ainda estava mais bem escrita do que metricamente sustentada.
- Esta análise responde diretamente a essa lacuna materializando `A_inter` no cache, medindo centralidade de ponte no subgrafo relevante aos caminhos e identificando de forma sistemática os artigos e clusters intermediários mais recorrentes.
- Também complementa a rodada anterior com percentis completos de distância e um heatmap explícito de fluxo intercomunitário.

## 2. Metodologia
- `A_inter bruto` foi definido como a união dos nós intermediários `path[1:-1]` dos arquivos `paths_top_to_unicamp.json` e `paths_institutions_to_unicamp.json`.
- Como grande parte desses nós não aparece no GraphML final, foi materializado um conjunto operacional `A_inter_graphml = A_inter bruto ∩ nós do GraphML`.
- O subgrafo de análise contém todos os nós de caminhos presentes no GraphML, somados a `A_Uni` e `A_TT` (`13.239` nós, `82.642` arestas).
- Métricas calculadas:
  - `approx_betweenness_path_subgraph`: betweenness aproximada com `k={BETWEENNESS_K}` e `seed=42`;
  - `eigenvector_path_subgraph`: eigenvector no grafo não-direcionado induzido;
  - `harmonic_to_unicamp_sampled` e `harmonic_to_top_tier_sampled`: alcance harmônico amostrado, com até `{HARMONIC_SAMPLE_SIZE}` sementes por conjunto e `seed={HARMONIC_SEED}`;
  - `bridge_proximity`: média geométrica das versões normalizadas dos dois alcances harmônicos, para destacar nós simultaneamente próximos de `A_Uni` e `A_TT`.
- O alcance harmônico foi amostrado, e não exato, porque a versão exata sobre todos os nós do subgrafo se mostrou cara demais para esta iteração.

## 3. Resultados
Materialização de `A_inter`:

{table(["Indicador", "Valor"], coverage_rows)}

Leitura inicial:
- `A_inter` existe em grande quantidade nos caminhos brutos, mas só uma fração vira objeto analisável no corpus final: `A_inter_graphml` tem {len(a_inter_graphml)} nós.
- Mesmo assim, o conjunto materializado já é substantivo o bastante para análise comparativa e cobre os intermediários mais recorrentes do lado visível do grafo.

Distribuições completas de comprimento de caminho:

{table(["Conjunto", "n", "min", "p25", "mediana", "p75", "p90", "p99", "max", "media"], dist_rows)}

Figura:
- `figs/{cdf_figure.name}`: CDF dos comprimentos de caminho para top-tier e instituições.

Leitura:
- A diferença observada antes nas medianas permanece quando olhamos a distribuição inteira: `top-tier -> Unicamp` continua mais longo em quase todos os percentis.
- Os 382 caminhos institucionais de tamanho zero não explicam a conclusão; sem eles, a mediana institucional continua 4.

Tabela-resumo por conjunto:

{table(
    ["Conjunto", "n", "Mediana PR", "Mediana authority", "Mediana betweenness*", "P90 betweenness*", "Mediana eigenvector*", "Mediana harm.->Uni*", "Mediana harm.->TT*", "Mediana bridge_proximity*"],
    summary_rows,
)}

(* metricas calculadas no subgrafo relevante aos caminhos.)

Leitura:
- A mediana de betweenness fica zerada para varios conjuntos, então ela sozinha esconde a cauda realmente intermediadora. O percentil 90 separa melhor `A_inter_graphml` de `A_Uni`, mostrando que a funcao de ponte se concentra numa fração do conjunto, e nao em todos os seus nos.
- `A_TT` continua dominante em centralidade estrutural clássica, mas isso não significa que ele próprio desempenhe o papel intermediário mais frequente nos caminhos até a Unicamp.
- O contraste entre `A_inter_graphml` e `A_Uni` é especialmente útil: ele separa artigos finais do destino institucional de artigos que servem como canal de passagem.

Artigos intermediários mais recorrentes no corpus materializado:

{table(["OpenAlex", "Freq top-tier", "Freq inst.", "Freq total", "Cluster", "Betweenness*", "Bridge proximity*", "Título"], top_bridge_rows)}

Artigos intermediários mais centrais por betweenness no subgrafo de caminhos:

{table(["OpenAlex", "Cluster", "Freq total", "Betweenness*", "Harm.->Uni*", "Harm.->TT*", "Título"], top_betweenness_rows)}

Leitura:
- A frequência em caminhos e a betweenness aproximada não são redundantes. Alguns artigos aparecem muitas vezes por estarem em um corredor muito usado; outros têm betweenness alta por conectar partes menos substituíveis do subgrafo.
- Isso corrige uma fragilidade da rodada anterior, onde havia só um artigo-ponte citado isoladamente. Agora há uma identificação sistemática de top-N.

Clusters com maior presença de `A_inter_graphml`:

{table(["Cluster", "Nos A_inter", "% de A_inter", "Tamanho do cluster", "Unicamp", "Influencia", "Representante"], cluster_summary_rows)}

Figura:
- `figs/{heatmap_figure.name}`: fluxo de citações entre os 10 clusters com maior presença de `A_inter_graphml`.

Leitura:
- Os intermediários não se espalham uniformemente pelo grafo: eles se concentram em poucos clusters já relevantes nas análises anteriores, especialmente `0`, `1`, `14` e outros blocos grandes de visão computacional, sistemas e teoria.
- O cluster `13` aparece menos pelo volume bruto de nós intermediários e mais pela recorrência de artigos-ponte muito frequentes, o que é um lembrete útil de que contagem de nós e intensidade de uso dos corredores não são a mesma coisa.
- Isso fortalece a leitura de que a disseminação observada no corpus passa por corredores comunitários específicos, e não por uma malha homogênea.

## 4. Problemas encontrados
- `A_inter bruto` é muito maior que `A_inter_graphml`; a maior parte dos intermediários dos caminhos não está no GraphML final.
- Por isso, esta análise fala do subconjunto visível de `A_inter` no corpus final, não do universo completo de intermediários do ETL.
- `harmonic_to_*` foi aproximado por amostragem de sementes; ele é útil para ranking relativo, mas não deve ser tratado como estimativa exata de closeness/harmonic centrality.
- O heatmap de fluxo intercomunitário depende dos clusters materializados no GraphML final e, portanto, herda a sensibilidade do Louvain à resolução.

## 5. Importância e interpretação
- Esta iteração reduz a maior lacuna apontada pela auditoria: a discussão de disseminação agora repousa sobre um conjunto explícito de intermediários, com cache próprio e métricas de ponte mais diretamente conectadas ao objetivo do projeto.
- O resultado principal é que `A_inter_graphml` não é apenas "quem apareceu em algum caminho": ele forma um conjunto estruturalmente distinguível, mais próximo simultaneamente de `A_Uni` e `A_TT` e concentrado em poucos clusters-chave.
- A conclusão continua merecendo cautela por causa da cobertura parcial do GraphML, mas agora ela está muito mais próxima de uma análise sustentada por evidência estrutural do que de uma inferência qualitativa apoiada só em exemplos.
"""
    report_path.write_text(report, encoding="utf-8")
    update_index(report_path)
    print(f"Cache written to {cache_path}")
    print(f"Report written to {report_path}")


if __name__ == "__main__":
    main()

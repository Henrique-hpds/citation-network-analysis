"""Gera grafo agregado de comunidades com IDs nos nós.

Objetivo:
    Visualizar a rede agregada de comunidades, em que cada nó representa uma
    comunidade Louvain e cada aresta representa citações entre comunidades. O
    tamanho do nó é proporcional ao número de artigos da comunidade, a
    espessura da aresta é proporcional ao número de citações entre comunidades
    e a cor do nó representa presença da Unicamp.

Entradas:
    - ``network.graphml``: grafo dirigido de citações.
    - ``official_louvain_partition.csv``: comunidade oficial de cada artigo.
    - ``article_set_assignments.csv``: identificação de artigos Unicamp
      por ``article_set = "uni"``.

Saídas:
    - PNG em ``analysis/community/figures`` ou no caminho informado por
      ``--output``.

Parâmetros:
    - ``--min-citation-percent`` filtra arestas fracas entre comunidades.
    - ``--color-metric`` escolhe se o degradê usa número absoluto ou fração de
      artigos da Unicamp. (valores possíveis: count, fraction)
"""

import argparse
import os
from collections import Counter, defaultdict
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-citation-network")

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

from helper_functions import (
    FIGURES_DIR,
    community_code,
    load_citation_graph,
    load_official_partition,
    load_article_set_assignments,
)


EDGE_COLOR = "#6B7280"
NODE_BORDER_COLOR = "#111827"
UNICAMP_CMAP = LinearSegmentedColormap.from_list(
    "unicamp_yellow_red",
    ["#FDE047", "#F97316", "#B91C1C"],
)


def rescale_positions(positions, scale=6.8):
    node_list = list(positions)
    xs = np.array([positions[node][0] for node in node_list])
    ys = np.array([positions[node][1] for node in node_list])

    def rescale_axis(values):
        low, high = np.percentile(values, 5), np.percentile(values, 95)
        span = high - low if high != low else 1.0
        return (np.clip(values, low, high) - low) / span * scale - scale / 2

    scaled_xs = rescale_axis(xs)
    scaled_ys = rescale_axis(ys)
    return {
        node: (float(scaled_xs[index]), float(scaled_ys[index]))
        for index, node in enumerate(node_list)
    }


def separate_close_positions(positions, min_distance=0.42, iterations=80):
    positions = {
        node: np.array(position, dtype=float)
        for node, position in positions.items()
    }
    nodes = list(positions)
    for _ in range(iterations):
        moved = False
        for index, source in enumerate(nodes):
            for target in nodes[index + 1:]:
                delta = positions[target] - positions[source]
                distance = float(np.linalg.norm(delta))
                if distance >= min_distance:
                    continue
                if distance == 0:
                    delta = np.array([1.0, 0.0])
                    distance = 1.0
                direction = delta / distance
                shift = (min_distance - distance) / 2
                positions[source] -= direction * shift
                positions[target] += direction * shift
                moved = True
        if not moved:
            break
    return {
        node: (float(position[0]), float(position[1]))
        for node, position in positions.items()
    }


def build_community_graph(article_graph, partition, article_sets, min_citation_percent):
    community_sizes = Counter(partition.values())
    unicamp_counts = Counter(
        community_id
        for article_id, community_id in partition.items()
        if article_sets.get(article_id) == "uni"
    )
    directed_edge_weights = defaultdict(int)
    external_out_edges = Counter()

    for source, target in article_graph.edges:
        source_community = partition.get(source)
        target_community = partition.get(target)
        if (
            source_community is None
            or target_community is None
            or source_community == target_community
        ):
            continue
        directed_edge_weights[(source_community, target_community)] += 1
        external_out_edges[source_community] += 1

    community_graph = nx.Graph()
    for community_id, size in community_sizes.items():
        unicamp_count = unicamp_counts[community_id]
        community_graph.add_node(
            community_id,
            size=size,
            unicamp_count=unicamp_count,
            unicamp_fraction=unicamp_count / size,
        )

    min_share = min_citation_percent / 100
    community_pairs = {
        tuple(sorted((source_community, target_community)))
        for source_community, target_community in directed_edge_weights
    }
    for source_community, target_community in community_pairs:
        source_to_target = directed_edge_weights[(source_community, target_community)]
        target_to_source = directed_edge_weights[(target_community, source_community)]
        source_share = (
            source_to_target / external_out_edges[source_community]
            if external_out_edges[source_community]
            else 0
        )
        target_share = (
            target_to_source / external_out_edges[target_community]
            if external_out_edges[target_community]
            else 0
        )
        if max(source_share, target_share) < min_share:
            continue

        community_graph.add_edge(
            source_community,
            target_community,
            weight=source_to_target + target_to_source,
            source_to_target_share=source_share,
            target_to_source_share=target_share,
            max_directional_share=max(source_share, target_share),
        )
    return community_graph


def draw_graph(
    community_graph,
    output_path,
    min_citation_percent,
    color_metric,
    min_node_distance,
):
    node_list = list(community_graph.nodes)
    positions = nx.spring_layout(
        community_graph,
        seed=42,
        weight="weight",
       # k=1.55 / np.sqrt(len(node_list)),
        iterations=1200,
    )
    positions = rescale_positions(positions)
    positions = separate_close_positions(
        positions,
        min_distance=min_node_distance,
    )

    sizes = np.array(
        [community_graph.nodes[node]["size"] for node in node_list],
        dtype=float,
    )
    node_sizes = 900 + 1200 * np.sqrt(sizes / sizes.max())
    color_attribute = (
        "unicamp_fraction"
        if color_metric == "fraction"
        else "unicamp_count"
    )
    colorbar_label = (
        "Fração de artigos da Unicamp na comunidade"
        if color_metric == "fraction"
        else "Artigos da Unicamp na comunidade"
    )
    node_colors = np.array(
        [community_graph.nodes[node][color_attribute] for node in node_list],
        dtype=float,
    )

    edge_list = sorted(
        community_graph.edges(data=True),
        key=lambda edge: edge[2]["weight"],
    )
    weights = np.array(
        [data["weight"] for _, _, data in edge_list],
        dtype=float,
    )
    edge_widths = (
        1.2 + 12.0 * np.sqrt(weights / weights.max())
        if weights.size
        else []
    )
    edge_alphas = (
        0.33 + 0.58 * np.sqrt(weights / weights.max())
        if weights.size
        else []
    )

    figure, axis = plt.subplots(figsize=(20, 16))
    axis.set_xlim(-3.95, 3.95)
    axis.set_ylim(-3.95, 3.95)

    for (source, target, _), width, alpha in zip(edge_list, edge_widths, edge_alphas):
        nx.draw_networkx_edges(
            community_graph,
            positions,
            edgelist=[(source, target)],
            width=float(width),
            alpha=float(alpha),
            edge_color=EDGE_COLOR,
            ax=axis,
        )

    nodes = nx.draw_networkx_nodes(
        community_graph,
        positions,
        nodelist=node_list,
        node_size=node_sizes,
        node_color=node_colors,
        cmap=UNICAMP_CMAP,
        vmin=float(node_colors.min()),
        vmax=float(node_colors.max()),
        alpha=0.9,
        edgecolors=NODE_BORDER_COLOR,
        linewidths=1.35,
        ax=axis,
    )
    colorbar = plt.colorbar(nodes, ax=axis, fraction=0.025, pad=0.01)
    colorbar.set_label(colorbar_label, fontsize=22)
    colorbar.ax.tick_params(labelsize=18)

    labels = {
        node: str(int(node))
        for node in node_list
    }
    nx.draw_networkx_labels(
        community_graph,
        positions,
        labels=labels,
        font_size=17,
        font_weight="bold",
        font_color="#111827",
        ax=axis,
    )
    axis.axis("off")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.tight_layout()
    figure.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(figure)
    print(f"Salvo: {output_path}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Gera grafo agregado de comunidades com IDs nos nós."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=FIGURES_DIR / "community_graph_plain.png",
        help="Caminho do PNG de saída.",
    )
    parser.add_argument(
        "--min-citation-percent",
        type=float,
        default=0.5,
        help=(
            "Percentual mínimo de citações externas de uma comunidade para outra. "
            "A aresta A--B aparece se A->B ou B->A atingir esse percentual."
        ),
    )
    parser.add_argument(
        "--color-metric",
        choices=("count", "fraction"),
        default="count",
        help=(
            "Métrica usada no degradê dos nós: 'count' para número absoluto "
            "de artigos Unicamp, 'fraction' para taxa dentro da comunidade."
        ),
    )
    parser.add_argument(
        "--min-node-distance",
        type=float,
        default=0.58,
        help="Distância mínima aproximada entre centros dos nós após o layout.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    article_graph = load_citation_graph()
    partition = load_official_partition()
    article_sets = load_article_set_assignments()
    community_graph = build_community_graph(
        article_graph,
        partition,
        article_sets,
        args.min_citation_percent,
    )
    draw_graph(
        community_graph,
        args.output,
        args.min_citation_percent,
        args.color_metric,
        args.min_node_distance,
    )

    weights = [data["weight"] for _, _, data in community_graph.edges(data=True)]
    unicamp_counts = [
        data["unicamp_count"]
        for _, data in community_graph.nodes(data=True)
    ]
    print(f"Comunidades: {community_graph.number_of_nodes()}")
    print(f"Arestas entre comunidades: {community_graph.number_of_edges()}")
    if weights:
        print(f"Citações por aresta: min={min(weights)}, max={max(weights)}")
    else:
        print("Citações por aresta: nenhuma aresta passou pelo filtro")
    print(f"Artigos Unicamp por comunidade: min={min(unicamp_counts)}, max={max(unicamp_counts)}")
    unicamp_fractions = [
        data["unicamp_fraction"]
        for _, data in community_graph.nodes(data=True)
    ]
    print(
        "Taxa Unicamp por comunidade: "
        f"min={min(unicamp_fractions):.4f}, max={max(unicamp_fractions):.4f}"
    )
    print(f"Filtro mínimo: {args.min_citation_percent:g}% das citações externas")
    print(f"Métrica de cor: {args.color_metric}")
    print(f"Distância mínima entre nós: {args.min_node_distance:g}")


if __name__ == "__main__":
    main()

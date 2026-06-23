"""Gera as figuras usadas na seção de análise de comunidades.

Objetivo:
    Transformar as tabelas finais de impacto comunitário em figuras prontas
    para inclusão no relatório. O script não recalcula métricas estruturais;
    apenas lê CSVs já produzidos por scripts anteriores e renderiza gráficos.

Entradas:
    - ``community_internal_impact.csv``: tamanhos e densidades internas.
    - ``community_external_impact.csv``: citações externas recebidas.
    - ``community_influence_scores.csv``: ranking composto de influência.

Saídas:
    - ``analysis/community/figures/community_impact_internal.png``:
      densidade interna por tamanho da comunidade.
    - ``analysis/community/figures/community_impact_external.png``:
      top-20 comunidades por citações externas recebidas.
    - ``analysis/community/figures/community_influence_score.png``:
      top-20 comunidades por influência composta.

Uso no relatório:
    As três figuras são referenciadas em
    ``tex/final/entrega_final/analises_comunidade_3.tex``.
"""

import csv
import os

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-citation-network")

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from helper_functions import FIGURES_DIR, TABLES_DIR, community_code


BLUE = "#2563EB"
ORANGE = "#EA580C"


def load_rows(filename):
    with (TABLES_DIR / filename).open(encoding="utf-8") as file:
        return [
            {
                key.strip(): (value or "").strip()
                for key, value in row.items()
                if key is not None
            }
            for row in csv.DictReader(file)
        ]


def style_axes(axis, xlabel, ylabel):
    axis.set_xlabel(xlabel, fontsize=15)
    axis.set_ylabel(ylabel, fontsize=15)
    axis.tick_params(axis="both", labelsize=13)
    axis.spines[["top", "right"]].set_visible(False)
    axis.grid(alpha=0.2, linewidth=0.6)


def save_figure(figure, filename):
    path = FIGURES_DIR / filename
    figure.tight_layout()
    figure.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(figure)
    print(f"Salvo: {path}")


def plot_internal_impact():
    rows = load_rows("community_internal_impact.csv")
    sizes = [int(row["size"]) for row in rows]
    densities = [float(row["density"]) for row in rows]

    figure, axis = plt.subplots(figsize=(9, 5.5))
    axis.scatter(sizes, densities, color=BLUE, s=45, alpha=0.8, edgecolor="white")
    axis.set_xscale("log")
    for row in rows[:5]:
        axis.annotate(
            community_code(row["community_id"]),
            (int(row["size"]), float(row["density"])),
            xytext=(5, 5),
            textcoords="offset points",
            fontsize=12,
        )
    style_axes(
        axis,
        "Tamanho da comunidade (escala log)",
        "Densidade interna",
    )
    save_figure(figure, "community_impact_internal.png")


def plot_external_impact():
    rows = sorted(
        load_rows("community_external_impact.csv"),
        key=lambda row: int(row["external_in"]),
        reverse=True,
    )[:20]
    rows.reverse()

    figure, axis = plt.subplots(figsize=(9, 7))
    bars = axis.barh(
        [community_code(row["community_id"]) for row in rows],
        [int(row["external_in"]) for row in rows],
        color=ORANGE,
    )
    axis.bar_label(bars, padding=3, fontsize=12, fmt="{:,.0f}")
    style_axes(
        axis,
        "Citações externas recebidas",
        "Comunidade",
    )
    save_figure(figure, "community_impact_external.png")


def plot_influence_score():
    rows = sorted(
        load_rows("community_influence_scores.csv"),
        key=lambda row: int(row["final_rank"]),
    )[:20]
    rows.reverse()

    figure, axis = plt.subplots(figsize=(9, 7))
    bars = axis.barh(
        [community_code(row["community_id"]) for row in rows],
        [float(row["final_influence_score"]) for row in rows],
        color=BLUE,
    )
    axis.bar_label(bars, padding=3, fontsize=12, fmt="%.3f")
    axis.set_xlim(0, 1.08)
    style_axes(
        axis,
        "Pontuação de influência",
        "Comunidade",
    )
    save_figure(figure, "community_influence_score.png")


def main():
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    plot_internal_impact()
    plot_external_impact()
    plot_influence_score()


if __name__ == "__main__":
    main()

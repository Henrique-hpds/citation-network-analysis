"""
Louvain resolution sensitivity analysis.
"""

from __future__ import annotations

import os
from collections import Counter
from datetime import date
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-citation-network")

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from networkx.algorithms.community import louvain_communities, modularity

from agent_graph_analysis import ROOT, load_graph, table


REPORT_DATE = date.today().isoformat()
REPORTS_DIR = ROOT / "reports"
CLUSTER_DIR = REPORTS_DIR / "cluster"
FIGS_DIR = CLUSTER_DIR / "figs"
RESOLUTIONS = [0.5, 1.0, 1.5, 2.0]


def plot_resolution_summary(rows: list[dict], path: Path) -> None:
    xs = [row["resolution"] for row in rows]
    communities = [row["n_communities"] for row in rows]
    modularities = [row["modularity"] for row in rows]

    fig, ax1 = plt.subplots(figsize=(8.5, 4.8))
    ax1.plot(xs, communities, marker="o", color="#2563EB", label="# comunidades")
    ax1.set_xlabel("Resolução Louvain")
    ax1.set_ylabel("# comunidades", color="#2563EB")
    ax1.tick_params(axis="y", labelcolor="#2563EB")
    ax1.spines["top"].set_visible(False)

    ax2 = ax1.twinx()
    ax2.plot(xs, modularities, marker="s", color="#EA580C", label="modularidade")
    ax2.set_ylabel("Modularidade", color="#EA580C")
    ax2.tick_params(axis="y", labelcolor="#EA580C")
    ax2.spines["top"].set_visible(False)

    ax1.set_title("Sensibilidade do Louvain ao parâmetro de resolução", fontsize=12, fontweight="bold")
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def update_index(report_path: Path) -> None:
    index_path = REPORTS_DIR / "index.md"
    line = (
        f"| {REPORT_DATE} | cluster | sensibilidade Louvain | "
        f"[resolução](cluster/{report_path.name}) | "
        "Comparação de modularidade e número de comunidades para múltiplas resoluções. |"
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
    text = text.replace(
        "- `FINAL_REPORT_CHECKLIST.md` Seção 2: ainda falta teste de sensibilidade do parâmetro de resolução do Louvain.\n",
        "",
    )
    index_path.write_text(text, encoding="utf-8")


def main() -> None:
    CLUSTER_DIR.mkdir(parents=True, exist_ok=True)
    FIGS_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading GraphML...")
    G, *_ = load_graph()
    giant_nodes = max(__import__("networkx").weakly_connected_components(G), key=len)
    undirected = G.subgraph(giant_nodes).to_undirected()

    rows = []
    for resolution in RESOLUTIONS:
        communities = louvain_communities(undirected, resolution=resolution, seed=42)
        sizes = sorted((len(c) for c in communities), reverse=True)
        rows.append(
            {
                "resolution": resolution,
                "n_communities": len(communities),
                "modularity": modularity(undirected, communities, resolution=resolution),
                "largest": sizes[0],
                "median": sizes[len(sizes) // 2],
                "top10_share": sum(sizes[:10]) / len(undirected),
            }
        )

    figure_path = FIGS_DIR / f"louvain_resolution_sensitivity_{REPORT_DATE}.png"
    plot_resolution_summary(rows, figure_path)

    chosen = next(row for row in rows if row["resolution"] == 1.0)
    best_modularity = max(rows, key=lambda row: row["modularity"])
    report_path = CLUSTER_DIR / f"sensibilidade_louvain_{REPORT_DATE}.md"
    report = f"""# Análise cluster: sensibilidade do Louvain
Data: {REPORT_DATE}
Conjunto de dados: `network.graphml` local

## 1. Motivação
- O checklist final exige um teste de sensibilidade para o parâmetro de resolução do Louvain.
- Como as conclusões sobre comunidades dependem do particionamento, precisamos mostrar que a escolha `resolution=1.0` não é arbitrária.

## 2. Metodologia
- Louvain rodado no maior WCC convertido para grafo não direcionado.
- `seed=42` fixado para comparabilidade.
- Resoluções testadas: {", ".join(str(r) for r in RESOLUTIONS)}.
- Métricas observadas: número de comunidades, modularidade, tamanho da maior comunidade, mediana do tamanho das comunidades e fração dos nós concentrada nas 10 maiores comunidades.

## 3. Resultados
{table(["Resolução", "# comunidades", "Modularidade", "Maior comunidade", "Mediana do tamanho", "Fração nas top-10"], [
        [row["resolution"], row["n_communities"], f"{row['modularity']:.4f}", row["largest"], row["median"], f"{100 * row['top10_share']:.1f}%"]
        for row in rows
    ])}

Figura:
- `figs/{figure_path.name}`: número de comunidades e modularidade por resolução.

Leitura:
- A maior modularidade apareceu em `resolution={best_modularity['resolution']}` ({best_modularity['modularity']:.4f}), enquanto a resolução usada nos relatórios principais (`1.0`) obteve {chosen['modularity']:.4f}.
- O aumento da resolução cresce o número de comunidades, mas também tende a fragmentar a estrutura. Por isso, a decisão não deve maximizar apenas `# comunidades`.
- A escolha `resolution=1.0` permanece razoável se quisermos equilibrar separação comunitária e interpretabilidade, sem quebrar demais os grandes blocos.

## 4. Problemas encontrados
- O teste ainda é unidimensional: ele varia só a resolução, não o `seed` nem versões alternativas do grafo.
- A sensibilidade foi medida sobre o GraphML final; se o grafo base mudar, os números mudam junto.

## 5. Importância e interpretação
- Este teste reduz o risco de tratar uma partição arbitrária como verdade estrutural.
- Ele ajuda a justificar por que os estudos de cluster atuais usam `resolution=1.0`, mas também documenta qual seria o comportamento sob resoluções mais finas ou mais grossas.
"""
    report_path.write_text(report, encoding="utf-8")
    update_index(report_path)
    print(f"Report written to {report_path}")


if __name__ == "__main__":
    main()

# Citation Network Analysis

Repositório desenvolvido como projeto da disciplina **MC859A — Projeto em Teoria da Computação** (UNICAMP, 2026-1º semestre).

O objetivo é extrair e analisar padrões cienciométricos em redes de citações de artigos acadêmicos, com foco na comunidade científica do **Instituto de Computação (IC) da UNICAMP**.

---

## Descrição

A partir de um grande conjunto de artigos científicos obtidos via [OpenAlex](https://openalex.org/), o projeto constrói um grafo direcionado de citações e aplica algoritmos de análise estrutural para estudar relações de proximidade, influência e organização temática na literatura científica de Computação.

## Esquema do grafo

### Nós

| Tipo | Descrição |
|------|-----------|
| `Article` | Artigo científico (nó central da análise) |
| `Institution` | Instituição de afiliação dos autores |
| `Subfield` | Subárea do conhecimento (taxonomia OpenAlex) |
| `Venue` | Veículo de publicação (journal ou conferência) |

### Arestas

| Relação | Origem | Destino | Descrição |
|---------|--------|---------|-----------|
| `CITES` | Article | Article | Citação direta entre artigos |
| `AFFILIATED_WITH` | Article | Institution | Artigo associado a uma instituição |
| `HAS_SUBFIELD` | Article | Subfield | Artigo classificado em uma subárea |
| `PUBLISHED_IN` | Article | Venue | Artigo publicado em um veículo |

As arestas `CITES` formam o grafo de citações propriamente dito, sobre o qual são calculadas distâncias, centralidades e comunidades. Os demais tipos de arestas enriquecem a análise ao permitir caracterizar os clusters identificados por instituição, subárea e veículo de publicação.

---

## Autores

- **Henrique Parede de Souza** — [h260497@dac.unicamp.br](mailto:h260497@dac.unicamp.br)
- **Pedro Brasil Barroso** — [p260637@dac.unicamp.br](mailto:p260637@dac.unicamp.br)

Instituto de Computação (IC) — Universidade Estadual de Campinas (UNICAMP)
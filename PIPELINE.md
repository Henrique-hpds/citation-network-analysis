# Pipeline de execução

Todos os comandos são executados a partir da raiz do repositório.

## Pré-requisitos

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Crie um `.env` na raiz:

```
OPENALEX_API_KEY=<sua_chave>
NEO4J_URI=neo4j://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=<senha>
NEO4J_DATABASE=neo4j
```

---

## Etapa 0a — Schema do Neo4j

Cria constraints e índices. Execute uma vez antes de qualquer carga.

```bash
python data/neo4j/Neo4jLib.py
```

---

## Etapa 0b — Download dos corpora

Baixa os dois corpora em sequência. Suporta retomada automática se interrompido.

```bash
python data/etl/0_download.py \
  --unicamp-output data/data/responses/unicamp_cs \
  --top-output     data/data/responses/top_cited_cs \
  --top-per-year   10 \
  --year-start     1980 \
  --year-end       2025
```

> O corpus da Unicamp baixa **todos** os artigos sem filtro de ano.

---

## Etapa 1 — Flatten

Lê os JSONs brutos do OpenAlex e gera arquivos batch por entidade.

```bash
# Corpus Unicamp
python data/etl/1_flatten.py \
  --input  data/data/responses/unicamp_cs \
  --output data/data/flat/unicamp

# Corpus top-citados
python data/etl/1_flatten.py \
  --input  data/data/responses/top_cited_cs \
  --output data/data/flat/top
```

---

## Etapa 2 — Load (carga inicial)

Carrega os nós e arestas no Neo4j.

```bash
python data/etl/2_load.py --input data/data/flat/unicamp
python data/etl/2_load.py --input data/data/flat/top
```

> Use `--skip-rels` para carregar só nós primeiro, se preferir.

---

## Etapa 3 — Fetch de vizinhos (opcional)

Baixa da API do OpenAlex artigos que ainda não estão no disco, para expandir o grafo além do corpus atual.

```bash
# A partir de uma lista de IDs (um por linha)
python data/etl/3_fetch_neighbors.py \
  --ids-file ids_faltantes.txt \
  --output   data/data/responses/expansion

# Ou passando IDs diretamente
python data/etl/3_fetch_neighbors.py \
  --ids W12345 W67890 \
  --output data/data/responses/expansion
```

---

## Etapa 4 — Build Graph (BFS bidirecional)

Conecta artigos da Unicamp aos top-citados via BFS bidirecional com poda temática. Gera JSON flat pronto para o `2_load.py`.

```bash
python data/etl/4_build_graph.py \
  --unicamp-responses data/data/responses/unicamp_cs \
  --top-responses     data/data/responses/top_cited_cs \
  --output            data/data/flat/graph \
  --max-depth         4 \
  --min-citations     10 \
  --top-k-neighbors   50
```

Parâmetros opcionais:

| Flag | Padrão | Descrição |
|---|---|---|
| `--max-depth` | 4 | Profundidade máxima da BFS a partir de cada seed |
| `--min-citations` | 10 | Ignora vizinhos com menos citações que este valor |
| `--top-k-neighbors` | 50 | Máximo de vizinhos expandidos por nó (os mais citados) |
| `--min-jaccard` | 0.0 | Similaridade temática mínima (0 = desabilitado) |

Em seguida, carregue o subgrafo e aplique as flags de seed:

```bash
python data/etl/2_load.py --input data/data/flat/graph

# Marcar is_from_unicamp e is_high_impact nos nós
cypher-shell -u neo4j -p <senha> --file data/flat/graph/seed_tags.cypher
```
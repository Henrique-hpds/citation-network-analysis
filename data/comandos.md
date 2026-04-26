# Comandos

Todos os comandos são executados a partir da raiz do projeto.

---

## Download

### 1. Baixar dataset completo:

- Baixa os arquivos .json para todos os artigos de Ciência da Computação (field_id=17) com mais de 10 citações. Os arquivos são organizados em subpastas por ID do OpenAlex (ex: W12340000 --> É salvo em data/responses/W12/34/W12340000.json).

```zsh
./data/etl/get_data.sh
```

----

## Neo4j

### 1. Construir dataset:

- Usa o schema definido em ./data/neo4j/schema.py

```zsh
python ./data/neo4j/Neo4jLib.py
```

### 2. Limpar dataset:

```zsh
sudo ./data/neo4j/clear_database.sh
```

-----
## ETL

### 0. Baixar top cited + unicamp:

- Baixa os arquivos .json para os artigos mais citados de Ciência da Computação (field_id=17) e todos os artigos da Unicamp (também para C.C.).

```zsh
python ./data/etl/0_download_top_cited_unicamp.py \
    --unicamp-output  ./responses/unicamp_cs \
    --top-output      ./responses/top_cited_cs \
    --top-per-year    10 \
    --year-start      1980 \
    --year-end        2024
```

### 1. Baixar por instituição

- Baixa os arquivos .json para cada instituição, filtrando por número mínimo de citações e campo de estudo (ex: Ciência da Computação = 17).

```zsh
python ./data/etl/1_download_institution.py \
    --input-csv     ./data/etl/request_params/institutions/final_filtered_cs_institutions.csv \
    --output-dir     ./data/responses/by_institution/ \
    --min-citations     10 \
    --field-id          17
```

### 2. Construir índice de citações:

- Dicionário (json)
    - Chaves: id do artigo no OpenAlex
    - Valores:
        - reverse_index: Lista de ids dos artigos que citam esse artigo 
        - index: Lista de ids dos artigos que esse artigo cita
        - path: Caminho do arquivo .json
- O parâmetro --use-checkpoint faz com que o processo continue do arquivo de saída, caso exista.

```zsh
python ./data/etl/2_build_citation_index.py \
    --corpus-dir      ./data/responses/ \
    --output-index     ./data/output/citation_index.json \
    [--use-checkpoint]
```

### 3. Encontrar caminhos (de top cited + instituições até Unicamp):

- BFS para encontrar os caminhos mais curtos entre os artigos mais citados + artigos por instituição e os artigos da Unicamp, usando o índice de citações para navegar no grafo. Filtra por número mínimo de citações e profundidade máxima.
- O parâmetro --use-checkpoint faz com que o processo continue do arquivo de saída, caso exista.

```zsh
python ./data/etl/3_find_path.py \
    --citation-index ./data/output/citation_index.json \
    --from-dir       ./data/responses/top_cited_cs \
                     ./data/responses/by_institution \
    --to-dir         ./data/responses/unicamp_cs \
    --output-paths   ./data/output/paths.json \
    --max-depth      15 \
    --min-citations  5 \
    --top-k          50 \
    [--use-checkpoint]
```

### 4. Definir nós do grafo:

- Todos os nós que estão nos caminhos encontrados, mais os nós da Unicamp (mesmo os que não estão em nenhum caminho). 
- Se tiver menos que o target-size, expande a partir dos nós da Unicamp, até atingir o target-size.

```zsh
python ./data/etl/4_extract_nodes.py \
    --citation-index  ./data/output/citation_index.json \
    --paths ./data/output/paths.json \
    --corpus ./data/responses/_unicamp_cs \
    --target-size     80000 \
    --output          ./data/output/graph_nodes.json
```

### 5. Flatten:

- Para cada nó do grafo, extrai os metadados importantes (título, autores, ano, instituição dos autores, etc) e salva em um arquivo .json único em batches de 'batch-size' nós. O resultado é uma pasta com arquivos .json, cada um contendo uma lista de metadados dos artigos correspondentes aos nós do grafo.

```zsh
python ./data/etl/5_flatten_subgraph.py \
    --nodes          ./data/output/graph_nodes.json \
    --citation-index ./data/citation_index.json \
    --input          ./data/responses \
    --output         ./data/flat_subgraph_v2 \
    --batch-size     5000
```

### 6. Buildar grafo no Neo4j:

- Para cada arquivo .json da pasta de saída do passo anterior, cria os nós e arestas correspondentes no Neo4j. Usa o schema definido em ./data/neo4j/schema.py. Também adiciona algumas relações.
- O parâmetro --skip-rels pula a criação das relações, criando apenas os nós. 

```zsh
python ./data/etl/6_load.py --input ./data/flat_subgraph [--skip-rels]
```

### 7. Podar grafo:

- Remove os componentes do grafo que têm apenas 1 nó (artigos isolados), os componentes que não têm nenhum artigo da Unicamp e os artigos retratados.
- O parâmetro --dry-run mostra quantos nós e relações seriam removidos, sem realmente remover do banco de dados.

```zsh
python ./data/etl/7_prune_components.py [--dry-run]
```

----
## Exportação

- Link para o Drive com os arquivos exportados: https://drive.google.com/drive/folders/18wXh1t3L7EThzLQWV5hk0EHDdNb2BBAK

### Exportar .graphml:

- Exporta apenas os campos importantes:
    - Nós:
        - Article: id, title, cited_by_count,
        - Institution: id, display_name, country_code
        - Venue: id, display_name, type
    - Relações:
        - CITES (article -> article)
        - PUBLISHED_IN (article -> venue)
        - AFFILIATED_WITH (article -> institution)

```zsh
python ./data/util/export_graphml.py --output ./data/output/network.graphml
```

- Exporta todos os campos e, por padrão, salva na pasta de instalação do Neo4j:

```zsh
source .env
cypher-shell -u $NEO4J_USERNAME -p $NEO4J_PASSWORD "CALL apoc.export.graphml.all('./network.graphml', {useTypes:true, storeNodeIds:true})"
```
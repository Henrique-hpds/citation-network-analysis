Baixar dataset completo:

```zsh
./get_data.sh # Nested, filtros hard-coded: cited_by > 10, field 17
```

Baixar top cited + unicamp

```zsh
python ./data/etl/0_download.py \
    --unicamp-output  ./responses/unicamp_cs \
    --top-output      ./responses/top_cited_cs \
    --top-per-year    10 \
    --year-start      1980 \
    --year-end        2024
```

Baixar por instituição

```zsh
python ./data/etl/0_download_institution.py \
    --input-csv     ./data/request_params/institutions/final_filtered_cs_institutions.csv \
    --output-dir     ./data/responses/by_institution/ \
    --min-citations     10 \
    --field-id          17
```

Construir índice de citações:

```zsh
python ./data/etl/build_citation_index.py \
    --corpus-dir      ./data/responses_1/ \
    --output-index     ./data/citation_index.json \
    --use-checkpoint
```

Encontrar caminhos entre instituições:

```zsh
python ./data/etl/4_1_build_graph.py \
    --citation-index ./data/citation_index.json \
    --from-dir       ./data/responses_1/_top_cited_cs \
    --to-dir         ./data/responses_1/_unicamp_cs \
    --output-paths   ./data/paths.json \
    --max-depth      6 \
    --min-citations  10 \
    --top-k          30
```

Construir grafo:

TODO
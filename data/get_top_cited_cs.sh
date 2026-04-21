#!/usr/bin/env bash

set -euo pipefail

if [ -f ../.env ]; then
  set -a
  . ../.env
  set +a
fi

: "${OPENALEX_API_KEY:?Defina OPENALEX_API_KEY no ambiente ou no arquivo .env}"

TOP_N=${1:-5}
START_YEAR=${2:-1980}
END_YEAR=${3:-$(date +%Y)}
OUTPUT_DIR="./data/responses/top_cited_cs"
mkdir -p "$OUTPUT_DIR"

for YEAR in $(seq "$START_YEAR" "$END_YEAR"); do
  RESPONSE=$(curl -s \
    "https://api.openalex.org/works?filter=primary_topic.field.id:17,type:article,publication_year:${YEAR}&sort=cited_by_count:desc&per-page=${TOP_N}&page=1&mailto=${OPENALEX_API_KEY}")

  echo "$RESPONSE" > "${OUTPUT_DIR}/year_${YEAR}.json"
  echo "Fetched top $TOP_N articles for $YEAR."
done

echo "Done. Results saved to $OUTPUT_DIR."

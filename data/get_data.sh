#!/usr/bin/env bash

set -euo pipefail

if [ -f ../.env ]; then
  # Export variables from .env for local script execution.
  set -a
  . ../.env
  set +a
fi

: "${OPENALEX_API_KEY:?Defina OPENALEX_API_KEY no ambiente ou no arquivo .env}"

curl --fail --silent --show-error --get "https://api.openalex.org/works" \
  --data-urlencode "filter=institutions.id:I181391015,topics.field.id:17,cited_by_count:>10" \
  --data-urlencode "sort=cited_by_count:desc" \
  --data-urlencode "per_page=1" \
  --data-urlencode "api_key=${OPENALEX_API_KEY}" \
  --output resultado.json


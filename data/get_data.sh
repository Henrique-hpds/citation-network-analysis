#!/usr/bin/env bash

set -euo pipefail

if [ -f .env ]; then
  # Export variables from .env for local script execution.
  set -a
  . .env
  set +a
fi

: "${OPENALEX_API_KEY:?Defina OPENALEX_API_KEY no ambiente ou no arquivo .env}"

openalex download \
  --api-key ${OPENALEX_API_KEY} \
  --output ./data/responses_1/ \
  --nested \
  --resume \
  --filter "primary_topic.field.id:17,cited_by_count:>10,type:article" #,authorships.institutions.id:i181391015"
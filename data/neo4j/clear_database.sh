#!/usr/bin/env bash

set -euo pipefail

if [ -f .env ]; then
  set -a
  . .env
  set +a
fi

while true; do
  deleted=$(cypher-shell -u "$NEO4J_USERNAME" -p "$NEO4J_PASSWORD" --format plain "CALL {MATCH (n) RETURN n LIMIT 100000} DETACH DELETE n RETURN count(*) AS deleted;" | tail -n 1)

  echo "Deleted: $deleted"

  if [ "$deleted" -eq 0 ]; then
    echo "Done."
    break
  fi
done
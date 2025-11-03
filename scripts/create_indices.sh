#!/usr/bin/env bash
set -euo pipefail
OS_URL=${OS_URL:-http://localhost:9200}


# Templates
curl -s -X PUT "$OS_URL/_index_template/trending-1m" -H 'Content-Type: application/json' --data-binary @conf/opensearch/index-templates/trending-1m.json
curl -s -X PUT "$OS_URL/_index_template/trending-5m" -H 'Content-Type: application/json' --data-binary @conf/opensearch/index-templates/trending-5m.json


# Create today’s indices and aliases
TODAY=$(date +%Y.%m.%d)
for PFX in trending-1m trending-5m; do
INDEX="$PFX-$TODAY"
curl -s -X PUT "$OS_URL/$INDEX" -H 'Content-Type: application/json' -d '{"aliases": {"'"$PFX"'": {}}}'
echo "Created $INDEX and alias $PFX"
done
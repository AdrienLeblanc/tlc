#!/bin/bash

set -e

ENDPOINT=$1

echo "=== [1] Add records ==="
curl --header "Content-Type: application/json" --request POST --data "[{\"id\":9,\"lat\":48.8601,\"lon\":2.3507,\"user\":\"lea\",\"timestamp\":1543775727}]" "https://cloudtlc.appspot.com/api/run"
echo ""

curl --request DELETE "https://cloudtlc.appspot.com/api/run/13"
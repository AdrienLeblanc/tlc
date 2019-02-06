#!/bin/bash

set -e

ENDPOINT=$1
LONGITUDE=${2-"lon"}
STOP=$3

echo "USE '$0 $1 long' command if you use 'long' and not 'lon' to represent longitude\n\n"

echo "=== [1] Add records ==="
curl \
  --header "Content-Type: application/json" \
  --request POST \
  --data "[{\"id\":9,\"lat\":48.8601,\"${LONGITUDE}\":2.3507,\"user\":\"lea\",\"timestamp\":1543775727}]" \
  "${ENDPOINT}"
echo ""

echo "=== [2] Add records ==="
curl \
  --header "Content-Type: application/json" \
  --request POST \
  --data "[{\"id\":145,\"lat\":47.8601,\"${LONGITUDE}\":2.8507,\"user\":\"john\",\"timestamp\":154379267}, {\"id\":145,\"lat\":47.8631,\"${LONGITUDE}\":2.8517,\"user\":\"john\",\"timestamp\":154379278}, {\"id\":145,\"lat\":47.8671,\"${LONGITUDE}\":2.8597,\"user\":\"john\",\"timestamp\":154379267}, {\"id\":145,\"lat\":47.8721,\"${LONGITUDE}\":2.8637,\"user\":\"john\",\"timestamp\":154379967}]" \
  "${ENDPOINT}"
echo ""

echo "=== [3] Add records ==="
curl \
  --header "Content-Type: application/json" \
  --request POST \
  --data "[{\"id\":13,\"lat\":33.8601,\"${LONGITUDE}\":2.8507,\"user\":\"alice\",\"timestamp\":1543778727}, {\"id\":13,\"lat\":33.8201,\"${LONGITUDE}\":2.9507,\"user\":\"alice\",\"timestamp\":1543778727}]" \
  "${ENDPOINT}"
echo ""

[ -n "$STOP" ] && read -p "Press [Enter] to continue..."

echo "=== [1] Search records ==="
curl "${ENDPOINT}?user=lea&timestamp=1543775726,1543775729"
echo ""

echo "=== [2] Search records ==="
curl "${ENDPOINT}?user=john&id=145&loc=47.8601,2.8507"
echo ""

echo "=== [3] Search records ==="
curl "${ENDPOINT}?id=145"
echo ""

echo "=== [4] Search records ==="
curl "${ENDPOINT}?timestamp=0,2000000000&id=145"
echo ""

[ -n "$STOP" ] && read -p "Press [Enter] to continue..."

echo "=== [1] Show all records ==="
curl "${ENDPOINT}"
echo ""

echo "=== [1] Delete records ==="
curl --request DELETE "${ENDPOINT}/9,145"
echo ""
curl "${ENDPOINT}"
echo ""

echo "=== [2] Delete records ==="
curl --request DELETE "${ENDPOINT}/13"
echo ""
curl "${ENDPOINT}"
echo ""


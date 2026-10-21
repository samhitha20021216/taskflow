#!/usr/bin/env bash
# Submits one job of each demo type and prints how each one ended.
set -euo pipefail
API=${API:-http://localhost:8080}

submit() {
  curl -sf -X POST "$API/api/jobs" -H 'Content-Type: application/json' "${@:2}" -d "$1" | sed -E 's/.*"id":"([^"]+)".*/\1/'
}

echo "Submitting jobs..."
report=$(submit '{"type":"report.generate","payload":{"rows":2000,"chunkSize":100,"msPerChunk":100}}')
flaky=$(submit '{"type":"demo.flaky","payload":{"succeedOnAttempt":3}}')
poison=$(submit '{"type":"demo.poison","payload":{"bad":true}}')
idem1=$(submit '{"type":"demo.sleep","payload":{"millis":500}}' -H 'Idempotency-Key: demo-key-1')
idem2=$(submit '{"type":"demo.sleep","payload":{"millis":500}}' -H 'Idempotency-Key: demo-key-1')
echo "Idempotency: first=$idem1 second=$idem2 (same id => no duplicate job)"

sleep 6
for id in "$report" "$flaky" "$poison" "$idem1"; do
  curl -sf "$API/api/jobs/$id" | python3 -c 'import json,sys; j=json.load(sys.stdin); print(f"{j[\"type\"]:16} {j[\"status\"]:10} attempt={j[\"attempt\"]} result={j[\"result\"]} error={j[\"lastError\"]}")'
done
echo; curl -sf "$API/api/stats"; echo

#!/usr/bin/env bash
# Kills the worker container that is running a long job, then shows the job being recovered by another
# worker and resumed from its last checkpoint.
set -euo pipefail
API=${API:-http://localhost:8080}

id=$(curl -sf -X POST "$API/api/jobs" -H 'Content-Type: application/json' \
  -d '{"type":"report.generate","payload":{"rows":5000,"chunkSize":100,"msPerChunk":200}}' \
  | sed -E 's/.*"id":"([^"]+)".*/\1/')
echo "Submitted long job $id"

field() { curl -sf "$API/api/jobs/$id" | python3 -c "import json,sys; print(json.load(sys.stdin)['$1'] or '')"; }

until [ "$(field status)" = "RUNNING" ] && [ "$(field progress)" -ge 20 ]; do sleep 0.5; done
owner=$(field leaseOwner)
container=${owner%-*}   # node id = <container hostname>-<random suffix>
echo "Job is at $(field progress)% on worker $owner -> killing container $container"
docker kill "$container" > /dev/null

echo "Waiting for the lease to expire and another worker to take over..."
until [ "$(field status)" = "SUCCEEDED" ]; do sleep 1; printf "  status=%s progress=%s owner=%s\n" "$(field status)" "$(field progress)" "$(field leaseOwner)"; done

echo; echo "Result: $(curl -sf "$API/api/jobs/$id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"])')"
echo "Expected checksum: $((5000 * 5001 / 2))"
echo; echo "History:"
curl -sf "$API/api/jobs/$id/events" | python3 -c '
import json,sys
for e in json.load(sys.stdin):
    print(f"  {e[\"createdAt\"][11:23]}  {e[\"type\"]:16} attempt={e[\"attempt\"]} {e[\"worker\"] or \"\":40} {e[\"message\"] or \"\"}")'
echo; echo "Restart the killed worker with: docker compose up -d --scale worker=3"

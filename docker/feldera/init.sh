#!/bin/sh
set -e

BASE=http://feldera:8080
PIPE=cdc-pipeline

until curl --fail --silent --output /dev/null "$BASE/healthz"; do
  echo "waiting for feldera..."
  sleep 2
done

if curl --fail --silent --output /dev/null "$BASE/v0/pipelines/$PIPE"; then
  echo "stopping existing pipeline"
  curl --fail-with-body -sS -X POST "$BASE/v0/pipelines/$PIPE/stop?force=true"
  while [ "$(curl -s "$BASE/v0/pipelines/$PIPE" | sed -n 's/.*"deployment_status":"\([^"]*\)".*/\1/p')" != "Stopped" ]; do
    sleep 2
  done
fi

curl --fail-with-body -sS -X PUT "$BASE/v0/pipelines/$PIPE" \
  -H 'Content-Type: application/json' \
  -d @/pipeline.json

curl --fail-with-body -sS -X POST "$BASE/v0/pipelines/$PIPE/start"

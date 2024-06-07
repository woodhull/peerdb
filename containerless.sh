#!/bin/bash

export PEERDB_LOG_DIR="$HOME/logs"
export PEERDB_CATALOG_HOST=localhost
export PEERDB_CATALOG_PORT=5432
export PEERDB_CATALOG_USER=postgres
export PEERDB_CATALOG_PASSWORD=postgres
export PEERDB_CATALOG_DATABASE=postgres
export PEERDB_FLOW_SERVER_HTTP=http://localhost:8111
export NEXTAUTH_SECRET=test
export DATABASE_URL=postgres://postgres:postgres@localhost/postgres

# trap 'kill $(jobs -pr)' SIGINT SIGTERM EXIT
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

temporal server start-dev &

mkdir -p "$PEERDB_LOG_DIR"
pushd nexus
cargo run &
popd

pushd flow
peer-flow worker &
peer-flow snapshot-worker &
peer-flow api &
popd

pushd ui
npm run start &
popd

wait

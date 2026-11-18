#!/usr/bin/env bash
# 无 Docker 环境下的本地双实例演示：两个 uvicorn 进程共享同一个 SQLite 文件。
# 与 docker-compose.yml 的双实例场景等价（同一文件、同一文件系统锁）。
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data
export DB_PATH=./data/leases.db
export POOL_CIDR="${POOL_CIDR:-10.99.0.0/29}"
export ALLOW_RESET=1

INSTANCE_ID=node1 python3 -m uvicorn --app-dir app main:app --host 0.0.0.0 --port 8001 &
INSTANCE_ID=node2 python3 -m uvicorn --app-dir app main:app --host 0.0.0.0 --port 8002 &

trap 'kill 0' EXIT
wait

#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/mnt/Homes/mikesell/Scripts/retreivr"
cd "$PROJECT_DIR"

IMAGE_NAME="${IMAGE_NAME:-retreivr:latest}"
DOCKERFILE="${DOCKERFILE:-docker/Dockerfile}"
COMPOSE_FILE="${COMPOSE_FILE:-docker/docker-compose.yml}"
DO_UP="${DO_UP:-0}"

if [ "${1-}" = "--up" ]; then
  DO_UP=1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is not installed or not on PATH" >&2
  exit 1
fi

if [ -f "$COMPOSE_FILE" ]; then
  docker compose -f "$COMPOSE_FILE" build
  if [ "$DO_UP" = "1" ]; then
    docker compose -f "$COMPOSE_FILE" up -d
  fi
else
  if [ -f "docker/docker-compose.yml.example" ]; then
    echo "No $COMPOSE_FILE found; you can copy the example:" >&2
    echo "  cp docker/docker-compose.yml.example $COMPOSE_FILE" >&2
  fi
  docker build -f "$DOCKERFILE" -t "$IMAGE_NAME" .
fi

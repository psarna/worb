#!/bin/sh
set -e

IMAGE="${1:-ghcr.io/psarna/worb}"
TAG="${2:-latest}"

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --tag "$IMAGE:$TAG" \
  --push \
  .

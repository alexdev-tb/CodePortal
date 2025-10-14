# syntax=docker/dockerfile:1

FROM golang:1.24-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["sleep", "infinity"]

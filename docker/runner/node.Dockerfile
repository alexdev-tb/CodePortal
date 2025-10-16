# syntax=docker/dockerfile:1

FROM node:22-slim

RUN useradd --create-home sandbox || true

ENTRYPOINT ["sleep", "infinity"]

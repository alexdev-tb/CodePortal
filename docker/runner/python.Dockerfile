# syntax=docker/dockerfile:1

FROM python:3.12-slim

RUN useradd --create-home sandbox || true

ENTRYPOINT ["sleep", "infinity"]

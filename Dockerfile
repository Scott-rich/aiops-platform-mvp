FROM python:3.11.14-slim-bookworm AS builder
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY requirements-runtime-lock.txt /tmp/requirements-runtime-lock.txt
RUN pip install --no-cache-dir -r /tmp/requirements-runtime-lock.txt && pip check

FROM python:3.11.14-slim-bookworm AS runtime
ENV PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1 DATA_DIR=/data
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY wire_contract.py ./wire_contract.py
COPY app ./app
COPY flink ./flink
COPY research/otel ./research/otel
COPY research/experiment.py ./research/experiment.py
COPY scripts/research/otel_bridge.py ./scripts/research/otel_bridge.py
COPY cases ./cases
RUN useradd --create-home --uid 1000 aiops && mkdir /data && chown aiops:aiops /data
USER 1000:1000
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/live', timeout=2)"
CMD ["uvicorn", "app.api:app", "--no-proxy-headers", "--host", "0.0.0.0", "--port", "8000"]

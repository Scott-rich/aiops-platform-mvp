FROM python:3.11.12-slim-bookworm
WORKDIR /opt/aiops
COPY requirements-runtime-lock.txt /tmp/requirements-runtime-lock.txt
RUN pip install --no-cache-dir -r /tmp/requirements-runtime-lock.txt && pip check
COPY simulator /opt/aiops/simulator
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "simulator.service:app", "--host", "0.0.0.0", "--port", "8000"]

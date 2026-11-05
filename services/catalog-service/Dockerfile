# Build context is the services/ directory:
#   docker build -f services/catalog-service/Dockerfile -t s24/catalog-service services
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SERVICE_NAME=catalog-service

WORKDIR /app

COPY common/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && pip install -r /tmp/requirements.txt

COPY common/ /app/common/
COPY catalog-service/app.py /app/app.py

RUN useradd --uid 10001 --create-home app && chown -R app:app /app
USER 10001

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2).status == 200 else 1)"

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--timeout-keep-alive", "30", "--no-access-log"]

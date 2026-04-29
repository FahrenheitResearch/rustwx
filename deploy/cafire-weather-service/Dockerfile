FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    RUSTWX_BASEMAP_DIR=/app/assets/basemap

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tini \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
COPY wheels/ /tmp/wheels/
ARG RUSTWX_VERSION=0.4.4
RUN python -m pip install --upgrade pip \
    && python -m pip install -r requirements.txt \
    && if ls /tmp/wheels/rustwx-*.whl >/dev/null 2>&1; then \
        python -m pip install --no-index --find-links /tmp/wheels "rustwx==${RUSTWX_VERSION}"; \
    else \
        python -m pip install "rustwx==${RUSTWX_VERSION}"; \
    fi

COPY app ./app

RUN mkdir -p /app/bin /data/artifacts /data/cache /data/glm /data/volume-stores

EXPOSE 8000
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

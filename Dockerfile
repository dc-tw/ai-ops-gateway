FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

COPY pyproject.toml ./

# CI network-only tests can skip the ~350 MB browser OS layer. Production keeps
# the secure default and installs Chromium plus all required shared libraries.
ARG INSTALL_BROWSER=true
RUN touch README.md \
    && mkdir -p app \
    && touch app/__init__.py \
    && pip install --upgrade pip \
    && if [ "${INSTALL_BROWSER}" = "true" ]; then \
      pip install '.[browser,mcp]'; \
    else \
      pip install .; \
    fi

RUN if [ "${INSTALL_BROWSER}" = "true" ]; then \
      python -m playwright install --with-deps chromium; \
    else \
      mkdir -p /ms-playwright; \
    fi \
    && useradd --create-home --uid 10001 gateway

# Keep application changes out of the expensive dependency/browser layers.
COPY app ./app
RUN chown -R gateway:gateway /app /ms-playwright

USER gateway

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--proxy-headers"]

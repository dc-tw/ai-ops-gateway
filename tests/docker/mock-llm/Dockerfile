FROM python:3.12-alpine
WORKDIR /srv
COPY server.py .
EXPOSE 8080
HEALTHCHECK --interval=2s --timeout=1s --retries=20 \
  CMD wget -q -O - http://127.0.0.1:8080/healthz >/dev/null || exit 1
CMD ["python", "server.py"]

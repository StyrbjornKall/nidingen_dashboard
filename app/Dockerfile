# One container: FastAPI serves /api and the built React site (HANDOFF.md §3).
# Build from the repo root:  docker build -f app/Dockerfile -t nidingen-dashboard .
# The database is never baked in; it is mounted at DUCKDB_PATH.

# ── Stage 1: frontend ────────────────────────────────────────────────────────
FROM node:24-slim AS web

WORKDIR /build
COPY web/package.json web/package-lock.json web/
RUN npm --prefix web ci

# gen-tokens.mjs reads design/tokens.json
COPY design/tokens.json design/tokens.json
COPY web web
RUN npm --prefix web run build


# ── Stage 2: Python packages ─────────────────────────────────────────────────
FROM python:3.13-slim AS builder

WORKDIR /build
COPY app/requirements.txt .
RUN pip install --upgrade pip \
    && pip install --prefix=/install --no-cache-dir -r requirements.txt


# ── Stage 3: runtime ─────────────────────────────────────────────────────────
FROM python:3.13-slim

# Non-root user, UID 1000 (required by SciLifeLab Serve).
ENV USER=appuser
ENV HOME=/home/$USER
RUN useradd -m -u 1000 $USER

COPY --from=builder /install /usr/local

# The layout mirrors the repo, because the API finds config/ and web/dist
# relative to the repo root (here $HOME).
COPY app/ $HOME/app/
COPY config/ $HOME/config/
COPY --from=web /build/web/dist $HOME/web/dist

# Serve bind-mounts its persistent volume over $HOME/data, so the directory
# has to exist in the image.
RUN mkdir -p $HOME/data \
    && chmod +x $HOME/app/start-script.sh \
    && chown -R $USER:$USER $HOME

WORKDIR $HOME

# Upload bird_ringing_0016.duckdb to the volume mounted at /home/appuser/data
# (Serve: Project Settings -> Storage, then select it for the Custom app).
ENV DUCKDB_PATH=/home/appuser/data/bird_ringing_0016.duckdb
ENV PYTHONUNBUFFERED=1
# Serve only allows ports 3000-9999; 8000 is used in all Serve examples.
ENV GUNICORN_CMD_ARGS="--bind=0.0.0.0:8000 --workers=2 --timeout=120 --forwarded-allow-ips='*' --access-logfile=-"

USER $USER
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/api/health').status == 200 else 1)"

ENTRYPOINT ["./app/start-script.sh"]

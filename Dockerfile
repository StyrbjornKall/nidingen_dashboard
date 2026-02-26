# ── Stage 1: build ──────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# Install only what's needed to build wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --prefix=/install --no-cache-dir -r requirements.txt


# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM python:3.12-slim

# ── Non-root user (UID 1000 required by SciLifeLab Serve) ────────────────────
ENV USER=appuser
ENV HOME=/home/$USER
RUN useradd -m -u 1000 $USER

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code and fix ownership
COPY app/ $HOME/app/
RUN chown -R $USER:$USER $HOME

WORKDIR $HOME/app

# ── Database path ─────────────────────────────────────────────────────────────
# On SciLifeLab Serve the persistent volume is mounted at the path you configure
# in Project Settings → Storage (e.g. /project-vol).  Upload bird_ringing.db
# into that volume and set the mount path to /project-vol in the app form.
# DUCKDB_PATH then tells the app where to find the file inside the container.
# Override with -e DUCKDB_PATH=... for local testing or other deployments.
ENV DUCKDB_PATH=/project-vol/bird_ringing.db

# ── Gunicorn / Dash settings ──────────────────────────────────────────────────
ENV PYTHONUNBUFFERED=1
# Serve only allows ports 3000-9999; 8000 is used in all Serve examples
ENV GUNICORN_CMD_ARGS="--bind=0.0.0.0:8000 --workers=2 --timeout=120 --forwarded-allow-ips='*' --access-logfile=-"
EXPOSE 8000

USER $USER

CMD ["gunicorn", "app:server"]

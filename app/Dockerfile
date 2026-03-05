# ── Stage 1: build ──────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# Install only what's needed to build wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
    && rm -rf /var/lib/apt/lists/*

COPY app/requirements.txt .
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

# Copy application code
COPY app/ $HOME/app/

# Create the data directory that Serve will mount the volume onto
# Must exist in the image before Serve can bind-mount over it
RUN mkdir -p $HOME/data \
    && chmod +x $HOME/start-script.sh \
    && chown -R $USER:$USER $HOME

WORKDIR $HOME/app

# ── Database path ─────────────────────────────────────────────────────────────
# Serve mounts the persistent volume at /home/appuser/data.
# Add /home/appuser/data as a Mount path in Project Settings → Storage,
# then select it when creating the Custom app.
# Upload bird_ringing.db into the volume via the File Manager — it will
# appear at /home/appuser/data/bird_ringing.db inside the container.
ENV DUCKDB_PATH=/home/appuser/data/bird_ringing.db

# ── Gunicorn / Dash settings ──────────────────────────────────────────────────
ENV PYTHONUNBUFFERED=1
# Serve only allows ports 3000-9999; 8000 is used in all Serve examples
ENV GUNICORN_CMD_ARGS="--bind=0.0.0.0:8000 --workers=2 --timeout=120 --forwarded-allow-ips='*' --access-logfile=-"

USER $USER
EXPOSE 8000

ENTRYPOINT ["/home/appuser/start-script.sh"]

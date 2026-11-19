#!/bin/bash
# Serve calls this as the entrypoint. Options come from GUNICORN_CMD_ARGS.
exec gunicorn -k uvicorn.workers.UvicornWorker app.api.main:app

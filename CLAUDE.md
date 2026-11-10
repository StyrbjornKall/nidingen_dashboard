# CLAUDE.md

Plotly Dash dashboard for historical bird-ringing data from Nidingens
Fågelstation (Sweden, station `0016`), backed by a single DuckDB file.

The full project and **database reference** (every table, column, join path
and data quirk) lives in `.github/copilot-instructions.md` and is imported
below. Keep that file as the single source of truth and update it when the
schema, pipeline or dashboard layout changes. Don't duplicate schema details
here.

@.github/copilot-instructions.md

## Quick commands (run from the repo root, Windows/PowerShell or bash)

```bash
uv sync                                                          # install deps (Python 3.13, uv)
uv run python app/app.py                                         # dev server on :8050; needs DUCKDB_PATH in .env
uv run python app/src/convert_mdb_to_duckdb.py                   # rebuild DB from data/RingDb0016.mdb (Windows + Access ODBC)
uv run python app/src/preprocess_data/fetch_smhi_weather.py      # refresh SMHI weather + weather_daily
uv run python app/src/preprocess_data/rebuild_precomputed_tables.py  # refresh start-up lookup tables only
uv run pytest app/tests/test_fetch_observations.py               # the only real pytest suite
```

## Working rules for this repo

- **The database is `data/bird_ringing_0016.duckdb`.** `data/bird_ringing.db`
  and the CSV pipeline (`initialize_database.py`, `preprocess_raw_data.py`)
  are legacy. Don't build new features on them.
- To explore data, open DuckDB **read-only**
  (`duckdb.connect(path, read_only=True)`) so you don't take the single write
  lock from a running app. Stop the app before running any script that writes.
- In `ring_records`, **always exclude `species_code = 'TOTAL'`** when you
  aggregate across species. Every real row is duplicated as TOTAL.
- `ring_records.muscle_score` is really partial juvenile moult (PJM). Text
  columns use `''` for "missing" more often than NULL.
- Species codes contain Å/Ä/Ö (`RÖHAK`). Join to taxonomy through
  `swedish_name`, not `species_metadata.species_code` (that one is an eBird code).
- When printing query results in the Windows terminal, set
  `PYTHONIOENCODING=utf-8`, otherwise Swedish letters show up as `�`. The
  data itself is correct.
- The UI is Swedish-only: write labels, titles and hover text in Swedish.
- Don't commit anything under `data/` or the `.env` file (it holds API keys). Treat ringer
  and reporter names as personal data.
- Deploying data changes means re-uploading the `.duckdb` file to SciLifeLab
  Serve. Pushing to `main` only rebuilds the Docker image.

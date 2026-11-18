# CLAUDE.md

Dashboard for 45+ years of bird-ringing data from Nidingens Fågelstation
(Halland, Sweden, station `0016`), backed by a single DuckDB file: about
396 000 newly ringed birds, 54 000 recaptures, 6 000 recoveries and SMHI
weather. The UI is **Swedish only**. Code, comments and commits are in English.

We are migrating from Plotly Dash to **FastAPI + React**. The plan is in
`HANDOFF.md`, which wins over everything else in case of conflict. Follow its
phases (§9) and keep the Dash app runnable until phase 7 removes it.

The full **database reference** (every table, column, join path and data
quirk) lives in `.github/copilot-instructions.md` and is imported below. Keep
that file as the single source of truth for the schema and update it when the
schema, pipeline or dashboard layout changes. Don't duplicate schema details
here.

@.github/copilot-instructions.md

## Sources of truth

| What | Where |
| --- | --- |
| Migration plan, decisions, statistics, answered questions | `HANDOFF.md` (§2 and §2b hold the phase-1 answers) |
| Seasons, gap years, period options, median minimum, single-season threshold, age display | `config/analysis.json` |
| Visual design and chart behaviour | `design/` (read `design/README.md` first; its data is illustrative, and its "Design deviations" section overrides the sketches) |
| Colours, fonts, radii | `design/tokens.json` |
| Database schema, joins, quirks | `.github/copilot-instructions.md` |

Never hard-code season boundaries, gap years, period presets or the
single-season threshold. Read them from `config/`.

## Quick commands (run from the repo root, Windows/PowerShell or bash)

```bash
uv sync                                                          # install deps (Python 3.13, uv)
uv run python app/app.py                                         # Dash dev server on :8050; needs DUCKDB_PATH in .env
uv run python app/src/convert_mdb_to_duckdb.py                   # rebuild DB from data/RingDb0016.mdb (Windows + Access ODBC)
uv run python app/src/preprocess_data/fetch_smhi_weather.py      # refresh SMHI weather + weather_daily
uv run python app/src/preprocess_data/rebuild_precomputed_tables.py  # refresh start-up lookup tables only
uv run pytest app/tests/test_fetch_observations.py               # the only real pytest suite today
```

Targets for the new stack (set up in phases 2–4; keep this list accurate as
they become real):

```bash
uv run python app/src/build_aggregates.py                   # agg_* tables, after every DB rebuild and weather fetch
uv run pytest                                               # tests
uv run uvicorn app.api.main:app --reload --port 8000        # API in dev
npm --prefix web install && npm --prefix web run dev        # frontend in dev (proxy /api to :8000)
npm --prefix web run build                                  # static build served by FastAPI
docker build -f app/Dockerfile -t nidingen-dashboard .      # single container
```

## Target layout

```text
app/
  api/                  FastAPI app (routers per page, schemas, cache)
  src/                  Data pipeline (convert_mdb_to_duckdb.py, build_aggregates.py, fetch_smhi_weather.py, db_manager.py)
  tests/                pytest: aggregates, statistics, API
web/                    Vite + React + TypeScript frontend
  src/pages/            One folder per page
  src/components/       Nav, Footer, SpeciesPicker, ChartCard, charts/
  src/lib/              api client, formatting (sv-SE), stats helpers
config/                 analysis.json
design/                 Design reference (read-only)
```

## Rules that are easy to get wrong

- **Seasons:** vår = januari–juni, höst = juli–december (`month <= 6`), on
  every page including Väder.
- **Gap years 1990–2004** are excluded from every trend, average, SD band,
  top/worst year and mean median. They are shown in raw series, marked
  "delvis bemannad".
- **"Ringmärkta" = new ringings (`ringon`) only.** Recaptures (`kontr`) are
  separate data (`n_recap`), never added in.
- **Median passage day** needs at least `median_min_birds` (10) birds in that
  species × year × season.
- **Single-season species:** more than 90 % of new ringings in one season →
  one median, for that season only; otherwise a median for both seasons.
- **Age:** show raw `age_code` values (`10`, `2+`, `20` …), never grouped.
- **Trends are absolute:** birds per year, or days for timing. Never percent.
- **Use the exact t quantile** for confidence intervals (`df = n − 2`).
- **Several species in one chart:** raw counts; ridgelines get one y axis per
  species. **Selected species first** in any list that also shows others.
- **Max 6 compared species,** colours by selection order from `tokens.json`;
  selection in the URL (`?arter=`). Codes without a Swedish name stay selectable.
- **Period presets only** (no free year range).
- **Swedish number format:** decimal comma, `−` for minus, space as thousands
  separator. Dates like "19 apr".
- **No Plotly** in the new frontend. Charts are React + SVG with d3 helpers.

## Working rules for this repo

- **The database is `data/bird_ringing_0016.duckdb`.** `data/bird_ringing.db`
  and the CSV pipeline (`initialize_database.py`, `preprocess_raw_data.py`)
  are legacy. Don't build new features on them.
- Open DuckDB **read-only** (`duckdb.connect(path, read_only=True)`) from the
  app, the API and when exploring, so you don't take the single write lock.
  Only the build scripts write; stop the app before running them.
- In `ring_records`, **always exclude `species_code = 'TOTAL'`** when you
  aggregate across species. Every real row is duplicated as TOTAL (removed in
  phase 7; the new aggregates use `ALLA` via `GROUPING SETS`).
- `ring_records.muscle_score` is really partial juvenile moult (PJM). Text
  columns use `''` for "missing" more often than NULL.
- Species codes contain Å/Ä/Ö (`RÖHAK`). Join to taxonomy through
  `swedish_name`, not `species_metadata.species_code` (that one is an eBird code).
- When printing query results in the Windows terminal, set
  `PYTHONIOENCODING=utf-8`, otherwise Swedish letters show up as `�`. The
  data itself is correct.
- Many tracked files under `app/` use **CRLF** line endings (no
  `.gitattributes`). Preserve them when editing, or the whole file shows as changed.
- Don't commit anything under `data/` or the `.env` file (it holds API keys).
  Treat ringer and reporter names as personal data.
- Never bake the database into the Docker image. It is mounted at `DUCKDB_PATH`.
  Deploying data changes means re-uploading the `.duckdb` file to SciLifeLab
  Serve. Pushing to `main` only rebuilds the Docker image.
- Work phase by phase from `HANDOFF.md`, one PR per phase or per page. Start
  each phase by stating the plan.
- Every new statistic gets a test, using small fixtures with known answers.
- Compare each finished page against its file in `design/pages/`.
- Ask before removing any feature of the old app that `HANDOFF.md` does not
  explicitly drop. Open questions are in `HANDOFF.md` §2; ask instead of guessing.

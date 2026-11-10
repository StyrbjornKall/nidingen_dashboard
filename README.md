# 🐦 Nidingens Fågelstation — Bird Ringing Dashboard

An interactive web dashboard for exploring more than 45 years of bird-ringing
data from **Nidingens Fågelstation**, a ringing station on an island off the
coast of Halland, Sweden. It covers about 396,000 newly ringed birds, 54,000
recaptures and 6,000 recoveries from all over the world, together with SMHI
weather data. Data run from 1980 to the present at daily resolution.

It is built with **Plotly Dash**, **DuckDB** and **Polars**, and is deployed
as a Docker container on SciLifeLab Serve. The user interface is in Swedish.

## Features

| Tab | What it shows |
|---|---|
| **Hem** | Landing page |
| **Sammanfattning** | Key figures, time series (day/week/month/year, bar or line) and the top 100 species of all time |
| **Morfometri** | Weight, wing length, age and fat-score distributions, plus a bubble chart |
| **Fenologi** | Migration timing: weekly distribution, ridgeline and seasonal plots |
| **Värmekarta** | Species × week heatmap for one year or the average over all years (top N species) |
| **Väderanalys** | SMHI weather time series for Nidingen A, gap-filled from Vinga A |
| **Återfynd** | World map of Nidingen birds found elsewhere and foreign-ringed birds caught at Nidingen |
| **Om websidan** | About page and FAQ |

The Sammanfattning, Morfometri, Fenologi and Värmekarta tabs share a filter
panel for species (including an aggregate `TOTAL`), time aggregation and
date range.

## Quick start (local)

Prerequisites: [uv](https://docs.astral.sh/uv/) and Python 3.13. Building the
database from the Access file also needs Windows with the Microsoft Access
ODBC driver.

```bash
# 1. Install dependencies
uv sync

# 2. Get the database. Either copy bird_ringing_0016.duckdb into data/, or build it:
uv run python app/src/convert_mdb_to_duckdb.py              # data/RingDb0016.mdb → data/bird_ringing_0016.duckdb
uv run python app/src/preprocess_data/fetch_smhi_weather.py # add SMHI weather (Nidingen A + Vinga A)

# 3. Tell the app where the database is (.env in the repo root)
echo DUCKDB_PATH=/absolute/path/to/data/bird_ringing_0016.duckdb >> .env

# 4. Run
uv run python app/app.py      # → http://localhost:8050
```

## Data pipeline

```text
RingDb0016.mdb (station's Access DB) ─┐
combined_species_metadata.csv ────────┼─► convert_mdb_to_duckdb.py ─► bird_ringing_0016.duckdb ─► app.py
SMHI Open Data API ───────────────────┴─► fetch_smhi_weather.py ───────┘
```

- **`convert_mdb_to_duckdb.py`** is the main build step. It copies the
  station's tables (`ringon`, `kontr`, `fynd`, `frring`, `signaturer`,
  `lokaler`), adds taxonomy, builds the analysis table `ring_records` and
  creates small lookup tables for fast start-up. Re-running it is safe and
  keeps the weather tables.
- **`fetch_smhi_weather.py`** downloads hourly weather for Nidingen A (SMHI
  71190) and Vinga A (71380). It fills Nidingen's gaps in precipitation,
  pressure and visibility from Vinga and builds the `weather_daily` table.
- **`rebuild_precomputed_tables.py`** refreshes only the small lookup tables.
- **`fetch_observation_data.py`** (optional) downloads Artportalen sightings
  into a separate `bird_observations.db`. The dashboard doesn't use it yet.

To add a new season, get an updated `RingDb0016.mdb` from the station and
re-run the converter.

## Database overview

`data/bird_ringing_0016.duckdb`:

| Table | Rows | Content |
|---|---:|---|
| `ringon` | ~396 k | Birds newly ringed at Nidingen |
| `kontr` | ~54 k | Recaptures at Nidingen |
| `fynd` | ~6 k | Recovery reports (Nidingen birds found elsewhere, foreign birds found at Nidingen) |
| `frring` | 764 | Original ringing data for foreign-ringed birds caught at Nidingen |
| `ring_records` | ~900 k | Analysis table: `ringon` + `kontr` + taxonomy, **plus a `TOTAL` copy of every row** |
| `species_metadata`, `artkod_lookup` | 133 | Taxonomy (Artfakta + eBird) and species code → Swedish name |
| `signaturer`, `lokaler` | 367 / 5 | Ringer codes and location codes |
| `weather_data`, `weather_data_vinga`, `weather_daily` | 303 k / 218 k / 16 k | SMHI hourly and daily weather |
| `species_list`, `year_list`, `date_range_cache`, `rediscoveries_species_options` | small | Precomputed lookups for app start-up |

Species are identified by Swedish 5-letter ringing codes, for example
`RÖHAK` (rödhake, European Robin), `LÖSÅN` (lövsångare, Willow Warbler),
`ÄNPIP` (ängspiplärka, Meadow Pipit), `KUFÅG` (kungsfågel, Goldcrest) and
`SKPIP` (skärpiplärka, Rock Pipit).

➡️ **The full column-level schema, join paths and data quirks are documented
in [`.github/copilot-instructions.md`](.github/copilot-instructions.md).**

## Project structure

```text
app/
  app.py                 Dash app (layout + callbacks)
  Dockerfile, start-script.sh, requirements.txt
  assets/                CSS, images, logos
  src/
    convert_mdb_to_duckdb.py   Build the database from the Access file
    db_manager.py              BirdRingingDB connection helper
    query_utils.py             SQL query builders used by the app
    data_processor.py          Polars helpers
    fetch_observation_data.py  Artportalen sightings → bird_observations.db
    preprocess_data/           Weather fetcher, lookup-table rebuild, legacy scripts
  tests/                 Exploratory scripts + pytest suite for the observation fetcher
data/                    Databases and raw/processed data (not in git)
notebooks/               Jupyter exploration
figures/                 Exported exploration plots and outreach screenshots
```

## Deployment

On every push to `main`, the GitHub Actions workflow builds `app/Dockerfile`
and pushes the image to Docker Hub as `nidingen-dashboard`. On SciLifeLab
Serve the container runs Gunicorn on port 8000 as UID 1000. It reads the
database from a persistent volume at
`/home/appuser/data/bird_ringing_0016.duckdb`, and you set the path with
`DUCKDB_PATH`. **The database is not baked into the image.** After rebuilding
the data, upload the new `.duckdb` file through the Serve File Manager.

```bash
docker build -f app/Dockerfile -t nidingen-dashboard .
docker run -p 8000:8000 -v "$PWD/data:/home/appuser/data" nidingen-dashboard
```

## Development

- Add queries to `app/src/query_utils.py`, then add a `dcc.Graph` and a
  `@callback` in `app/app.py`. See the conventions in
  `.github/copilot-instructions.md`.
- Open the database read-only while the app is running, because DuckDB
  allows only one writer.
- Tests: `uv run pytest app/tests/test_fetch_observations.py`. The other
  files in `app/tests/` are exploratory scripts.

## Technology

Plotly Dash · Dash Bootstrap Components · DuckDB · Polars · Plotly ·
flask-caching · Gunicorn · Docker · uv

## Data sources

- Ringing data: Nidingens Fågelstation / Ringmärkningscentralen
  (`RingDb0016.mdb`)
- Weather: [SMHI Open Data](https://opendata.smhi.se/)
- Taxonomy: [Artfakta](https://artfakta.se/) and the
  [eBird taxonomy](https://ebird-api-ui.com/taxonomy/ebird-taxonomy)
- Sightings: [Artdatabanken SOS API](https://github.com/biodiversitydata-se/SOS)

---

**Last updated:** 2026-11-10

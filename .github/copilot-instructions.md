# Nidingen Bird Ringing Dashboard — Developer & AI-Assistant Reference

This is the single source of truth for how the codebase and the database are
organised. `CLAUDE.md` imports this file, and GitHub Copilot reads it
automatically. **Update this file whenever you change the database schema, the
data pipeline or the dashboard layout.**

## 1. What the project is

A Plotly Dash dashboard for the historical bird-ringing data from **Nidingens
Fågelstation** (station code `0016`, location code `0016NID`), an island off
the coast of Halland, Sweden (≈57.30 °N, 11.90 °E). It covers daily ringing
data from **1980-04-13 to the present**: about 396 k newly ringed birds, 54 k
recaptures, 6 k recoveries elsewhere, and SMHI weather from 1982 onwards.

- The UI is **Swedish only**, with hard-coded strings in `app/app.py`. There is
  no translation layer or `locales.json`.
- Deployed as a Docker image on **SciLifeLab Serve**. CI builds and pushes the
  image to Docker Hub on every push to `main`.

## 2. Tech stack

| Layer | Tool |
|---|---|
| Web UI | Dash ≥ 2.14, Dash Bootstrap Components, Font Awesome icons |
| Charts | Plotly (`plotly.express`, `plotly.graph_objects`) |
| Database | DuckDB (embedded, single file, opened read-only by the app) |
| DataFrames | Polars (preferred); pandas only right before Plotly |
| Caching | flask-caching `SimpleCache`, 1 h TTL |
| Server | Gunicorn (`app:server`) in Docker, port 8000 |
| Source DB | MS Access `RingDb0016.mdb`, read via `pyodbc` (Windows only) |
| Python | 3.13 locally (`.python-version`, managed by **uv**); 3.12 in Docker |

There are two dependency lists:
- `pyproject.toml` + `uv.lock` for local development (includes `pyodbc`,
  `matplotlib`, `seaborn` and others).
- `app/requirements.txt`, which the Docker image uses. It has no `pyodbc`
  because the MDB conversion never runs in the container.

## 3. Repository layout

```
ringing/
├── app/                              # Everything that ships in the Docker image
│   ├── app.py                        # The whole Dash app: layout + all callbacks (~2.3 k lines)
│   ├── Dockerfile                    # Multi-stage build, non-root UID 1000 (Serve requirement)
│   ├── start-script.sh               # `exec gunicorn app:server` (Serve entrypoint)
│   ├── requirements.txt              # Runtime deps for Docker
│   ├── assets/                       # Auto-served by Dash: custom.css, hero image, logos
│   ├── src/
│   │   ├── convert_mdb_to_duckdb.py  # ★ PRIMARY DB BUILD: RingDb0016.mdb → bird_ringing_0016.duckdb
│   │   ├── db_manager.py             # BirdRingingDB class (connection, schema helpers)
│   │   ├── query_utils.py            # BirdRingingQueries: static SQL builders used by app.py
│   │   ├── data_processor.py         # BirdDataProcessor: Polars helpers (mostly legacy/notebooks)
│   │   ├── fetch_observation_data.py # Artdatabanken SOS API → data/bird_observations.db
│   │   ├── initialize_database.py    # DEPRECATED legacy CSV → bird_ringing.db pipeline
│   │   └── preprocess_data/
│   │       ├── fetch_smhi_weather.py         # SMHI → weather_data / weather_data_vinga / weather_daily
│   │       ├── rebuild_precomputed_tables.py # Rebuild small lookup tables on an existing DB
│   │       ├── preprocess_raw_data.py        # DEPRECATED: raw yearly .txt → processed CSV
│   │       └── scrape_artfakta.py            # Selenium scraper for Artfakta RUBIN codes
│   └── tests/                        # See §9: mostly exploratory scripts, not a clean suite
├── data/                             # NOT in git (except structure), NOT in Docker image
│   ├── RingDb0016.mdb                # Authoritative Access DB from the station (~100 MB)
│   ├── bird_ringing_0016.duckdb      # ★ The database the app uses (~260 MB)
│   ├── bird_ringing.db               # DEPRECATED legacy DuckDB from the CSV pipeline
│   ├── bird_observations.db          # Artportalen sightings (separate DuckDB, not used by the app)
│   ├── metadata/                     # artfakta_species_metadata.csv, ebird_species_info.csv
│   ├── processed/                    # combined_species_metadata.csv (needed by the converter), weather CSVs, …
│   └── raw/                          # Legacy yearly exports 0016år-<YEAR>.txt + per-year folders (1980–2025)
├── notebooks/exploration.ipynb
├── figures/                          # Exported HTML/SVG exploration plots + outreach/ screenshots
├── .github/workflows/docker-publish.yml
├── pyproject.toml, uv.lock, .python-version
└── .env                              # Local only: DUCKDB_PATH, API keys (never commit)
```

## 4. Data pipeline

```
RingDb0016.mdb ──(convert_mdb_to_duckdb.py, Windows + Access ODBC driver)──► bird_ringing_0016.duckdb
data/processed/combined_species_metadata.csv ──┘   (artkod_lookup, species_metadata)
SMHI Open Data API ──(fetch_smhi_weather.py)──► weather_data, weather_data_vinga, weather_daily
                    (rebuild_precomputed_tables.py)──► species_list, year_list, date_range_cache, … (refresh only)
Artdatabanken SOS API ──(fetch_observation_data.py)──► bird_observations.db  (separate file)
```

Full rebuild, run from the repo root:

```bash
uv sync
uv run python app/src/convert_mdb_to_duckdb.py           # drops and recreates all bird tables; keeps weather tables
uv run python app/src/preprocess_data/fetch_smhi_weather.py   # downloads Nidingen A + Vinga A, gap-fills, builds weather_daily
# optional, only if lookup tables are stale or missing:
uv run python app/src/preprocess_data/rebuild_precomputed_tables.py
```

- The converter needs the **Microsoft Access Driver (*.mdb, *.accdb)**
  (Windows) and decodes text as **cp1252**.
- Converter options: `--mdb PATH --db PATH`. Weather fetcher options:
  `--db-path`, `--dry-run`, `--no-vinga`, `--output-nidingen-csv`,
  `--output-vinga-csv`. Rebuild script option: `--db PATH`.
- To add a new season, get an updated `RingDb0016.mdb` from the station and
  re-run the converter. Do **not** append CSVs. The old
  `load_csv_to_table`/`initialize_database.py` route targets the legacy schema.

## 5. Database reference: `data/bird_ringing_0016.duckdb`

Row counts are from the database as of 2026-11 (ringing data up to
2026-05-09). All tables live in schema `main`. There are no foreign-key
constraints; joins are by convention (see §5.4).

### 5.0 Table overview

| Table | Rows | Kind | Purpose |
|---|---:|---|---|
| `ringon` | 395,777 | native (MDB `Ringon`) | Every bird **newly ringed** at Nidingen (`location='0016NID'`) |
| `kontr` | 54,292 | native (MDB `Kontr`) | **Recaptures (controls) at Nidingen** of already-ringed birds |
| `fynd` | 6,223 | native (MDB `Fynd`) | **Recovery reports** involving Nidingen birds (found elsewhere, or foreign birds at Nidingen) |
| `frring` | 764 | native (MDB `FrRing`) | **Original ringing data of foreign-ringed birds** later caught at Nidingen |
| `signaturer` | 367 | native | Ringer code → full name |
| `lokaler` | 5 | native | Station's location codes → coordinates |
| `artkod_lookup` | 133 | derived (CSV) | RUBIN species code (`RÖHAK`) → Swedish name (`rödhake`) |
| `species_metadata` | 133 | derived (CSV) | Taxonomy (Artfakta + eBird), keyed by Swedish name |
| `ring_records` | 900,138 | derived | **Main analysis table** used by the dashboard: `ringon ∪ kontr` + taxonomy + `TOTAL` duplicates |
| `species_list` | 216 | precomputed | Species dropdown, with taxonomy sort keys |
| `rediscoveries_species_options` | 109 | precomputed | Species dropdown for the Återfynd tab |
| `date_range_cache` | 1 | precomputed | `(min_date, max_date)` of ringing data |
| `year_list` | 47 | precomputed | Distinct years 1980–2026 |
| `weather_data` | 302,554 | external (SMHI) | Hourly weather, Nidingen A (71190), **gap-filled from Vinga** |
| `weather_data_vinga` | 218,230 | external (SMHI) | Hourly weather, Vinga A (71380), raw |
| `weather_daily` | 15,957 | precomputed | One row per day, 1982-01-01 → present |

### 5.1 General data conventions (read before writing SQL)

1. **Empty string vs NULL.** Access text fields come through as `''` far more
   often than `NULL`, e.g. `sex`, `notes`, `mark_*`, `bio_code_*`,
   `condition`, `ringer_code`, `hour`. Filter with `NULLIF(col, '') IS NOT
   NULL` or `COALESCE(col, '') <> ''`, never just `IS NOT NULL`.
2. **"Not measured" sentinels have been turned into NULL** by the converter:
   `wing_length = 0`, `weight = 0.0`, `fat_score = 10` and
   `partial_juv_moult = 10` are all stored as `NULL`. Treat NULL as "not
   measured". Most birds lack fat scores: only ~34 k of 396 k new ringings
   have one.
3. **Species codes** are Swedish 5-letter **RUBIN codes** in upper case with
   Swedish letters (`RÖHAK`, `LÖSÅN`, `GÄSMY`, `KUFÅG`, `SKPIP`). Always quote
   them as UTF-8 literals. **Swedish names are lower case** (`rödhake`,
   `lövsångare`).
4. `species_metadata.species_code` is an **eBird code** (`eurrob1`). It is
   **not** the RUBIN code. Never join `ring_records.species_code` to it. Join
   through Swedish names (§5.4).
5. **Dates are `DATE`** (the Access datetimes were always midnight). Time of
   day lives in a separate column: `hour` (VARCHAR `'07'`) in the native
   tables, `time` (DOUBLE, hours) in `ring_records`. A few values are invalid
   (`'91'`, `''`, NULL). Keep only `time BETWEEN 0 AND 23` for hour-of-day
   work.
6. **Age codes** (`age_code`) are Swedish calendar-year codes, stored as text.
   Most common values: `10`, `2+`, `20`, `3+`, `00`, `1+`, then `4+`, `30`,
   `5+`. As commonly used in Swedish ringing, `10` = 1st calendar year, `20` =
   2nd calendar year, `2+` = at least 2nd calendar year, `1+` = full-grown,
   age unknown, `00` = unknown/pullus-related. Check with the station before
   publishing interpretations.
7. **Sex**: `M`, `F`, plus rarer `N`, `G`, `W` and `''`. Unknown sex is
   mostly NULL or `''`.
8. Many numeric columns are `BIGINT` (inherited from Polars), and `weight` is
   `DOUBLE`. Float32 noise from Access is visible, e.g. `4.900000095` in
   `frring`/`fynd`. Round for display.
9. Text in Access was cp1252 and is now proper UTF-8 in DuckDB. If you see
   `Ã¤`/`�` in terminal output, the problem is the console encoding, not the
   data.

### 5.2 Native tables (from `RingDb0016.mdb`)

Original Access column names are in brackets.

#### Shared "observation core": columns in `ringon`, `kontr`, `fynd` and `frring`

| Column [Access] | Type | Meaning |
|---|---|---|
| `ring_number` [Ring] | VARCHAR | Ring ID as printed, e.g. `'9125726'`, `'1HA71987'`, `'H   8401'` (inner spaces kept) |
| `centre` [Centr] | VARCHAR | Ringing scheme/centre: `SVS` (Sweden, Stockholm), `NOS` (Norway), `DKC` (Denmark), `GBT` (UK, BTO), `BLB` (Belgium), `NLA` (NL)… Always `SVS` in `ringon` |
| `location` [Lokal] | VARCHAR | Location code. `'0016NID'` = Nidingen. Foreign locations look like `RC007046` |
| `trap_type` [Tr] | VARCHAR | Capture method/trap code (`'1'` dominates in `ringon`; also `2`,`3`,`7`,`X`,`Y`,`M`,`0`,`4`) |
| `brood_id` [IdKull] | VARCHAR | Brood/nest identifier for pulli, e.g. `00162022411` |
| `date` [Datum] | DATE | Event date |
| `hour` [Tim] | VARCHAR | Hour of capture `'00'`–`'23'`; `''`/NULL if unknown |
| `species_code` [Artkod] | VARCHAR | RUBIN code |
| `ringer_code` [Signatur] | VARCHAR | Ringer initials → `signaturer.ringer_code` (e.g. `UNO`) |
| `sex` [Sex] | VARCHAR | See §5.1 |
| `age_code` [Age] | VARCHAR | See §5.1 |
| `status` [Status] | VARCHAR | RC status code (mostly `''`) |
| `warning_flag` [Varn] | VARCHAR | RC warning/validation flag (`4`, `5`, `9`, `3`, `''`) |
| `pullus` [Pullus] | VARCHAR | Pullus/brood-size code |
| `reserved` [Reserv] | VARCHAR | Reserve/misc code from RC |
| `wing_length` [Vinge] | BIGINT | Wing length, mm (NULL = not measured) |
| `weight` [Vikt] | DOUBLE | Body mass, g (NULL = not measured) |
| `fat_score` [Fett] | BIGINT | Fat score 0–9 (NULL = not measured) |
| `partial_juv_moult` [PJM] | BIGINT | Partial juvenile moult score 0–9 (NULL = not measured) |
| `condition` [Kondition] | VARCHAR | Free-text condition flag: `BRUTEN`, `PARASIT`, `FORM`, `SÅR`, `SVAG`, `TUMÖR`… |
| `notes` [Textrad] | VARCHAR | Free-text note (Swedish) |
| `mark_type_1/2` [Typ1/2] | VARCHAR | Extra marks: `FRING` (colour ring), `FRAR`, `FLAG`, `SATEL`, `LOGG`, `TRANS`, `HRING`, `MRING` |
| `mark_text_1/2` [Text1/2] | VARCHAR | Code on the extra mark |
| `mark_color_1/2` [Color1/2] | VARCHAR | Colour, e.g. `BLÅ`, `LBLÅ/MET` |

#### `ringon`: new ringings at Nidingen (395,777 rows, 1980-04-13 → 2026-05-09)

Core columns plus:

| Column [Access] | Type | Meaning |
|---|---|---|
| `station_code` [Mnr] | VARCHAR | Always `'0016'` |
| `bio_code_1..7` [B1..B7] | VARCHAR | Code of an extra biometric measurement (e.g. `N`, `M`, `R`, `S`, `H`, `T`) |
| `bio_value_1..3` [Bio1..3] | DOUBLE | Numeric value for bio_code_1..3 (**0.0 when absent**, never NULL) |
| `bio_value_4..7` [Bio4..7] | VARCHAR | Text values, e.g. `'93'`, `'näbbhöjd 18,3'`, `'hp1-136'` |
| `foreign_bird` [Utland] | VARCHAR | Foreign-bird flag (in practice `''`/NULL) |
| `original_centre` [GCentr] | VARCHAR | For re-ringed birds: previous scheme |
| `original_ring` [GRing] | VARCHAR | For re-ringed birds: previous ring number |

`ring_number` is effectively unique (395,772 distinct out of 395,777 rows).
Indexes: `date`, `species_code`, `(date, species_code)`, `ring_number`.

#### `kontr`: recaptures at Nidingen (54,292 rows, 1980-04-14 → 2026-05-08)

Core columns, the bio columns, `station_code`, `foreign_bird`, plus:

| Column [Access] | Type | Meaning |
|---|---|---|
| `find_type` [FKND] | VARCHAR | EURING-style finding-circumstance code (`'8'` = caught & released by ringer, ≈97 %) |
| `find_date_type` [FDT] | VARCHAR | Finding-condition/date-type code |
| `find_date_accuracy` [FDTA] | VARCHAR | Date accuracy (`0`/`1`) |
| `distance_km` [Distans] | BIGINT | Distance from original ringing site (0 for local birds) |
| `bearing_deg` [Kurs] | BIGINT | Direction from original ringing site, 0–359 |
| `days_since_ring` [Dagar] | BIGINT | Days since ringing (max ≈ 11,195 ≈ 30 years) |
| `hours_since_ring` [Timmar] | BIGINT | Extra hours component |
| `serial_number` [LöpNr] | BIGINT | Running record number in RC |

Each row is one recapture event, so a ring can appear many times (25,269
distinct rings). 24,582 of those rings exist in `ringon`. The rest were
ringed elsewhere or under other schemes.

#### `fynd`: recovery reports (6,223 rows, events 1980-05-01 → 2026-04-08)

All recovery reports linked to station 0016. **`date`, `latitude` and
`longitude` describe the find event, not the ringing.** It contains two
populations:

- **Outbound** (≈5,218 rows): `ring_number` exists in `ringon`. These are
  Nidingen birds found elsewhere (Spain, Morocco, Svalbard, …).
- **Inbound** (≈1,004 rows): `ring_number` exists in `frring`. These are
  foreign-ringed birds encountered at Nidingen, so their coordinates are
  Nidingen's.

834 rows have `location = '0016NID'`. Core columns (no bio columns) plus:

| Column [Access] | Type | Meaning |
|---|---|---|
| `report_number` [Rapp] | VARCHAR | Report ID (`RA32632`) or centre code |
| `find_type`, `find_date_type`, `find_date_accuracy` | VARCHAR | As in `kontr` (`find_type` codes 0–8 + letters; `app.py` maps them to Swedish labels in `_FIND_TYPE_LABELS`) |
| `distance_km`, `bearing_deg`, `days_since_ring`, `hours_since_ring` | BIGINT | Movement since ringing (max 8,886 km) |
| `new_centre`, `new_ring` [NyCentr, NyRing] | VARCHAR | If the bird was re-ringed at the find |
| `report_centre`, `report_ring`, `report_species`, `report_type` [Rapp*] | VARCHAR | As reported by the finder (`report_type` ∈ `A`,`M`,`C`,`K`) |
| `case_number` [Dnr] | VARCHAR | Unique case ID, e.g. `1990:0347.01` (unique per row) |
| `wgs84` [WGS84] | VARCHAR | Coordinates as text, e.g. `'5718  N 1154  E'` |
| `latitude`, `longitude` | DOUBLE | Decimal degrees of the find (always present) |
| `coordinate_accuracy` [KNog] | VARCHAR | Coordinate precision code |
| `province_code` [Prov] | VARCHAR | Country/province code (`SVNHA`, `DKNJ`, `ES65`, `MA00`…) |
| `city`, `locality` [Storort, Litenort] | VARCHAR | Place names |
| `reporter`, `reporter_name` [Rapportör, SigNamn] | VARCHAR | Who reported it |

#### `frring`: foreign-ringed birds (764 rows, ringing dates 1973 → 2025)

**`date`, `latitude` and `longitude` are the original ringing event abroad or
elsewhere in Sweden.** The encounter at Nidingen is the matching `fynd` row
(`fynd.ring_number = frring.ring_number`). Core columns plus `station_code`,
`original_centre`, `original_ring`, `wgs84`, `latitude`, `longitude`,
`coordinate_accuracy`, `province_code`, `city`, `locality`,
`original_ringer_name` [Märkare] (often a scheme, e.g. `NORGE, STAVANGER`)
and `recovery_ringer_name` [SigNamn].

#### `signaturer` (367 rows)
`station_code` (`'0016'`), `ringer_code` (unique, e.g. `UNO`), `full_name`.
Join with `ring_records.ringer = signaturer.ringer_code` or
`ringon.ringer_code = signaturer.ringer_code`. **These are personal names;
don't expose them in public charts without consent.**

#### `lokaler` (5 rows)
The station's own location codes: `0016NID` (Nidingen), `RC-NID`, `0016FLA`
(Fladens kassunfyr), and two others. Columns: `location`, `province_code`,
`city`, `locality`, `comment`, `wgs84`, `grid_square`, `rt90_north`,
`rt90_east`, `latitude`, `longitude`, `coordinate_accuracy`,
`location_type`, `station_code`.

### 5.3 Derived tables

#### `artkod_lookup` (133 rows)
| Column | Type | Meaning |
|---|---|---|
| `artkod` | VARCHAR | RUBIN code, upper case (`RÖHAK`) — indexed |
| `swedish_name` | VARCHAR | Swedish common name, lower case (`rödhake`) |

Built from the `sökträff` column of
`data/processed/combined_species_metadata.csv`. **Only 133 of the 216 codes
in `ring_records` are mapped.** 82 codes (1,526 rows, e.g. `KASS`, hybrids,
rare species) get `swedish_name = NULL`. To map more species, add rows to the
CSV and re-run the converter.

#### `species_metadata` (133 rows, PK `swedish_name`)
| Column | Type | Meaning |
|---|---|---|
| `swedish_name` | VARCHAR PK | Join key (lower case) |
| `species_code` | VARCHAR | **eBird** code (`eurrob1`), NULL for 8 taxa |
| `scientific_name` | VARCHAR | e.g. `Erithacus rubecula` |
| `english_name` | VARCHAR | e.g. `European Robin` |
| `taxon_id` | DOUBLE | Artdatabanken/Dyntaxa taxon ID |
| `taxon_order` | DOUBLE | eBird taxonomic sort order |
| `category` | VARCHAR | eBird category (`species`) |
| `order_scientific_name` | VARCHAR | e.g. `Passeriformes` (indexed) |
| `family_english_name`, `family_scientific_name`, `family_code` | VARCHAR | Family info (scientific name indexed) |
| `auktor` | VARCHAR | Taxonomic authority, e.g. `(Linnaeus, 1758)` |
| `taxonkategori` | VARCHAR | `Art` or `Underart` |
| `extinct`, `extinct_year` | BOOLEAN, DOUBLE | Always NULL |
| `com_name_codes`, `sci_name_codes`, `banding_codes` | VARCHAR | eBird 4-letter codes |
| `report_as` | VARCHAR | Always NULL |

#### `ring_records`: the main analysis table (900,138 rows)

A materialised view: `ringon` (as `record_type='C'`) ∪ `kontr` (as
`record_type='R'`) = 450,069 real rows, plus **a full duplicate of every row
with `species_code='TOTAL'`** (another 450,069 rows), so that "TOTAL" works
as a species in the dropdown.

> ⚠️ **Always filter out `TOTAL` when you aggregate across species**:
> `WHERE species_code <> 'TOTAL'`. Otherwise every count is doubled. When
> the user picks TOTAL, filter `species_code = 'TOTAL'` and nothing else.

| Column | Type | Source / meaning |
|---|---|---|
| `record_id` | BIGINT | `ROW_NUMBER()` over (date, ring, type); TOTAL rows are offset by max id. Not a stable key across rebuilds |
| `date` | DATE | Capture date |
| `time` | DOUBLE | `TRY_CAST(hour AS DOUBLE)`, hour of day (a handful of values > 23 are data errors; ~9 k real rows have NULL) |
| `record_type` | VARCHAR | `'C'` = new ringing (`ringon`), `'R'` = recapture (`kontr`) |
| `ring_number` | VARCHAR | Ring ID |
| `age_code`, `age` | VARCHAR | **Identical** (both = `age_code`; `age` is kept for legacy queries) |
| `species_code` | VARCHAR | RUBIN code or `'TOTAL'` |
| `ringer` | VARCHAR | `ringer_code` |
| `sex` | VARCHAR | M/F/… |
| `wing_length` | BIGINT | mm |
| `weight` | DOUBLE | g |
| `fat_score` | BIGINT | 0–9 |
| `muscle_score` | BIGINT | ⚠️ **Actually `partial_juv_moult` (PJM)**, aliased for legacy compatibility. It is **not** a muscle score |
| `brood_patch`, `moult_score` | DOUBLE | **Always NULL** (legacy placeholders) |
| `notes`, `trap_type`, `condition` | VARCHAR | As in the native tables |
| `mark_type_1/2`, `mark_text_1/2`, `mark_color_1/2` | VARCHAR | Extra marks |
| `bio_code_1..7`, `bio_value_1..3` (DOUBLE), `bio_value_4..7` (VARCHAR) | | Extra biometrics |
| `foreign_bird`, `original_centre`, `original_ring` | VARCHAR | Only from `ringon` (NULL for `'R'`) |
| `find_type`, `find_date_type`, `find_date_accuracy` | VARCHAR | Only from `kontr` (NULL for `'C'`) |
| `distance_km`, `bearing_deg`, `days_since_ring`, `hours_since_ring`, `serial_number` | BIGINT | Only from `kontr` |
| `swedish_name` | VARCHAR | Via `artkod_lookup`; `'Total'` for TOTAL rows; NULL for 82 unmapped codes |
| `scientific_name`, `taxon_id` | VARCHAR, DOUBLE | Via `species_metadata` |
| `data_source` | VARCHAR | Always `'RingDb0016'` |
| `created_at`, `updated_at` | TIMESTAMPTZ | Build time |

Indexes: `date`, `species_code`, `(date, species_code)`, `ring_number`,
`swedish_name`.

Useful facts: ringing per year varies a lot. The low points are 1995 (527
new ringings) and 1994 (793), and the peak is 2008 (16,418). The top species are RÖHAK (robin), LÖSÅN (willow
warbler), ÄNPIP (meadow pipit), KUFÅG (goldcrest) and SKPIP (rock pipit).

#### Precomputed lookup tables (read once at app start-up)

| Table | Columns | Built by |
|---|---|---|
| `species_list` | `species_code`, `swedish_name`, `order_name`, `family_name`, `sci_name`, `english_name`. Unmapped taxonomy uses `'~'` so it sorts last | converter / rebuild script |
| `rediscoveries_species_options` | `species_code`, `swedish_name` (distinct codes in `fynd` ∪ `frring`) | converter / rebuild script |
| `date_range_cache` | `min_date`, `max_date` (excludes TOTAL) | converter / rebuild script |
| `year_list` | `year` INTEGER | converter / rebuild script |
| `weather_daily` | see §5.5 | `fetch_smhi_weather.py` / rebuild script |

If they're missing, `app.py` falls back to slow direct queries and prints a
warning.

### 5.4 Join cheat-sheet

```sql
-- RUBIN code → names/taxonomy (the only correct path)
FROM ring_records r
LEFT JOIN species_metadata m ON r.swedish_name = m.swedish_name      -- ring_records already has swedish_name
-- for native tables / fynd / frring:
LEFT JOIN artkod_lookup al    ON t.species_code = al.artkod
LEFT JOIN species_metadata sm ON lower(al.swedish_name) = lower(sm.swedish_name)

-- Life history of one bird
SELECT 'ringed' ev, date, NULL lat, NULL lon FROM ringon WHERE ring_number = ?
UNION ALL SELECT 'recaptured', date, NULL, NULL FROM kontr WHERE ring_number = ?
UNION ALL SELECT 'recovered',  date, latitude, longitude FROM fynd WHERE ring_number = ?;

-- Outbound recoveries (Nidingen birds found elsewhere)
SELECT f.*, r.date AS ring_date
FROM fynd f JOIN ringon r USING (ring_number)
WHERE NOT (f.latitude BETWEEN 57.25 AND 57.35 AND f.longitude BETWEEN 11.85 AND 11.95);

-- Inbound (foreign birds at Nidingen): origin from frring, encounter from fynd
SELECT fr.latitude AS origin_lat, fr.longitude AS origin_lon, fr.date AS ring_date, fj.date AS found_at_nidingen
FROM frring fr JOIN fynd fj USING (ring_number);

-- Ringer names
LEFT JOIN signaturer s ON r.ringer = s.ringer_code

-- Weather on capture day (fast)
LEFT JOIN weather_daily w ON w.date = r.date
-- Weather at capture hour (ASOF, see BirdRingingQueries.get_weather_at_capture_time)
```

### 5.5 Weather tables

#### `weather_data` (Nidingen A, SMHI 71190) and `weather_data_vinga` (Vinga A, SMHI 71380)

Both tables have the same schema. PK `observation_time`, with indexes on
`observation_time` and `CAST(observation_time AS DATE)`.

| Column | Type | Meaning (SMHI parameter id) |
|---|---|---|
| `observation_time` | TIMESTAMPTZ PK | UTC timestamp |
| `temperature` | DOUBLE | Air temp °C, instantaneous (1) |
| `wind_direction` | DOUBLE | °, 10-min mean (3) |
| `wind_speed` | DOUBLE | m/s, 10-min mean (4) |
| `humidity` | DOUBLE | RH %, instantaneous (6) |
| `precipitation` | DOUBLE | mm, 1-h sum (7) |
| `pressure` | DOUBLE | hPa, reduced to sea level (9) |
| `visibility` | DOUBLE | m, instantaneous (12), capped at 75,000 |
| `cloud_cover` | DOUBLE | Total cloud % (16); values > 100 (up to 113) are SMHI codes for an obscured sky |
| `gust_wind` | DOUBLE | m/s, max in 1 h (21) |
| `<param>_quality` ×9 | VARCHAR(2) | SMHI quality flag: `G` = checked/approved, `Y` = suspect/aggregated |
| `station_id`, `station_name` | INTEGER, VARCHAR | 71190 `Nidingen A` / 71380 `Vinga A` |
| `data_source` | VARCHAR | `'SMHI-OpenData'` |
| `fetched_at` | TIMESTAMPTZ | Download time |

Coverage at Nidingen:
- 1982–1994: 3-hourly synoptic observations (~8/day). 1995 is a transition
  year. From 1996: hourly, ~100 %.
- `humidity`, `precipitation` and `gust_wind` are mostly NULL before ~1995.
- Nidingen stopped measuring **precipitation after 2007-03-22**,
  **pressure after 1995-06-30** and **visibility after 2007**.
  `fetch_smhi_weather.patch_nidingen_from_vinga()` **overwrites those NULLs
  in `weather_data` with Vinga values**, including the quality flags. So
  `weather_data` is *not* pure Nidingen data for those three parameters, and
  you can't tell which rows were filled except by comparing with
  `weather_data_vinga`.

#### `weather_daily` (15,957 rows, PK `date`)
`date`, `year`, `month`, `day_of_year`, `mean/min/max_temperature`,
`mean_wind_speed`, `max_gust`, `mean_wind_direction` (arithmetic mean of
degrees, so it's misleading around north), `mean_humidity`,
`total_precipitation`, `mean_pressure`, `mean_visibility` (these three are
`COALESCE(nidingen, vinga)`), `mean_cloud_cover`,
`data_completeness` (= non-null temperature count / 24; ≈0.33 in the
3-hourly era, can slightly exceed 1), and `vinga_gap_fill_used`.

> Known quirk: `vinga_gap_fill_used` is **always FALSE** in the current
> database. The patch step has already filled `weather_data` before the
> daily table is built, so the COALESCE never sees a NULL.

### 5.6 Separate database: `data/bird_observations.db`

This file holds Artportalen sightings within ~5 km of Nidingen, fetched with
`app/src/fetch_observation_data.py` (needs `ARTDATABANKEN_API_KEY`). **The
dashboard doesn't use it.** It has one table, `observations` (261,623 rows,
PK `OccurrenceId`), with Darwin-Core-style columns: `DecimalLatitude/
Longitude`, `CoordinateUncertaintyInMeters`, `Locality`, `DyntaxaTaxonId`,
`ScientificName`, `VernacularName`, `Family`, `Order`, `RedlistCategory`,
`IndividualCount`/`OrganismQuantity` (text) + `OrganismQuantityInt`,
`RecordedBy`, `ReportedBy`, `Sex`, `LifeStage`, `Activity`, `Behavior`,
`OccurrenceRemarks`, `Verified`, `UncertainIdentification`,
`VerificationStatus`, `Projects`, `Url`, `fetched_at`.

> Known issue: in the current file **`StartDate`, `EndDate` and `Modified`
> are all NULL**, so the sightings can't be placed in time yet. Fix the date
> parsing in `fetch_observation_data.py` before you use this table for
> phenology.

### 5.7 Legacy: `data/bird_ringing.db`

This is the old database built from yearly text exports
(`initialize_database.py` + `preprocess_raw_data.py`). It has a slimmer
`ring_records` and none of the native tables. It is **deprecated**. Some
scripts in `app/tests/` and `BirdRingingDB.initialize_schema()` /
`load_csv_to_table()` still target it.

## 6. Python modules

### `db_manager.BirdRingingDB`
A context manager around `duckdb.connect`. Every connection sets
`memory_limit='4GB'` and `threads=4`.

```python
with BirdRingingDB(DB_PATH, read_only=True) as db:
    df = db.execute_query(sql).pl()          # DuckDB relation → Polars
```

Methods in current use: `execute_query`, `get_data_as_polars(query=None,
table_name='ring_records', filters=None)`, `get_summary_stats`,
`optimize_database` (ANALYZE + CHECKPOINT), `export_table_to_parquet`,
`initialize_native_schema` (checks or creates aux tables),
`initialize_weather_schema` / `initialize_vinga_schema` (idempotent and
migrate missing columns), `initialize_observations_schema`.
Legacy: `initialize_schema`, `load_csv_to_table`, `ensure_total_species`
(all for the old CSV DB).

### `query_utils.BirdRingingQueries`
Static methods that **return SQL strings**. Callers run them with
`db.execute_query(...)`. Parameters are interpolated with f-strings, not
bound. They're only fed from dropdown values today; never pass free user text.

| Method | Use |
|---|---|
| `get_species_time_series(start_date, end_date, species_codes, aggregation)` | Counts per day/week/month/year |
| `get_morphometric_distributions(species_codes, year)` | Raw weight/wing/age rows |
| `get_recapture_analysis()` | Birds caught > 1× |
| `get_phenology_daily_distribution(…, aggregate_years)` / `get_phenology_weekly_distribution(…)` | Seasonal distributions (preferred) |
| `get_phenology_migration_windows(…, spring_months, autumn_months)` | Spring/autumn IQR timing |
| `get_phenology_by_species(…)` | **Deprecated** (unimodal) |
| `get_ringer_statistics`, `get_species_diversity_over_time`, `get_conditional_body_metrics` | Misc stats |
| `get_weekly_weight_by_species`, `get_yearly_weight_by_species`, `get_year_over_year_comparison` | Weight / YoY |
| `get_weekly_heatmap_data(year, top_n_species)` | Species × week heatmap (`year=None` = all-year mean) |
| `get_weather_for_date_range(…, aggregation)` | Hourly/daily/weekly/monthly weather |
| `get_daily_weather_summary(start, end)` | Reads `weather_daily` |
| `get_weather_joined_with_ringing(…, weather_aggregation='daily'\|'nearest', max_gap_hours)` | Daily counts + weather (ASOF in nearest mode) |
| `get_weather_at_capture_time(…, max_gap_hours)` | One row per capture with the nearest hourly weather (ASOF JOIN) |
| `get_rediscoveries_map_data(species_codes, start_date, end_date, direction)` | Outbound + inbound map points; returns `ring_number, species_code, swedish_name, english_name, event_date, ring_date, latitude, longitude, city, locality, find_type, distance_km, days_since_ring, direction` |
| `get_rediscoveries_species_options()` | Fallback for the precomputed table |
| `get_species_with_taxonomy(species_codes)` | Species + taxonomy + record counts |

### `data_processor.BirdDataProcessor`
Static Polars helpers (`clean_ring_records`, `add_time_features`,
`calculate_recapture_stats`, `calculate_phenology_metrics`,
`detect_outliers`, …). The app imports it, but it's mostly used from
notebooks.

## 7. Dashboard (`app/app.py`)

- `DB_PATH = os.getenv("DUCKDB_PATH", "/data/bird_ringing_0016.duckdb")`.
  `.env` is loaded with `python-dotenv`. If the file is missing, the app
  raises a descriptive `FileNotFoundError` at import time.
- At start-up it reads the precomputed tables once into module globals:
  `species_options` (sorted taxonomically by `sort_species_by_taxonomy`,
  TOTAL first), `year_options`, `date_range`, `rediscoveries_species_options`.
- **Navigation**: a `dbc.Tabs` bar with id `tabs`. Each tab's content is a
  `html.Div(id="content-<tab_id>")` that `update_tab_visibility` shows or
  hides. All tabs stay mounted. The shared filter panel (`species-dropdown`,
  `aggregation-dropdown`, `date-range-picker`, collapsed by default) is hidden
  on Hem, Väderanalys, Återfynd and Om websidan.

| Tab label | `tab_id` | Components / graph ids | Inputs |
|---|---|---|---|
| Hem | `tab-home` | Hero image + title | — |
| Sammanfattning | `tab-summary-timeseries` | `summary-stats` (cards), `time-series-plot` (+ `plot-type-toggle` bar/line), `species-total-bar` (top-100, **cached** via `@cache.memoize`) | shared filters |
| Morfometri | `tab-morpho` | `morpho-bubble-chart`, `weight-distribution`, `wing-length-distribution`, `age-distribution`, `fat-score-distribution` | species, dates |
| Fenologi | `tab-phenology` | `phenology-weekly-plot`, `phenology-ridgeline-plot`, `phenology-seasonal-plot` | species, dates |
| Värmekarta | `tab-heatmap` | `weekly-heatmap` | `heatmap-year-dropdown`, `heatmap-top-n-dropdown` |
| Väderanalys | `tab-weather` | `weather-timeseries-plot` | `weather-date-picker`, `weather-variable-checklist` |
| Återfynd | `tab-rediscoveries` | `rediscoveries-map` (Scattergeo), `rediscoveries-summary` | `rediscoveries-species-dropdown`, `rediscoveries-date-picker`, `rediscoveries-direction-checklist`, `rediscoveries-lines-toggle` |
| Om websidan | `tab-about-webpage` | Static FAQ/about text (Swedish) | — |

### Conventions for new callbacks

```python
@callback(Output("my-plot", "figure"), Input("species-dropdown", "value"), ...)
def update_my_plot(species_codes, start_date, end_date):
    if not species_codes:
        return go.Figure()
    with BirdRingingDB(DB_PATH, read_only=True) as db:      # always read-only, one short-lived connection per callback
        df = db.execute_query(BirdRingingQueries.some_query(...)).pl()
    fig = ...                                                # build with df.to_pandas() only if Plotly needs it
    fig.update_layout(template="plotly_white",
                      plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                      font=dict(family="Arial, sans-serif", size=12, color="#495057"))
    return fig
```

- Use `PASTEL_COLORS` (10 pastel hex colours defined at the top of `app.py`)
  for traces. The weather tab uses `#E8D4C5` for Vinga-derived traces.
- Wrap graphs in `dbc.Spinner`. Write all user-facing text in **Swedish**.
- Do aggregation in SQL. Exclude `TOTAL` unless the user selected it (§5.3).
- Use `@cache.memoize(timeout=3600)` for expensive figures that don't depend
  on inputs. The cache is per-process, so each Gunicorn worker has its own.
- Keep `pandas` imported first in `app.py`. There's a pandas 3 import-order
  workaround at the top of the file.

## 8. Running and deploying

```bash
# Local (repo root). Put DUCKDB_PATH=<abs path>/data/bird_ringing_0016.duckdb in .env
uv sync
uv run python app/app.py                 # http://localhost:8050 (debug=True)

# Docker locally
docker build -f app/Dockerfile -t nidingen-dashboard .
docker run -p 8000:8000 -v "$PWD/data:/home/appuser/data" nidingen-dashboard
```

- The image copies only `app/`. `.dockerignore` excludes `data/`,
  `notebooks/`, `figures/`, `*.md` and `.env`.
- In the container: `DUCKDB_PATH=/home/appuser/data/bird_ringing_0016.duckdb`.
  On **SciLifeLab Serve**, upload the `.duckdb` file through the File Manager
  to the persistent volume mounted at `/home/appuser/data`. Gunicorn runs
  2 workers on port 8000 with a 120 s timeout. The container must run as
  UID 1000.
- CI (`.github/workflows/docker-publish.yml`): on push to `main` or a `v*.*.*`
  tag it builds `linux/amd64` and pushes
  `<DOCKERHUB_USERNAME>/nidingen-dashboard` tagged with branch, `sha-…`, date
  and semver.
- When you change the database, rebuild it locally and **re-upload the
  `.duckdb` file to Serve**. A new image alone won't change the data.

## 9. Tests

`app/tests/` is mostly **exploratory scripts** that print results or write
HTML figures to `figures/`, not assertions. Several still point at the
legacy `data/bird_ringing.db` (`test_setup.py`, `test_correlations.py`).
The one real pytest suite is `test_fetch_observations.py`, which uses
temporary DBs and mocks:

```bash
uv run pytest app/tests/test_fetch_observations.py
```

To check a query, run it directly against the real DB:

```python
with BirdRingingDB("data/bird_ringing_0016.duckdb", read_only=True) as db:
    print(db.execute_query("SELECT ... LIMIT 20").pl())
```

## 10. Gotchas checklist

- DuckDB allows only **one writer**. Stop the running app (or anything else
  holding a read-write handle) before you run the converter or the weather
  fetcher.
- `ring_records` = 2 × real rows because of TOTAL. Filter it.
- `muscle_score` is PJM. `brood_patch` and `moult_score` are empty. `age` = `age_code`.
- Use `''` checks, not just NULL checks, on text columns.
- Join species through `swedish_name`, never via `species_metadata.species_code`.
- `fynd` coordinates = find site; `frring` coordinates = original ringing site.
- `weather_data` precipitation, pressure and visibility are partly Vinga data.
- `bird_observations.db` dates are NULL. It isn't used by the app.
- Personal data (ringer and reporter names in `signaturer`, `fynd.reporter_name`,
  `frring.*_ringer_name`, `observations.RecordedBy`) should not be published.

## 11. Useful links
- DuckDB SQL: https://duckdb.org/docs/sql/introduction
- Polars: https://docs.pola.rs/api/python/stable/reference/
- Dash: https://dash.plotly.com/ · DBC: https://dash-bootstrap-components.opensource.faculty.ai/
- SMHI Open Data (metobs): https://opendata.smhi.se/apidocs/metobs/
- Artdatabanken SOS API: https://github.com/biodiversitydata-se/SOS

---
**Last updated:** 2026-11-10 — schema verified against `bird_ringing_0016.duckdb`
(ringing data to 2026-05-09, weather to 2026-05-14).

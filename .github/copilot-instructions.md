# Bird Ringing Dashboard - Development Instructions

## Project Overview

This project is a web-based dashboard application for visualizing and analyzing bird ringing data from Nidingen ringing station in Sweden. The application uses **Plotly Dash** for the frontend, **DuckDB** for the database, and **Polars** for high-performance data processing.

**Key Features:**
- Interactive time series visualizations of bird observations (bar and line charts)
- Morphometric analysis (weight, wing length, age class, fat score distributions)
- Phenology (migration timing) analysis with bimodal spring/autumn patterns
- Weekly heatmap showing observation patterns for the top 30 species
- Summary statistics dashboard
- **Weather Analysis tab** — standalone multi-panel time series of all SMHI weather parameters with dual-station attribution (Nidingen A + Vinga A gap-fill)
- Multi-dimensional filtering (species, date range, time aggregation)
- Support for millions of records with efficient query performance

## Technology Stack

- **Frontend**: Plotly Dash + Dash Bootstrap Components (Python web framework)
- **Database**: DuckDB (embedded analytical database)
- **Data Processing**: Polars (high-performance DataFrame library)
- **Visualization**: Plotly (interactive charts)
- **Language**: Python 3.9+
- **Web Server**: Gunicorn (production deployment)

## Project Structure

```
märkning/
├── app.py                       # Main Dash application (entry point)
├── requirements.txt             # Python dependencies
├── data/
│   ├── bird_ringing.db          # DuckDB database (generated)
│   ├── processed/               # Processed CSV files
│   │   ├── processed_nidingen_data.csv
│   │   └── weather.csv          # SMHI weather export (300K+ rows, 1982–present)
│   └── raw/                     # Raw yearly data files (2020–2024)
│       ├── 0016år-<year>.txt    # Combined yearly files
│       └── 0016år-<year>/       # Per-year directories with sub-files
│           ├── ring_records.txt
│           ├── meta_header.txt
│           ├── meta_locations.txt
│           ├── meta_ringer_initials.txt
│           └── meta_station.txt
├── figures/                     # Generated HTML visualization exports
├── notebooks/                   # Jupyter notebooks for exploration
│   └── exploration.ipynb
├── src/
│   ├── db_manager.py           # DuckDB database operations
│   ├── data_processor.py       # Polars data processing utilities
│   ├── query_utils.py          # Pre-built SQL queries
│   ├── initialize_database.py  # Database setup script
│   ├── preprocess_raw_data.py  # Raw data preprocessing
│   ├── fetch_smhi_weather.py   # SMHI weather downloader (CLI script)
│   └── scrape_artfakta.py      # Selenium scraper for species metadata
└── tests/                      # Test files
    ├── test_setup.py
    ├── test_heatmap.py
    ├── test_phenology_patterns.py
    └── test_phenology_visualizations.py
```

## Data Schema

### Main Table: `ring_records`

| Column | Type | Description |
|--------|------|-------------|
| record_id | INTEGER | Primary key |
| date | DATE | Observation date |
| time | DOUBLE | Time of day (hours) |
| record_type | VARCHAR(5) | Record type code (C=capture, R=recapture) |
| ring_number | VARCHAR(50) | Unique ring identifier |
| age_code | VARCHAR(10) | Age code |
| species_code | VARCHAR(20) | Species code (e.g., GÄSMY, BLMES) |
| ringer | VARCHAR(10) | Ringer initials |
| age | VARCHAR(10) | Age description |
| wing_length | INTEGER | Wing length (mm) |
| weight | DOUBLE | Weight (grams) |
| fat_score | INTEGER | Fat score (0-10) |
| muscle_score | INTEGER | Muscle score (0-10) |
| brood_patch | DOUBLE | Brood patch score |
| moult_score | DOUBLE | Moult score |
| notes | TEXT | Additional notes |
| scientific_name | VARCHAR(100) | Scientific species name |
| swedish_name | VARCHAR(100) | Swedish species name |
| taxon_id | DOUBLE | Taxonomic ID |
| data_source | VARCHAR(50) | Data source identifier |
| created_at | TIMESTAMP | Record creation timestamp |
| updated_at | TIMESTAMP | Last update timestamp |

**Indexes:** date, species_code, (date, species_code), ring_number

### Supporting Tables

- `species_metadata`: Extended species information
- `weather_data`: SMHI hourly meteorological observations — see full schema below
- `ringer_info`: Ringer contact information

### `weather_data` Table Schema

| Column | Type | Description |
|--------|------|-------------|
| observation_time | TIMESTAMPTZ PK | UTC timestamp of the observation |
| temperature | DOUBLE | Air temperature (°C, instantaneous) |
| wind_direction | DOUBLE | Wind direction (°, 10-min mean) |
| wind_speed | DOUBLE | Wind speed (m/s, 10-min mean) |
| gust_wind | DOUBLE | Gust wind speed (m/s, max in 1 h) |
| humidity | DOUBLE | Relative humidity (%, instantaneous) |
| precipitation | DOUBLE | Precipitation (mm, 1-h sum) |
| pressure | DOUBLE | Air pressure sea-level (hPa) |
| visibility | DOUBLE | Visibility (m, instantaneous) |
| cloud_cover | DOUBLE | Total cloud cover (%) |
| temperature_quality | VARCHAR(2) | SMHI quality flag for temperature |
| wind_speed_quality | VARCHAR(2) | SMHI quality flag for wind speed |
| precipitation_quality | VARCHAR(2) | SMHI quality flag for precipitation |
| visibility_quality | VARCHAR(2) | SMHI quality flag for visibility |
| station_id | INTEGER | SMHI station ID (71190 = Nidingen A) |
| station_name | VARCHAR | Station name |
| data_source | VARCHAR | Always `'SMHI-OpenData'` |
| fetched_at | TIMESTAMP | When the row was downloaded |

**Indexes:** `idx_weather_time` on `observation_time`, `idx_weather_date` on `CAST(observation_time AS DATE)`

**Coverage reality** (important for join logic):
- **1982–1994**: 3-hourly synoptic schedule (00, 03, 06 … 21 UTC) — ~8 obs/day, `data_completeness ≈ 0.33`
- **1995**: transition year (~18% coverage)
- **1996–present**: hourly, ~99–100% coverage
- **Ringing data era (2020–2024)**: essentially 100% hourly — ASOF join gaps are 0 h
- `humidity`, `precipitation`, `gust_wind` are NULL for most of 1982–1994 (sensors not yet installed)
- `precipitation` is NULL after **2007-03-22** — gap-filled from Vinga A in `get_daily_weather_summary`
- `pressure` is NULL after **1995-06-30** — gap-filled from Vinga A in `get_daily_weather_summary`
- `visibility` is NULL after **2007** — gap-filled from Vinga A in `get_daily_weather_summary`

### `weather_data_vinga` Table Schema

Identical column layout to `weather_data`. Stores the **full** SMHI archive for **Vinga A (station 71380)**, ~30 km from Nidingen. Kept separate and never merged into `weather_data`; gap-filling is done at query time via `COALESCE`.

| Parameter | Vinga coverage |
|-----------|---------------|
| Precipitation | 2007-06-01 → present |
| Pressure | 1968 → present |
| Visibility | 1949 → present |
| Temperature, wind, humidity, cloud | Available but Nidingen is preferred |

**Indexes:** `idx_vinga_time` on `observation_time`, `idx_vinga_date` on `CAST(observation_time AS DATE)`

## Core Modules

### 1. `db_manager.py` - Database Management

**Class: `BirdRingingDB`**

Main database interface for all operations.

```python
# Initialize database
with BirdRingingDB("data/bird_ringing.db") as db:
    db.initialize_schema()
    db.load_csv_to_table("data/processed/processed_nidingen_data.csv")
    
    # Query data
    df = db.get_data_as_polars(filters={"species_code": "GÄSMY"})
    
    # Get statistics
    stats = db.get_summary_stats()
```

**Key Methods:**
- `initialize_schema()`: Create database tables and indexes (uses a sequence for `record_id` auto-increment)
- `initialize_weather_schema()`: Create (or migrate) the `weather_data` table. Detects and drops an old schema that lacks `observation_time`; also runs an **incremental column migration** — adds `visibility` and `visibility_quality` if they are missing from an older table (safe to call repeatedly).
- `initialize_vinga_schema()`: Create (or migrate) the `weather_data_vinga` table with the same schema as `weather_data`. Also applies incremental column migration for `visibility`/`visibility_quality`. Safe to call repeatedly.
- `load_csv_to_table(csv_path, table_name, if_exists)`: Load CSV data into database; auto-excludes `record_id` from insert so the sequence handles it
- `get_data_as_polars(query, table_name, filters)`: Retrieve data as Polars DataFrame; `filters` dict supports single values, lists, and scalars
- `execute_query(query)`: Execute raw SQL and return DuckDB result object (call `.pl()` or `.fetchall()` on result)
- `get_summary_stats()`: Get total records, date range, unique species/ringers, and top 10 species
- `optimize_database()`: Run `ANALYZE` and `CHECKPOINT`
- `export_table_to_parquet(table_name, output_path, partition_by)`: Export to Parquet format

**Configuration:** DuckDB is initialized with `memory_limit='4GB'` and `threads=4`. Adjust in `__init__` for different environments.

### 2. `data_processor.py` - Data Processing

**Class: `BirdDataProcessor`** (all static methods)

High-performance data processing using Polars.

```python
# Load and clean data
df = BirdDataProcessor.load_csv("data.csv")
df = BirdDataProcessor.clean_ring_records(df)

# Add time features
df = BirdDataProcessor.add_time_features(df)

# Get species summary
summary = BirdDataProcessor.get_species_summary(df)

# Calculate phenology
phenology = BirdDataProcessor.calculate_phenology_metrics(df)
```

**Key Methods:**
- `load_csv()`: Load CSV with optimized settings; forces `notes` column to `Utf8`
- `clean_ring_records()`: Strip whitespace, standardize empty strings to `null`, parse date column
- `add_time_features()`: Add `year`, `month`, `day`, `weekday`, `day_of_year`, `week_of_year`, `season` columns
- `filter_by_date_range()`: Filter by date range (inclusive)
- `filter_by_species()`: Filter by species code(s)
- `aggregate_daily_counts()`: Group by date+species and aggregate counts and mean morphometrics
- `calculate_recapture_stats()`: Find birds captured >1 time, calculate days between captures
- `pivot_species_by_time()`: Create a time × species pivot table
- `get_species_summary()`: Generate per-species statistics (counts, morphometrics, most common age)
- `calculate_phenology_metrics()`: Migration timing by species and year (first/last/median/IQR arrival day)
- `detect_outliers()`: Flag outliers using IQR or z-score method
- `merge_with_metadata()`: Join ring records with a metadata DataFrame
- `export_to_formats()`: Export to Parquet, CSV, or JSON

### 3. `query_utils.py` - Pre-built Queries

**Class: `BirdRingingQueries`** (all static methods)

Optimized SQL query builders for common dashboard operations. All methods return SQL strings to pass to `db.execute_query()`.

```python
from query_utils import BirdRingingQueries

query = BirdRingingQueries.get_species_time_series(
    start_date="2020-01-01",
    species_codes=["GÄSMY", "BLMES"],
    aggregation="weekly"
)

with BirdRingingDB("data/bird_ringing.db", read_only=True) as db:
    df = db.execute_query(query).pl()
```

**Available Query Builders:**

| Method | Description |
|--------|-------------|
| `get_species_time_series(start_date, end_date, species_codes, aggregation)` | Observation counts over time; aggregation: `daily`, `weekly`, `monthly`, `yearly` |
| `get_morphometric_distributions(species_codes, year)` | Raw weight/wing_length/age rows for box plots |
| `get_recapture_analysis()` | Birds captured >1 time with days-between stats |
| `get_phenology_by_species(species_codes, start_year, end_year)` | **DEPRECATED** — per-year first/median/last arrival day; does not capture bimodal patterns well |
| `get_phenology_daily_distribution(species_codes, start_year, end_year, aggregate_years)` | Daily counts or avg counts per day-of-year; preferred for ridge plots |
| `get_phenology_weekly_distribution(species_codes, start_year, end_year, aggregate_years)` | Weekly counts or avg counts; used for weekly phenology and year-over-year plots |
| `get_phenology_migration_windows(species_codes, start_year, end_year, spring_months, autumn_months)` | Separate spring/autumn IQR timing per species per year |
| `get_ringer_statistics(start_date, end_date)` | Per-ringer totals, unique species, active days |
| `get_species_diversity_over_time(aggregation)` | Species richness and total observations by time period |
| `get_conditional_body_metrics(metric, group_by)` | Mean/std/quantile body metrics grouped by any columns |
| `get_year_over_year_comparison(species_codes)` | Yearly counts with YoY absolute and percent change |
| `get_weekly_heatmap_data(year, top_n_species)` | Top-N species × week pivot data for heatmap; `year=None` averages across all years |
| `get_weather_for_date_range(start_date, end_date, aggregation)` | Raw or aggregated SMHI weather; `aggregation`: `hourly`, `daily`, `weekly`, `monthly` |
| `get_weather_joined_with_ringing(start_date, end_date, species_codes, weather_aggregation, max_gap_hours)` | Ringing counts joined to weather; `weather_aggregation='daily'` (robust default) or `'nearest'` (ASOF JOIN, adds `weather_match_hours` gap column) |
| `get_weather_at_capture_time(start_date, end_date, species_codes, max_gap_hours)` | Record-level ASOF join — one row per individual capture with nearest weather attached; `weather_match_hours` always present; weather columns NULL when gap > `max_gap_hours` |
| `get_daily_weather_summary(start_date, end_date)` | Compact daily table: min/max/mean temp, wind, rain, visibility, cloud, `data_completeness`, `vinga_gap_fill_used`. Uses `COALESCE(w.x, v.x)` for precipitation, pressure, and visibility. |

## Dashboard Architecture (`app.py`)

The dashboard is fully implemented in `app.py`. It uses:
- **Dash Bootstrap Components** (`dbc`) for layout (cards, tabs, spinners, radio items)
- **Font Awesome** icons via `dbc.icons.FONT_AWESOME`
- A shared **pastel colour palette** (`PASTEL_COLORS`) used consistently across all plots
- All filters and species/year metadata are **loaded once at startup** from the database

### Global Setup (at module level)

```python
DB_PATH = Path(__file__).parent / "data" / "bird_ringing.db"

# Loaded once at startup
species_options   # list of {label, value} for the species dropdown
year_options      # list of {label, value} for the heatmap year dropdown
date_range        # (min_date, max_date) tuple
```

### Tabs and Callbacks

| Tab | Tab ID | Callback inputs | Plot IDs |
|-----|--------|-----------------|----------|
| 📈 Time Series | `tab-timeseries` | species, aggregation, date range, plot type toggle | `time-series-plot` |
| 📊 Morphometrics | `tab-morpho` | species, date range | `weight-distribution`, `wing-length-distribution`, `age-distribution`, `fat-score-distribution` |
| 🌸 Phenology | `tab-phenology` | species, date range | `phenology-weekly-plot`, `phenology-ridgeline-plot`, `phenology-seasonal-plot`, `phenology-yearly-plot` |
| 🔥 Weekly Heatmap | `tab-heatmap` | heatmap year dropdown | `weekly-heatmap` |
| 📋 Summary | `tab-summary` | species, date range | `summary-stats` (returns Bootstrap cards) |
| 🌤️ Weather Analysis | `tab-weather` | variable checklist, date range | `weather-timeseries-plot` |

### Callback Pattern

All callbacks follow this pattern:

```python
@callback(
    Output("plot-id", "figure"),
    [Input("species-dropdown", "value"), ...]
)
def update_plot(species_codes, ...):
    if not species_codes:
        return go.Figure()   # Return empty figure for missing input

    with BirdRingingDB(DB_PATH, read_only=True) as db:
        query = BirdRingingQueries.some_query(species_codes=species_codes, ...)
        df = db.execute_query(query).pl().to_pandas()

    fig = ...   # Build Plotly figure
    fig.update_layout(
        template="plotly_white",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50")
    )
    return fig
```

### Colour Palette

All charts use the shared pastel palette defined at the top of `app.py`:

```python
PASTEL_COLORS = [
    '#B4D4E1',  # Pastel blue
    '#FFD4B8',  # Pastel orange
    '#C5E1B5',  # Pastel green
    '#FFB8C3',  # Pastel pink
    '#E0C5E8',  # Pastel purple
    '#FFE8B8',  # Pastel yellow
    '#B8E6E6',  # Pastel cyan
    '#FFD4E5',  # Pastel rose
    '#D4E8D4',  # Pastel mint
    '#E8D4C5',  # Pastel tan
]
```

The Weather Analysis callback also defines `VINGA_COLOR = "#E8D4C5"` (pastel
tan, index 9 of `PASTEL_COLORS`) to visually distinguish Vinga A gap-fill
traces from the primary Nidingen A traces.

When converting hex colours to `rgba` for fills (e.g. phenology area charts), parse with:
```python
r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
fill_color = f'rgba({r}, {g}, {b}, 0.3)'
```

## Development Workflow

### Initial Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Initialize database:**
   ```bash
   python src/initialize_database.py
   ```

3. **Download weather data:**
   ```bash
   python src/fetch_smhi_weather.py
   ```

4. **Run dashboard:**
   ```bash
   python app.py
   # Opens at http://localhost:8050
   ```

### Loading New Data

When adding new years or data:

```python
from src.db_manager import BirdRingingDB

with BirdRingingDB("data/bird_ringing.db") as db:
    db.load_csv_to_table(
        csv_path="data/processed/new_data.csv",
        if_exists="append"
    )
    db.optimize_database()
```

### Adding a New Dashboard Visualisation

1. **Define query** in `BirdRingingQueries` (or use inline SQL)
2. **Add a `dcc.Graph`** component to the relevant tab in `app.layout`
3. **Register a `@callback`** following the callback pattern above
4. **Use `PASTEL_COLORS`** and the standard `update_layout` kwargs for visual consistency

## Performance Best Practices

### Database Operations

1. **Use indexes**: Queries on `date`, `species_code`, and `(date, species_code)` are indexed
2. **Filter early**: Apply `WHERE` clauses before aggregations
3. **Use `read_only=True`**: For all dashboard callbacks (prevents locking)
4. **Optimize regularly**: Run `db.optimize_database()` after bulk loads

### Data Processing with Polars

1. **Prefer DuckDB aggregation**: Do as much as possible in SQL before converting to Polars/pandas
2. **Use `.pl()` method**: Convert DuckDB results directly to Polars, avoid intermediate copies
3. **Convert to pandas only for Plotly**: Call `.to_pandas()` immediately before plotting
4. **Parallel processing**: Polars automatically parallelizes operations

### Dashboard Performance

1. **Startup queries are cached** in module-level variables (`species_options`, `year_options`, `date_range`)
2. **Aggregation in SQL**: All heavy aggregation happens in DuckDB, not Python
3. **Spinners**: All graphs are wrapped in `dbc.Spinner` to provide loading feedback

## Common Queries

### Time Series
```python
query = BirdRingingQueries.get_species_time_series(
    start_date="2020-01-01", end_date="2024-12-31",
    species_codes=["GÄSMY", "BLMES"], aggregation="monthly"
)
```

### Morphometrics (Box Plot Input)
```python
query = BirdRingingQueries.get_morphometric_distributions(
    species_codes=["GÄSMY"], year=2023
)
```

### Phenology – Weekly Distribution (Recommended)
```python
# Averaged across all years
query = BirdRingingQueries.get_phenology_weekly_distribution(
    species_codes=["GÄSMY"], start_year=2020, end_year=2024, aggregate_years=True
)

# Per-year (for year-over-year plots)
query = BirdRingingQueries.get_phenology_weekly_distribution(
    species_codes=["GÄSMY"], start_year=2020, end_year=2024, aggregate_years=False
)
```

### Migration Windows (Spring vs Autumn)
```python
query = BirdRingingQueries.get_phenology_migration_windows(
    species_codes=["GÄSMY"],
    spring_months=[3, 4, 5],
    autumn_months=[8, 9, 10]
)
```

### Weekly Heatmap
```python
query = BirdRingingQueries.get_weekly_heatmap_data(year=2023, top_n_species=30)
query = BirdRingingQueries.get_weekly_heatmap_data(year=None, top_n_species=30)  # all-year average
```

### Weather – Daily Summary (robust, any era)
```python
query = BirdRingingQueries.get_daily_weather_summary(
    start_date="2020-01-01", end_date="2024-12-31"
)
# Returns: date, mean/min/max_temperature, mean_wind_speed, max_gust,
#          total_precipitation, mean_pressure, mean_visibility,
#          mean_cloud_cover, data_completeness, vinga_gap_fill_used
# precipitation/pressure/visibility: COALESCE(Nidingen, Vinga)
```

### Weather + Ringing – Daily Join (recommended)
```python
# Daily mean weather joined to daily capture counts — works for any date range
query = BirdRingingQueries.get_weather_joined_with_ringing(
    start_date="2022-01-01", end_date="2022-12-31",
    species_codes=["GÄSMY", "BLMES"],
    weather_aggregation="daily",   # default
)
# Returns: date, species_code, captures, mean_weight, mean_fat_score,
#          mean/min/max_temperature, total_precipitation, data_completeness, …
```

### Weather + Ringing – Nearest-Hour Join (for hourly correlation)
```python
# ASOF JOIN: nearest weather observation per capture-hour group
# max_gap_hours=2 nullifies weather columns if gap > 2 h (safe for 2020+ era)
query = BirdRingingQueries.get_weather_joined_with_ringing(
    start_date="2022-09-01", end_date="2022-10-31",
    species_codes=["GÄSMY"],
    weather_aggregation="nearest",
    max_gap_hours=2,
)
# Returns: date, capture_hour, captures, weather_match_hours, temperature, wind_speed, …
```

### Weather at Individual Capture Time (record-level)
```python
# One row per ringing record with nearest weather observation attached
# Use for scatter plots: fat_score vs temperature, weight vs wind_speed, etc.
query = BirdRingingQueries.get_weather_at_capture_time(
    start_date="2022-09-01", end_date="2022-10-31",
    species_codes=["GÄSMY"],
    max_gap_hours=2,
)
# Returns: record_id, date, time, ring_number, species_code, age, weight,
#          wing_length, fat_score, muscle_score,
#          weather_ts, weather_match_hours, temperature, wind_speed, …
```

## Testing Queries

Always test queries before adding to dashboard:

```python
with BirdRingingDB("data/bird_ringing.db", read_only=True) as db:
    query = "YOUR_SQL_QUERY"
    result = db.execute_query(query)
    print(result.pl())  # Check results as Polars DataFrame
```

## Extending the Database

### Adding Weather Data

Weather data is fetched from the SMHI Open Data API using `src/fetch_smhi_weather.py`:

```bash
# Full download + load into DB (run once; re-run to refresh)
python src/fetch_smhi_weather.py

# Download and also save to CSV (for inspection)
python src/fetch_smhi_weather.py --output-csv data/processed/weather.csv

# Fetch without writing to DB
python src/fetch_smhi_weather.py --dry-run

# Skip Vinga A supplementary fetch (Nidingen only)
python src/fetch_smhi_weather.py --no-vinga
```

The script downloads **9 parameters** (temperature, wind direction, wind speed,
humidity, precipitation, pressure, **visibility**, cloud cover, gust wind) for
**Nidingen A** (SMHI station 71190) from 1980 to present.  It also downloads
the same parameter set for **Vinga A** (station 71380) into the separate
`weather_data_vinga` table, then runs `patch_nidingen_from_vinga()` which
back-fills NULL values in `weather_data` for precipitation (post-2007),
pressure (post-1995), and visibility (post-2007) using the Vinga readings.

Both `initialize_weather_schema()` and `initialize_vinga_schema()` are called
automatically and perform **incremental column migration** — if the table
already exists but lacks the `visibility` / `visibility_quality` columns they
are added via `ALTER TABLE` without touching existing data.  The full fetch is
safe to re-run on an existing DB.

### Adding New Species Metadata

```python
with BirdRingingDB("data/bird_ringing.db") as db:
    db.conn.execute("""
        INSERT INTO species_metadata (species_code, scientific_name, ...)
        VALUES (?, ?, ...)
    """, parameters)
```

## Troubleshooting

### Database Lock Issues
- Always use `read_only=True` for callbacks in `app.py`
- Only one write connection at a time
- Use the context manager (`with BirdRingingDB(...) as db:`) to ensure proper cleanup

### Memory Issues
- Apply filters in SQL before fetching data
- Increase `memory_limit` in `BirdRingingDB.__init__` if needed
- Use `export_table_to_parquet()` and load from Parquet for very large static datasets

### Performance Issues
- Use `EXPLAIN query` to check index usage
- Run `db.optimize_database()` (`ANALYZE` + `CHECKPOINT`) after bulk loads
- Profile with `EXPLAIN ANALYZE`

## Useful Resources

- DuckDB SQL: https://duckdb.org/docs/sql/introduction
- Polars API: https://docs.pola.rs/api/python/stable/reference/
- Plotly Dash: https://dash.plotly.com/
- Plotly Graphs: https://plotly.com/python/
- Dash Bootstrap Components: https://dash-bootstrap-components.opensource.faculty.ai/

## Common Species Codes

- GÄSMY: Gärdsmyg (Winter Wren)
- BLMES: Blåmes (Blue Tit)
- SKPIP: Skärpiplärka (Rock Pipit)
- RÖHAK: Rödhake (European Robin)
- KOTRA: Koltrast (Common Blackbird)
- JÄSPA: Järnsparv (Dunnock)
- TATRA: Taltrast (Song Thrush)
- STSTR: Större strandpipare (Ringed Plover)
- VIHÄM: Vinterhämpling (Twite)

---

**Last Updated**: 2026-02-20
**Database Version**: 1.0
**Schema Version**: 1.1

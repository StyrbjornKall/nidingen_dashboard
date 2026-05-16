"""
rebuild_precomputed_tables.py — Add / refresh precomputed lookup tables
=======================================================================

Run this script against an existing bird_ringing_0016.duckdb to create (or
recreate) the small precomputed tables that the dashboard reads at startup.
These tables are normally built automatically by convert_mdb_to_duckdb.py;
use this script when the database already exists and you don't want to
re-run the full MDB conversion.

Tables rebuilt
--------------
  species_list                  — All distinct species with taxonomy sort keys.
  rediscoveries_species_options — Species in fynd / frring tables.
  date_range_cache              — Single-row (min_date, max_date).
  year_list                     — Distinct calendar years.
  weather_daily                 — Daily aggregated weather summary.

Usage
-----
    uv run python app/src/preprocess_data/rebuild_precomputed_tables.py
    uv run python app/src/preprocess_data/rebuild_precomputed_tables.py --db /path/to/db
"""

import argparse
import sys
from pathlib import Path

import duckdb

_SCRIPT_DIR = Path(__file__).parent
_PROJECT_DIR = _SCRIPT_DIR.parent.parent.parent
_DEFAULT_DB = _PROJECT_DIR / "data" / "bird_ringing_0016.duckdb"


def rebuild_species_tables(conn: duckdb.DuckDBPyConnection) -> None:
    print("[1/3] Building species precomputed tables …")

    conn.execute("DROP TABLE IF EXISTS species_list")
    conn.execute("""
        CREATE TABLE species_list AS
        SELECT
            r.species_code,
            r.swedish_name,
            COALESCE(m.order_scientific_name, '~') AS order_name,
            COALESCE(m.family_scientific_name, '~') AS family_name,
            COALESCE(m.scientific_name, r.swedish_name, r.species_code) AS sci_name,
            m.english_name
        FROM (SELECT DISTINCT species_code, swedish_name FROM ring_records) r
        LEFT JOIN species_metadata m ON r.swedish_name = m.swedish_name
        ORDER BY order_name, family_name, sci_name
    """)
    n1 = conn.execute("SELECT COUNT(*) FROM species_list").fetchone()[0]
    print(f"  species_list: {n1} rows")

    conn.execute("DROP TABLE IF EXISTS rediscoveries_species_options")
    conn.execute("""
        CREATE TABLE rediscoveries_species_options AS
        SELECT src.species_code, al.swedish_name
        FROM (
            SELECT DISTINCT species_code FROM fynd
            UNION
            SELECT DISTINCT species_code FROM frring
        ) src
        LEFT JOIN artkod_lookup al ON src.species_code = al.artkod
        WHERE src.species_code IS NOT NULL
        ORDER BY src.species_code
    """)
    n2 = conn.execute("SELECT COUNT(*) FROM rediscoveries_species_options").fetchone()[0]
    print(f"  rediscoveries_species_options: {n2} rows")

    conn.execute("DROP TABLE IF EXISTS date_range_cache")
    conn.execute("""
        CREATE TABLE date_range_cache AS
        SELECT MIN(date) AS min_date, MAX(date) AS max_date
        FROM ring_records
        WHERE species_code != 'TOTAL' AND date IS NOT NULL
    """)
    print("  date_range_cache: 1 row")

    conn.execute("DROP TABLE IF EXISTS year_list")
    conn.execute("""
        CREATE TABLE year_list AS
        SELECT DISTINCT EXTRACT(YEAR FROM date)::INTEGER AS year
        FROM ring_records
        WHERE species_code != 'TOTAL' AND date IS NOT NULL
        ORDER BY year
    """)
    n4 = conn.execute("SELECT COUNT(*) FROM year_list").fetchone()[0]
    print(f"  year_list: {n4} rows")


def rebuild_weather_daily(conn: duckdb.DuckDBPyConnection) -> None:
    print("[2/3] Building weather_daily precomputed table …")

    # Check if hourly weather data exists
    n_weather = conn.execute("SELECT COUNT(*) FROM weather_data").fetchone()[0]
    if n_weather == 0:
        print("  weather_data is empty — skipping weather_daily (run fetch_smhi_weather.py first)")
        # Create empty table with correct schema so the app doesn't crash
        conn.execute("DROP TABLE IF EXISTS weather_daily")
        conn.execute("""
            CREATE TABLE weather_daily (
                date                DATE PRIMARY KEY,
                year                INTEGER,
                month               INTEGER,
                day_of_year         INTEGER,
                mean_temperature    DOUBLE,
                min_temperature     DOUBLE,
                max_temperature     DOUBLE,
                mean_wind_speed     DOUBLE,
                max_gust            DOUBLE,
                mean_wind_direction DOUBLE,
                mean_humidity       DOUBLE,
                total_precipitation DOUBLE,
                mean_pressure       DOUBLE,
                mean_visibility     DOUBLE,
                mean_cloud_cover    DOUBLE,
                data_completeness   DOUBLE,
                vinga_gap_fill_used BOOLEAN
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_weather_daily_date ON weather_daily(date)")
        return

    conn.execute("DROP TABLE IF EXISTS weather_daily")
    conn.execute("""
        CREATE TABLE weather_daily AS
        SELECT
            CAST(w.observation_time AS DATE)                       AS date,
            EXTRACT(YEAR  FROM CAST(w.observation_time AS DATE))::INTEGER AS year,
            EXTRACT(MONTH FROM CAST(w.observation_time AS DATE))::INTEGER AS month,
            EXTRACT(DOY   FROM CAST(w.observation_time AS DATE))::INTEGER AS day_of_year,
            AVG(w.temperature)                                     AS mean_temperature,
            MIN(w.temperature)                                     AS min_temperature,
            MAX(w.temperature)                                     AS max_temperature,
            AVG(w.wind_speed)                                      AS mean_wind_speed,
            MAX(w.gust_wind)                                       AS max_gust,
            AVG(w.wind_direction)                                  AS mean_wind_direction,
            AVG(w.humidity)                                        AS mean_humidity,
            SUM(COALESCE(w.precipitation, v.precipitation))        AS total_precipitation,
            AVG(COALESCE(w.pressure, v.pressure))                  AS mean_pressure,
            AVG(COALESCE(w.visibility, v.visibility))              AS mean_visibility,
            AVG(w.cloud_cover)                                     AS mean_cloud_cover,
            COUNT(w.temperature) * 1.0 / 24.0                     AS data_completeness,
            BOOL_OR(
                (w.precipitation IS NULL AND v.precipitation IS NOT NULL)
                OR (w.pressure   IS NULL AND v.pressure      IS NOT NULL)
                OR (w.visibility IS NULL AND v.visibility    IS NOT NULL)
            )                                                      AS vinga_gap_fill_used
        FROM weather_data w
        LEFT JOIN weather_data_vinga v ON w.observation_time = v.observation_time
        GROUP BY CAST(w.observation_time AS DATE)
        ORDER BY date
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_weather_daily_date ON weather_daily(date)")

    n = conn.execute("SELECT COUNT(*) FROM weather_daily").fetchone()[0]
    date_range = conn.execute("SELECT MIN(date), MAX(date) FROM weather_daily").fetchone()
    print(f"  weather_daily: {n:,} rows  ({date_range[0]} → {date_range[1]})")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=_DEFAULT_DB,
        help=f"Path to the DuckDB database (default: {_DEFAULT_DB})",
    )
    parser.add_argument(
        "--weather-only",
        action="store_true",
        help="Only rebuild weather_daily (skip species/date tables).",
    )
    parser.add_argument(
        "--species-only",
        action="store_true",
        help="Only rebuild species/date/year tables (skip weather_daily).",
    )
    args = parser.parse_args()

    if not args.db.exists():
        print(f"ERROR: Database not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    conn = duckdb.connect(str(args.db))
    conn.execute("SET memory_limit='4GB'")
    conn.execute("SET threads=4")

    if not args.weather_only:
        rebuild_species_tables(conn)

    if not args.species_only:
        rebuild_weather_daily(conn)

    print("[3/3] Running ANALYZE + CHECKPOINT …")
    conn.execute("ANALYZE")
    conn.execute("CHECKPOINT")
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()

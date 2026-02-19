"""
SMHI Open Data Weather Fetcher
==============================
Downloads hourly meteorological observations from SMHI for Nidingen A (station 71190)
and loads them into the DuckDB database.

Station: Nidingen A
Station ID: 71190
Coordinates: 57.3036°N, 11.9049°E
Height: 3.785 m

Parameters downloaded (all hourly, corrected-archive period):
  1  – Lufttemperatur      (Air temperature, °C, instantaneous)
  3  – Vindriktning        (Wind direction, °, 10-min mean)
  4  – Vindhastighet       (Wind speed, m/s, 10-min mean)
  6  – Relativ Luftfuktighet (Relative humidity, %, instantaneous)
  7  – Nederbördsmängd     (Precipitation, mm, hourly sum)
  9  – Lufttryck reducerat (Air pressure sea-level, hPa, instantaneous)
 16  – Total molnmängd     (Total cloud cover, %, instantaneous)
 21  – Byvind              (Gust wind speed, m/s, max in 1 h)

Usage:
    python src/fetch_smhi_weather.py
    python src/fetch_smhi_weather.py --db-path data/bird_ringing.db
    python src/fetch_smhi_weather.py --dry-run      # fetch but don't write to DB
"""

import argparse
import io
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

import requests
import polars as pl

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://opendata-download-metobs.smhi.se/api/version/1.0"
STATION_ID = 71190          # Nidingen A
STATION_NAME = "Nidingen A"
PERIOD = "corrected-archive"  # Full archive (CSV only); recent data via latest-months

# Parameters: (id, short_name, description, unit)
PARAMETERS = [
    (1,  "temperature",    "Air temperature",         "°C"),
    (3,  "wind_direction", "Wind direction",           "°"),
    (4,  "wind_speed",     "Wind speed (10-min mean)", "m/s"),
    (6,  "humidity",       "Relative humidity",        "%"),
    (7,  "precipitation",  "Precipitation (1-h sum)",  "mm"),
    (9,  "pressure",       "Air pressure (sea level)", "hPa"),
    (16, "cloud_cover",    "Total cloud cover",        "%"),
    (21, "gust_wind",      "Gust wind speed",          "m/s"),
]

# Nidingen ringing data starts around 2020; SMHI archive for station 71190
# goes back to 1969.  We download from 1980 for historical climate context.
START_YEAR = 1980

REQUEST_DELAY_S = 0.5   # polite delay between API calls

# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "nidingen-bird-dashboard/1.0 (research)"})


def _get(url: str, retries: int = 3, backoff: float = 5.0) -> requests.Response:
    """GET with automatic retry on transient errors."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=60)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 404:
                raise  # no point retrying a 404
            if attempt < retries - 1:
                wait = backoff * (2 ** attempt)
                print(f"  HTTP {status} — retrying in {wait:.0f}s …", file=sys.stderr)
                time.sleep(wait)
            else:
                raise
        except requests.exceptions.RequestException as exc:
            if attempt < retries - 1:
                wait = backoff * (2 ** attempt)
                print(f"  {exc} — retrying in {wait:.0f}s …", file=sys.stderr)
                time.sleep(wait)
            else:
                raise


# ---------------------------------------------------------------------------
# SMHI CSV parsing
# ---------------------------------------------------------------------------

def _parse_smhi_csv(csv_text: str, value_column: str) -> pl.DataFrame:
    """
    Parse a SMHI corrected-archive CSV response.

    The CSV format has several comment/header rows followed by data rows.
    Data columns (semi-colon separated):
        Datum;Tid (UTC);<value>;<quality>
    or for some parameters:
        Datum;Tid (UTC);<value>;<quality>;<extra>

    Returns a Polars DataFrame with columns:
        observation_time  TIMESTAMP (UTC, truncated to the hour)
        <value_column>    FLOAT64
        <value_column>_quality  VARCHAR
    """
    lines = csv_text.splitlines()

    # Find the header row — it starts with "Datum"
    header_idx = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.lower().startswith("datum"):
            header_idx = i
            break

    if header_idx is None:
        raise ValueError("Could not find data header row in SMHI CSV response")

    data_lines = lines[header_idx:]
    if len(data_lines) < 2:
        # No data rows
        return pl.DataFrame(
            {
                "observation_time": pl.Series([], dtype=pl.Datetime("us", "UTC")),
                value_column: pl.Series([], dtype=pl.Float64),
                f"{value_column}_quality": pl.Series([], dtype=pl.Utf8),
            }
        )

    csv_block = "\n".join(data_lines)

    df = pl.read_csv(
        io.StringIO(csv_block),
        separator=";",
        infer_schema_length=1000,
        ignore_errors=True,
    )

    # Rename columns robustly — positions: 0=Datum, 1=Tid(UTC), 2=value, 3=quality
    cols = df.columns
    rename_map = {}
    if len(cols) >= 1:
        rename_map[cols[0]] = "date_str"
    if len(cols) >= 2:
        rename_map[cols[1]] = "time_str"
    if len(cols) >= 3:
        rename_map[cols[2]] = value_column
    if len(cols) >= 4:
        rename_map[cols[3]] = f"{value_column}_quality"

    df = df.rename(rename_map)

    # Keep only the columns we care about
    keep = ["date_str", "time_str", value_column]
    quality_col = f"{value_column}_quality"
    if quality_col in df.columns:
        keep.append(quality_col)
    df = df.select([c for c in keep if c in df.columns])

    # Parse datetime
    df = df.with_columns(
        pl.concat_str(["date_str", "time_str"], separator=" ")
        .str.strptime(pl.Datetime("us"), format="%Y-%m-%d %H:%M:%S", strict=False)
        .dt.replace_time_zone("UTC")
        .alias("observation_time")
    ).drop(["date_str", "time_str"])

    # Cast value to float
    if value_column in df.columns:
        df = df.with_columns(
            pl.col(value_column)
            .cast(pl.Float64, strict=False)
        )

    # Add missing quality column if absent
    if quality_col not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(quality_col))

    # Drop rows where observation_time or value is null
    df = df.filter(
        pl.col("observation_time").is_not_null()
    )

    return df


# ---------------------------------------------------------------------------
# Fetching a single parameter
# ---------------------------------------------------------------------------

def fetch_parameter(param_id: int, value_column: str) -> pl.DataFrame:
    """
    Download *all* corrected-archive data for one parameter from Nidingen A.

    SMHI serves the full corrected archive as a single CSV file.
    For very recent data not yet in the corrected archive we also try
    latest-months and append it.

    Returns DataFrame with columns:
        observation_time, <value_column>, <value_column>_quality
    """
    frames = []

    # ---- corrected-archive (historical + corrected recent) ----
    url = f"{BASE_URL}/parameter/{param_id}/station/{STATION_ID}/period/{PERIOD}/data.csv"
    print(f"  Fetching parameter {param_id:>2} ({value_column}) … ", end="", flush=True)
    try:
        resp = _get(url)
        df = _parse_smhi_csv(resp.text, value_column)
        print(f"{len(df):>7,} rows (corrected-archive)")
        frames.append(df)
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        print(f"HTTP {status} — skipping corrected-archive")

    time.sleep(REQUEST_DELAY_S)

    # ---- latest-months (most recent ~4 months, not yet in archive) ----
    url_recent = f"{BASE_URL}/parameter/{param_id}/station/{STATION_ID}/period/latest-months/data.csv"
    try:
        resp_r = _get(url_recent)
        df_r = _parse_smhi_csv(resp_r.text, value_column)
        if len(df_r) > 0:
            print(f"    + {len(df_r):>6,} rows (latest-months)")
            frames.append(df_r)
    except requests.exceptions.HTTPError:
        pass  # Not all parameters support latest-months; silently skip

    time.sleep(REQUEST_DELAY_S)

    if not frames:
        return pl.DataFrame(
            {
                "observation_time": pl.Series([], dtype=pl.Datetime("us", "UTC")),
                value_column: pl.Series([], dtype=pl.Float64),
                f"{value_column}_quality": pl.Series([], dtype=pl.Utf8),
            }
        )

    combined = pl.concat(frames, how="diagonal_relaxed")

    # Deduplicate — keep last (latest-months wins over archive for overlapping rows)
    combined = combined.sort("observation_time").unique(
        subset=["observation_time"], keep="last"
    )

    # Filter to START_YEAR and onwards
    cutoff = datetime(START_YEAR, 1, 1, tzinfo=timezone.utc)
    combined = combined.filter(
        pl.col("observation_time") >= pl.lit(cutoff).dt.replace_time_zone("UTC")
    )

    return combined


# ---------------------------------------------------------------------------
# Merge all parameters into one wide table
# ---------------------------------------------------------------------------

def fetch_all_parameters() -> pl.DataFrame:
    """Fetch all configured parameters and join them on observation_time."""
    master: pl.DataFrame | None = None

    for param_id, short_name, _desc, _unit in PARAMETERS:
        df = fetch_parameter(param_id, short_name)
        if len(df) == 0:
            print(f"    WARNING: no data returned for parameter {param_id}")
            continue

        if master is None:
            master = df
        else:
            master = master.join(df, on="observation_time", how="full", coalesce=True)

    if master is None:
        raise RuntimeError("No weather data could be fetched from SMHI.")

    # Add station metadata columns
    master = master.with_columns([
        pl.lit(STATION_ID).cast(pl.Int32).alias("station_id"),
        pl.lit(STATION_NAME).alias("station_name"),
        pl.lit("SMHI-OpenData").alias("data_source"),
        pl.lit(datetime.now(timezone.utc)).alias("fetched_at"),
    ])

    # Sort by time
    master = master.sort("observation_time")
    return master


# ---------------------------------------------------------------------------
# Database loading
# ---------------------------------------------------------------------------

def load_into_db(df: pl.DataFrame, db_path: str) -> None:
    """
    Write the weather DataFrame into the DuckDB `weather_data` table.
    Existing rows with the same observation_time are replaced (upsert via
    DELETE + INSERT pattern).
    """
    # Import here so the script can still be used for --dry-run without a DB.
    sys.path.insert(0, str(Path(__file__).parent))
    from db_manager import BirdRingingDB

    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found: {db_path}\n"
            "Run `python src/initialize_database.py` first."
        )

    print(f"\nLoading {len(df):,} rows into {db_path.name} …")
    with BirdRingingDB(str(db_path)) as db:
        # Ensure the schema exists (idempotent)
        db.initialize_weather_schema()

        # Register the Polars frame as a DuckDB relation and upsert
        conn = db.conn
        conn.register("_new_weather", df)

        # Build the column list dynamically from the DataFrame
        cols = [c for c in df.columns if c != "fetched_at"]
        cols_sql = ", ".join(cols)

        # Delete rows that overlap in time with the incoming batch, then insert
        min_t = df["observation_time"].min()
        max_t = df["observation_time"].max()
        deleted = conn.execute(
            "DELETE FROM weather_data WHERE observation_time BETWEEN ? AND ?",
            [min_t, max_t],
        ).fetchone()[0]
        print(f"  Removed {deleted:,} pre-existing rows in time window.")

        conn.execute(f"""
            INSERT INTO weather_data ({cols_sql}, fetched_at)
            SELECT {cols_sql}, CURRENT_TIMESTAMP FROM _new_weather
        """)

        total = conn.execute("SELECT COUNT(*) FROM weather_data").fetchone()[0]
        print(f"  weather_data now has {total:,} rows.")

        conn.unregister("_new_weather")
        db.optimize_database()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download SMHI hourly weather data for Nidingen A and load into DuckDB."
    )
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).parent.parent / "data" / "bird_ringing.db"),
        help="Path to the DuckDB database (default: data/bird_ringing.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data and print a summary, but do NOT write to the database.",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="Optionally save the fetched data as a CSV file.",
    )
    args = parser.parse_args()

    print("=" * 60)
    print(f"SMHI Weather Fetcher — {STATION_NAME} (ID {STATION_ID})")
    print(f"Period: {START_YEAR} – present   |   {len(PARAMETERS)} parameters")
    print("=" * 60)

    df = fetch_all_parameters()

    print(f"\nFetched {len(df):,} hourly observations.")
    print(f"Time range: {df['observation_time'].min()} → {df['observation_time'].max()}")
    print("\nColumn summary:")
    for col in df.columns:
        if df[col].dtype == pl.Float64:
            non_null = df[col].drop_nulls()
            print(
                f"  {col:<25s} non-null={len(non_null):>7,}  "
                f"min={non_null.min():.2f}  max={non_null.max():.2f}"
                if len(non_null) > 0
                else f"  {col:<25s} all null"
            )

    if args.output_csv:
        out = Path(args.output_csv)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.write_csv(str(out))
        print(f"\nSaved to {out}")

    if args.dry_run:
        print("\n[dry-run] Skipping database write.")
    else:
        load_into_db(df, args.db_path)

    print("\nDone.")


if __name__ == "__main__":
    main()

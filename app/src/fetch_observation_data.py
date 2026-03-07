"""
Artdatabanken / SOS API Observation Fetcher
============================================
Download bird (Aves) observations from Nidingen (HI) using the SLU
Artdatabanken Species Observation System (SOS) API and store them in a
dedicated DuckDB database ``bird_observations.db``.

Station: Nidingen (island off the west coast of Sweden)
Station coordinates: 57.30250°N, 11.90056°E
Search radius: 5 km by default (covers the island and immediate waters)

Strategy
--------
The API limits synchronous CSV exports to 25 000 observations per request.
When more observations exist we iterate year-by-year (or in smaller windows)
so that each request stays below the limit.  Progress is printed to stdout
exactly like the SMHI weather fetcher.

Data stored
-----------
Every observation is stored flat (``Extended`` field set) in the
``observations`` table of ``bird_observations.db``.  Intermediary CSV files
are written to ``data/processed/`` for inspection.

API reference
-------------
Base URL  : https://api.artdatabanken.se/species-observation-system/v1
Endpoints :
  POST /Observations/Count           – count matching filter
  POST /Exports/Download/Csv         – sync CSV (≤ 25 000 rows)
  POST /Exports/Order/Csv            – async CSV order (≤ 2 M rows)
  GET  /Jobs/{jobId}/Status          – check async job status

Auth      : Ocp-Apim-Subscription-Key header (free subscription key)
Docs      : https://github.com/biodiversitydata-se/SOS/tree/master/Docs

Usage
-----
    python src/fetch_observation_data.py
    python src/fetch_observation_data.py --db-path data/bird_observations.db
    python src/fetch_observation_data.py --dry-run
    python src/fetch_observation_data.py --buffer-m 3000 --start-year 1990
    python src/fetch_observation_data.py --output-csv data/processed/obs.csv
"""

import argparse
import io
import sys
import time
from pathlib import Path
from datetime import datetime, timezone, date
from typing import Optional

import requests
import polars as pl

import os
import json
import zipfile

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://api.artdatabanken.se/species-observation-system/v1"

# Load API key from environment variable (set in .env or system env)
# Obtain a free key at https://api-portal.artdatabanken.se/
API_KEY = os.environ.get("ARTDATABANKEN_API_KEY", "")

# Nidingen lighthouse coordinates [longitude, latitude] — WGS84
NIDINGEN_COORDS = [11.90056, 57.30250]  # [lon, lat]

# Default search radius around the point (metres)
DEFAULT_BUFFER_M = 5000

# SOS Aves taxon ID (class Aves — all birds)
AVES_TAXON_ID = 4000104

# Artportalen data provider ID
ARTPORTALEN_DATA_PROVIDER_ID = 1

# Export limits (from SOS docs)
SYNC_EXPORT_LIMIT = 25_000      # rows returned inline
ASYNC_EXPORT_LIMIT = 2_000_000  # rows via email link

# Earliest year to download
START_YEAR = 1950

# Field set for CSV exports: "Minimum" | "Extended" | "AllWithValues"
FIELD_SET = "Extended"

# Polite delay between API calls (seconds) — keeps us well under rate limits
REQUEST_DELAY_S = 2.0

# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "nidingen-bird-dashboard/1.0 (research)",
    "Content-Type": "application/json",
    "Accept": "application/json",
})
if API_KEY:
    SESSION.headers["Ocp-Apim-Subscription-Key"] = API_KEY


def _post(
    url: str,
    body: dict,
    retries: int = 6,
    backoff: float = 30.0,
    extra_headers: Optional[dict] = None,
) -> requests.Response:
    """POST with automatic retry on transient errors.

    ``extra_headers`` are merged into the request (but not the session), so
    individual endpoints can override e.g. ``Accept`` without side effects.

    On HTTP 429 the ``Retry-After`` response header is respected; if absent
    the wait doubles from *backoff* seconds up to a cap of 5 minutes.
    """
    for attempt in range(retries):
        try:
            resp = SESSION.post(url, json=body, timeout=120, headers=extra_headers)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in (400, 401, 403, 404):
                raise  # non-retryable
            if attempt < retries - 1:
                if status == 429:
                    retry_after = exc.response.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after else min(backoff * (2 ** attempt), 300)
                else:
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


def _get(url: str, retries: int = 3, backoff: float = 5.0) -> requests.Response:
    """GET with automatic retry on transient errors."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=60)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in (400, 401, 403, 404):
                raise
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
# Search filter builder
# ---------------------------------------------------------------------------

def build_filter(
    start_date: str,
    end_date: str,
    lon: float = NIDINGEN_COORDS[0],
    lat: float = NIDINGEN_COORDS[1],
    buffer_m: int = DEFAULT_BUFFER_M,
    taxon_id: int = AVES_TAXON_ID,
) -> dict:
    """
    Build a SOS API search filter body for bird observations around a point.

    Parameters
    ----------
    start_date : str
        ISO date string ``YYYY-MM-DD`` (inclusive).
    end_date : str
        ISO date string ``YYYY-MM-DD`` (inclusive).
    lon, lat : float
        WGS84 coordinates of the search centre [lon, lat].
    buffer_m : int
        Radius in metres around the point.
    taxon_id : int
        Dyntaxa taxon ID.  Default is Aves (4000104).

    Returns
    -------
    dict
        JSON-serialisable filter body accepted by ``/Observations/Count`` and
        ``/Exports/Download/Csv``.
    """
    return {
        "taxon": {
            "ids": [taxon_id],
            "includeUnderlyingTaxa": True,
        },
        "date": {
            "startDate": start_date,
            "endDate": end_date,
            "dateFilterType": "BetweenStartDateAndEndDate",
        },
        "geographics": {
            "geometries": [
                {"type": "point", "coordinates": [lon, lat]}
            ],
            "maxDistanceFromPoint": buffer_m,
            "considerObservationAccuracy": False,
        },
        # Artportalen only (data provider 1) for clean ringing-station data
        "dataProvider": {
            "ids": [ARTPORTALEN_DATA_PROVIDER_ID]
        },
        "output": {
            "fieldSet": FIELD_SET,
        },
    }


# ---------------------------------------------------------------------------
# Count observations
# ---------------------------------------------------------------------------

def count_observations(
    start_date: str,
    end_date: str,
    buffer_m: int = DEFAULT_BUFFER_M,
) -> int:
    """
    Return the number of observations matching the filter for the given window.
    """
    body = build_filter(start_date, end_date, buffer_m=buffer_m)
    # Remove output from count request (not needed)
    body.pop("output", None)

    url = f"{BASE_URL}/Observations/Count"
    resp = _post(url, body)
    return int(resp.json())


# ---------------------------------------------------------------------------
# Download CSV for a single time window
# ---------------------------------------------------------------------------

def download_csv_window(
    start_date: str,
    end_date: str,
    buffer_m: int = DEFAULT_BUFFER_M,
) -> pl.DataFrame:
    """
    Download up to ``SYNC_EXPORT_LIMIT`` observations as a CSV and return a
    Polars DataFrame.

    Raises
    ------
    ValueError
        If the window contains more than ``SYNC_EXPORT_LIMIT`` observations.
        Call ``count_observations`` first and split the window if needed.
    RuntimeError
        If the API returns an unexpected response.
    """
    body = build_filter(start_date, end_date, buffer_m=buffer_m)

    url = f"{BASE_URL}/Exports/Download/Csv"
    params = {
        "cultureCode": "en-GB",
        "propertyLabelType": "PropertyName",  # English camelCase column names
    }

    # Add query params to URL
    full_url = url + "?" + "&".join(f"{k}={v}" for k, v in params.items())

    # Override session-level Accept so the API returns text/csv, not a ZIP.
    # The session default is 'application/json' which causes the export endpoint
    # to wrap the CSV in a ZIP archive for some (usually older) requests.
    resp = _post(full_url, body, extra_headers={"Accept": "text/csv, */*"})

    content_type = resp.headers.get("Content-Type", "").lower()

    # --- Handle ZIP-wrapped CSV -------------------------------------------------
    # The API sometimes returns a ZIP archive (Content-Type: application/zip or
    # application/octet-stream) even when text/csv is requested.  Unzip and take
    # the first .csv entry.
    raw_bytes = resp.content
    if "zip" in content_type or "octet-stream" in content_type or (
        raw_bytes[:2] == b"PK"  # ZIP magic bytes
    ):
        try:
            with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
                csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not csv_names:
                    raise RuntimeError(
                        f"ZIP response from {url} contains no .csv entries: {zf.namelist()}"
                    )
                raw_bytes = zf.read(csv_names[0])
        except zipfile.BadZipFile as exc:
            raise RuntimeError(
                f"Response looked like a ZIP but could not be unzipped: {exc}\n"
                f"First 50 bytes (hex): {resp.content[:50].hex()}"
            ) from exc

    # --- Reject tiny non-CSV bodies -------------------------------------------
    if "csv" not in content_type and len(raw_bytes) < 10:
        raise RuntimeError(
            f"Unexpected response from {url}: "
            f"status={resp.status_code}, content-type={content_type!r}, "
            f"body={resp.content[:200]!r}"
        )

    # --- Decode text -----------------------------------------------------------
    # Try UTF-8 (with BOM), then windows-1252, then latin-1.
    # Older Artportalen records are sometimes returned in windows-1252.
    csv_text = None
    for encoding in ("utf-8-sig", "windows-1252", "latin-1"):
        try:
            csv_text = raw_bytes.decode(encoding)
            break
        except (UnicodeDecodeError, LookupError):
            continue
    if csv_text is None:
        raise RuntimeError(
            "Could not decode CSV response in utf-8, windows-1252 or latin-1."
        )

    if not csv_text.strip():
        # No observations in this window
        return pl.DataFrame()

    try:
        df = pl.read_csv(
            io.StringIO(csv_text),
            separator="\t",
            infer_schema_length=5000,
            ignore_errors=True,
            truncate_ragged_lines=True,
            null_values=["", "null", "NULL"],
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to parse CSV response: {exc}\nFirst 500 chars: {csv_text[:500]}"
        ) from exc

    return df


# ---------------------------------------------------------------------------
# Fetch all years with year-by-year iteration
# ---------------------------------------------------------------------------

def fetch_all_observations(
    start_year: int = START_YEAR,
    end_year: Optional[int] = None,
    buffer_m: int = DEFAULT_BUFFER_M,
    chunk_years: int = 10,
) -> pl.DataFrame:
    """
    Fetch all bird observations near Nidingen from *start_year* to *end_year*
    (inclusive, defaults to the current year).

    The function first tries to download *chunk_years* years at a time with a
    single request (fast path for sparse historical data).  If a chunk exceeds
    ``SYNC_EXPORT_LIMIT`` it falls back to year-by-year.  A single year that
    still exceeds the limit is split into monthly windows, and a month that
    exceeds it is split into individual days.

    Parameters
    ----------
    chunk_years : int
        Number of years to attempt per request (default 10).  Set to 1 to
        revert to the original year-by-year behaviour.

    Returns
    -------
    pl.DataFrame
        All observations concatenated, with duplicate ``OccurrenceId`` rows
        removed (keeps last).
    """
    if end_year is None:
        end_year = datetime.now(timezone.utc).year

    all_frames: list[pl.DataFrame] = []
    total_downloaded = 0

    print(f"Fetching Aves observations near Nidingen ({NIDINGEN_COORDS[1]}°N, {NIDINGEN_COORDS[0]}°E)")
    print(f"Radius: {buffer_m} m  |  Years: {start_year}–{end_year}  |  Field set: {FIELD_SET}")
    print(f"Chunk size: {chunk_years} year(s) per request")
    print("=" * 60)

    year = start_year
    while year <= end_year:
        chunk_end_year = min(year + chunk_years - 1, end_year)
        chunk_start = f"{year}-01-01"
        chunk_end = f"{chunk_end_year}-12-31"
        label = f"{year}" if chunk_end_year == year else f"{year}–{chunk_end_year}"

        print(f"\n[{label}] Counting … ", end="", flush=True)
        try:
            n = count_observations(chunk_start, chunk_end, buffer_m=buffer_m)
        except requests.exceptions.HTTPError as exc:
            print(f"HTTP error {exc.response.status_code} — skipping chunk")
            time.sleep(REQUEST_DELAY_S)
            year = chunk_end_year + 1
            continue
        print(f"{n:,} observations")
        time.sleep(REQUEST_DELAY_S)

        if n == 0:
            year = chunk_end_year + 1
            continue

        if n <= SYNC_EXPORT_LIMIT:
            # Whole chunk fits in a single request
            print(f"  Downloading {label} … ", end="", flush=True)
            try:
                df = download_csv_window(chunk_start, chunk_end, buffer_m=buffer_m)
                print(f"{len(df):,} rows")
                if len(df) > 0:
                    all_frames.append(df)
                    total_downloaded += len(df)
            except Exception as exc:
                print(f"ERROR: {exc}")
            time.sleep(REQUEST_DELAY_S)
        else:
            # Chunk too large — fall back to year-by-year within the chunk
            print(f"  Chunk exceeds sync limit — splitting year by year …")
            for yr in range(year, chunk_end_year + 1):
                year_start = f"{yr}-01-01"
                year_end_s = f"{yr}-12-31"

                print(f"  [{yr}] Counting … ", end="", flush=True)
                try:
                    ny = count_observations(year_start, year_end_s, buffer_m=buffer_m)
                except requests.exceptions.HTTPError as exc:
                    print(f"HTTP error {exc.response.status_code} — skipping year")
                    time.sleep(REQUEST_DELAY_S)
                    continue
                print(f"{ny:,} observations")
                time.sleep(REQUEST_DELAY_S)

                if ny == 0:
                    continue

                if ny <= SYNC_EXPORT_LIMIT:
                    print(f"    Downloading {yr} … ", end="", flush=True)
                    try:
                        df = download_csv_window(year_start, year_end_s, buffer_m=buffer_m)
                        print(f"{len(df):,} rows")
                        if len(df) > 0:
                            all_frames.append(df)
                            total_downloaded += len(df)
                    except Exception as exc:
                        print(f"ERROR: {exc}")
                    time.sleep(REQUEST_DELAY_S)
                else:
                    # Year too large — split into months/days
                    print(f"    Exceeds sync limit — splitting into months …")
                    df_year = _fetch_chunked(yr, buffer_m=buffer_m)
                    if df_year is not None and len(df_year) > 0:
                        all_frames.append(df_year)
                        total_downloaded += len(df_year)

        year = chunk_end_year + 1

    if not all_frames:
        print("\nNo observations fetched.")
        return pl.DataFrame()

    print(f"\n{'='*60}")
    print(f"Total rows fetched (before dedup): {total_downloaded:,}")

    combined = pl.concat(all_frames, how="diagonal_relaxed")

    # Deduplicate by OccurrenceId if that column exists
    if "OccurrenceId" in combined.columns:
        before = len(combined)
        combined = combined.unique(subset=["OccurrenceId"], keep="last")
        removed = before - len(combined)
        if removed:
            print(f"Removed {removed:,} duplicate OccurrenceId rows.")

    print(f"Final row count: {len(combined):,}")
    return combined


def _fetch_chunked(year: int, buffer_m: int) -> Optional[pl.DataFrame]:
    """
    Fetch a single *year* by iterating over months, and if needed days.
    Returns a concatenated DataFrame or None if nothing was found.
    """
    import calendar

    frames: list[pl.DataFrame] = []

    for month in range(1, 13):
        _, last_day = calendar.monthrange(year, month)
        m_start = f"{year}-{month:02d}-01"
        m_end = f"{year}-{month:02d}-{last_day:02d}"

        print(f"    [{year}-{month:02d}] Counting … ", end="", flush=True)
        try:
            n = count_observations(m_start, m_end, buffer_m=buffer_m)
        except requests.exceptions.HTTPError as exc:
            print(f"HTTP {exc.response.status_code} — skipping")
            time.sleep(REQUEST_DELAY_S)
            continue
        print(f"{n:,}")
        time.sleep(REQUEST_DELAY_S)

        if n == 0:
            continue

        if n <= SYNC_EXPORT_LIMIT:
            print(f"    Downloading {year}-{month:02d} … ", end="", flush=True)
            try:
                df = download_csv_window(m_start, m_end, buffer_m=buffer_m)
                print(f"{len(df):,} rows")
                if len(df) > 0:
                    frames.append(df)
            except Exception as exc:
                print(f"ERROR: {exc}")
            time.sleep(REQUEST_DELAY_S)
        else:
            # Fall back to day-by-day for this month
            print(f"    Month exceeds sync limit — splitting into days …")
            for day in range(1, last_day + 1):
                d_str = f"{year}-{month:02d}-{day:02d}"
                try:
                    df = download_csv_window(d_str, d_str, buffer_m=buffer_m)
                    if len(df) > 0:
                        frames.append(df)
                        print(f"      {d_str}: {len(df):,} rows")
                except Exception as exc:
                    print(f"      {d_str}: ERROR {exc}")
                time.sleep(REQUEST_DELAY_S)

    if not frames:
        return None
    return pl.concat(frames, how="diagonal_relaxed")


# ---------------------------------------------------------------------------
# Database loading
# ---------------------------------------------------------------------------

def initialize_observations_schema(conn) -> None:
    """
    Create the ``observations`` table and its indexes in *conn* (a DuckDB
    connection).  Safe to call repeatedly (uses IF NOT EXISTS).
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS observations (
            -- Darwin Core / SOS identifiers
            OccurrenceId          VARCHAR PRIMARY KEY,
            DatasetName           VARCHAR,

            -- Temporal
            StartDate             TIMESTAMP,
            EndDate               TIMESTAMP,

            -- Spatial
            DecimalLatitude       DOUBLE,
            DecimalLongitude      DOUBLE,
            CoordinateUncertaintyInMeters DOUBLE,
            Municipality          VARCHAR,
            County                VARCHAR,
            Locality              VARCHAR,
            Province              VARCHAR,

            -- Taxon
            DyntaxaTaxonId        INTEGER,
            ScientificName        VARCHAR,
            VernacularName        VARCHAR,
            OrganismGroup         VARCHAR,
            Family                VARCHAR,
            "Order"               VARCHAR,
            Class                 VARCHAR,
            Kingdom               VARCHAR,
            RedlistCategory       VARCHAR,

            -- Occurrence
            IndividualCount       VARCHAR,
            OrganismQuantity      VARCHAR,
            OrganismQuantityInt   INTEGER,
            OrganismQuantityUnit  VARCHAR,
            OccurrenceStatus      VARCHAR,
            RecordedBy            VARCHAR,
            ReportedBy            VARCHAR,
            Sex                   VARCHAR,
            LifeStage             VARCHAR,
            Activity              VARCHAR,
            Behavior              VARCHAR,
            Biotope               VARCHAR,
            OccurrenceRemarks     VARCHAR,
            Weight                INTEGER,
            Length                INTEGER,

            -- Identification / verification
            Verified              BOOLEAN,
            UncertainIdentification BOOLEAN,
            VerificationStatus    VARCHAR,

            -- Extra extended fields
            BasisOfRecord         VARCHAR,
            DataProviderId        INTEGER,
            Modified              TIMESTAMP,
            Url                   VARCHAR,
            Projects              VARCHAR,

            -- Metadata
            fetched_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_obs_start_date
        ON observations(CAST(StartDate AS DATE))
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_obs_taxon
        ON observations(DyntaxaTaxonId)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_obs_species
        ON observations(ScientificName)
    """)
    print("Observations schema initialized.")


def load_into_db(
    df: pl.DataFrame,
    db_path: str,
) -> None:
    """
    Write observation rows into the ``observations`` table in *db_path*
    (DuckDB file ``bird_observations.db``).

    Uses upsert-style DELETE + INSERT based on ``OccurrenceId`` so that
    re-running the script does not create duplicates.
    """
    import duckdb

    db_path_obj = Path(db_path)
    db_path_obj.parent.mkdir(parents=True, exist_ok=True)

    print(f"\nLoading {len(df):,} rows into 'observations' in {db_path_obj.name} …")

    conn = duckdb.connect(str(db_path_obj))
    conn.execute("SET memory_limit='2GB'")
    conn.execute("SET threads=4")

    initialize_observations_schema(conn)

    # Select only columns that exist in both the DataFrame and the table schema
    table_cols_result = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'observations' AND column_name != 'fetched_at'"
    ).fetchall()
    table_cols = {row[0] for row in table_cols_result}

    # Normalise DataFrame column names to match schema (SOS returns PascalCase)
    # Handle the quoted "Order" column — rename if df has it
    df_cols = set(df.columns)
    available = [c for c in df.columns if c in table_cols]

    if not available:
        print("  WARNING: no matching columns found between DataFrame and schema.")
        conn.close()
        return

    df_subset = df.select(available)

    # Cast date columns to Datetime if they are strings
    # SOS returns ISO 8601 with offset e.g. "2023-04-15T06:00:00+02:00"
    for col in ("StartDate", "EndDate", "Modified"):
        if col in df_subset.columns and df_subset[col].dtype == pl.Utf8:
            # Parse eagerly on the Series (avoids Polars lazy tz inference error)
            parsed = (
                df_subset[col]
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False)
                .dt.convert_time_zone("UTC")
                .dt.replace_time_zone(None)
            )
            df_subset = df_subset.with_columns(parsed.alias(col))

    # Cast integer columns
    for col in ("DyntaxaTaxonId", "OrganismQuantityInt", "Weight", "Length", "DataProviderId"):
        if col in df_subset.columns and df_subset[col].dtype != pl.Int32:
            df_subset = df_subset.with_columns(
                pl.col(col).cast(pl.Int32, strict=False).alias(col)
            )

    # Cast boolean columns
    for col in ("Verified", "UncertainIdentification"):
        if col in df_subset.columns and df_subset[col].dtype not in (pl.Boolean,):
            df_subset = df_subset.with_columns(
                pl.col(col).cast(pl.Boolean, strict=False).alias(col)
            )

    conn.register("_new_obs", df_subset)

    # Delete existing rows with the same OccurrenceId, then insert fresh rows
    if "OccurrenceId" in available:
        deleted = conn.execute("""
            DELETE FROM observations
            WHERE OccurrenceId IN (SELECT OccurrenceId FROM _new_obs)
        """).fetchone()[0]
        print(f"  Removed {deleted:,} pre-existing rows (same OccurrenceId).")

    cols_sql = ", ".join(f'"{c}"' for c in available)
    conn.execute(f"""
        INSERT INTO observations ({cols_sql}, fetched_at)
        SELECT {cols_sql}, CURRENT_TIMESTAMP FROM _new_obs
    """)

    total = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    print(f"  'observations' table now has {total:,} rows.")

    conn.unregister("_new_obs")
    conn.execute("ANALYZE")
    conn.execute("CHECKPOINT")
    conn.close()
    print("  Database optimized and closed.")


# ---------------------------------------------------------------------------
# Column summary helper (mirrors SMHI fetcher output)
# ---------------------------------------------------------------------------

def _print_column_summary(df: pl.DataFrame) -> None:
    """Print a brief summary of key columns in the fetched DataFrame."""
    key_cols = [
        "OccurrenceId", "StartDate", "EndDate",
        "ScientificName", "VernacularName", "IndividualCount",
        "DecimalLatitude", "DecimalLongitude", "Locality",
        "RecordedBy", "OccurrenceStatus",
    ]
    print("\nColumn summary:")
    for col in key_cols:
        if col in df.columns:
            non_null = df[col].drop_nulls()
            n = len(non_null)
            if df[col].dtype in (pl.Float64, pl.Float32):
                sample = f"min={non_null.min():.4f}  max={non_null.max():.4f}" if n > 0 else "all null"
            elif df[col].dtype in (pl.Int32, pl.Int64):
                sample = f"min={non_null.min()}  max={non_null.max()}" if n > 0 else "all null"
            else:
                sample = f"sample={non_null.head(3).to_list()}" if n > 0 else "all null"
            print(f"  {col:<35s} non-null={n:>7,}  {sample}")
        else:
            print(f"  {col:<35s} (not in response)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Download Aves observations near Nidingen from the Artdatabanken "
            "SOS API and store them in bird_observations.db."
        )
    )
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).parent.parent / "data" / "bird_observations.db"),
        help="Path to the output DuckDB database (default: data/bird_observations.db)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=START_YEAR,
        help=f"First year to download (default: {START_YEAR})",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Last year to download (default: current year)",
    )
    parser.add_argument(
        "--buffer-m",
        type=int,
        default=DEFAULT_BUFFER_M,
        help=f"Search radius around Nidingen in metres (default: {DEFAULT_BUFFER_M})",
    )
    parser.add_argument(
        "--chunk-years",
        type=int,
        default=10,
        help="Number of years to attempt per request before falling back to year-by-year (default: 10)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data and print a summary but do NOT write to disk.",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="Optionally save the fetched data as a CSV file.",
    )
    args = parser.parse_args()

    if not API_KEY:
        print(
            "WARNING: ARTDATABANKEN_API_KEY environment variable is not set.\n"
            "         Requests will likely return HTTP 401.\n"
            "         Set the variable or pass it via a .env file.\n",
            file=sys.stderr,
        )

    print("=" * 60)
    print("Artdatabanken SOS Observation Fetcher")
    print(f"Station : Nidingen  ({NIDINGEN_COORDS[1]}°N, {NIDINGEN_COORDS[0]}°E)")
    print(f"Radius  : {args.buffer_m} m")
    print(f"Period  : {args.start_year} – {args.end_year or datetime.now().year}")
    print(f"Taxon   : Aves (id {AVES_TAXON_ID})")
    print("=" * 60)

    df = fetch_all_observations(
        start_year=args.start_year,
        end_year=args.end_year,
        buffer_m=args.buffer_m,
        chunk_years=args.chunk_years,
    )

    if len(df) == 0:
        print("No observations fetched — nothing to store.")
        return

    _print_column_summary(df)

    if args.output_csv:
        out = Path(args.output_csv)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.write_csv(str(out))
        print(f"\nSaved CSV to {out}")

    if args.dry_run:
        print("\n[dry-run] Skipping database write.")
    else:
        # Also save CSV to default processed location as intermediary
        default_csv = Path(args.db_path).parent / "processed" / "observations_nidingen.csv"
        default_csv.parent.mkdir(parents=True, exist_ok=True)
        df.write_csv(str(default_csv))
        print(f"\nIntermediary CSV saved to {default_csv}")

        load_into_db(df, args.db_path)

    print("\nDone.")


if __name__ == "__main__":
    main()


"""
convert_mdb_to_duckdb.py — Migrate RingDb0016.mdb → bird_ringing_0016.duckdb

This is the primary database setup entry point. It replaces the old
initialize_database.py + preprocess_raw_data.py pipeline by reading directly
from the authoritative Access database (RingDb0016.mdb).

Tables migrated
---------------
  ringon      — All birds originally ringed at Nidingen (0016NID), 395 k rows
  kontr       — Recaptures at Nidingen of birds ringed there previously
  fynd        — Recoveries of Nidingen-ringed birds reported from elsewhere
  frring      — Foreign-ringed birds caught at Nidingen
  signaturer  — Lookup: ringer code → full name
  lokaler     — Lookup: location code → coordinates

Derived tables
--------------
  artkod_lookup    — RUBIN species code (e.g. GÄSMY) → Swedish common name
  species_metadata — Full taxonomy (from combined_species_metadata.csv)
  ring_records     — Materialized compatibility table: ringon ∪ kontr, with
                     all old dashboard columns + new columns (sex, trap_type,
                     bio fields, etc.) + TOTAL aggregate rows
  weather_data         — SMHI Nidingen A (empty — run fetch_smhi_weather.py)
  weather_data_vinga   — SMHI Vinga A (empty — run fetch_smhi_weather.py)

Usage
-----
    uv run python app/src/convert_mdb_to_duckdb.py
    uv run python app/src/convert_mdb_to_duckdb.py --mdb data/RingDb0016.mdb --db data/bird_ringing_0016.duckdb

Re-running the script is safe: all bird tables are dropped and recreated from
the MDB. Weather tables are preserved if they already exist.
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl
import pyodbc

# ── default paths ────────────────────────────────────────────────────────────
_SCRIPT_DIR = Path(__file__).parent
_PROJECT_DIR = _SCRIPT_DIR.parent.parent
_DEFAULT_MDB = _PROJECT_DIR / "data" / "RingDb0016.mdb"
_DEFAULT_DB  = _PROJECT_DIR / "data" / "bird_ringing_0016.duckdb"
_METADATA_CSV = _PROJECT_DIR / "data" / "processed" / "combined_species_metadata.csv"


# ── column rename maps ───────────────────────────────────────────────────────

# Shared observation core (Ringon, Kontr, Fynd, FrRing all have these)
_CORE = {
    "Ring":      "ring_number",
    "Centr":     "centre",
    "Lokal":     "location",
    "Tr":        "trap_type",
    "IdKull":    "brood_id",
    "Datum":     "date",
    "Tim":       "hour",
    "Artkod":    "species_code",
    "Signatur":  "ringer_code",
    "Sex":       "sex",
    "Age":       "age_code",
    "Status":    "status",
    "Varn":      "warning_flag",
    "Pullus":    "pullus",
    "Reserv":    "reserved",
    "Vinge":     "wing_length",
    "Vikt":      "weight",
    "Fett":      "fat_score",
    "PJM":       "partial_juv_moult",
    "Kondition": "condition",
    "Textrad":   "notes",
    "Typ1":      "mark_type_1",
    "Text1":     "mark_text_1",
    "Color1":    "mark_color_1",
    "Typ2":      "mark_type_2",
    "Text2":     "mark_text_2",
    "Color2":    "mark_color_2",
}

# Bio measurement codes/values (present in Ringon and Kontr, not in Fynd/FrRing)
_BIO = {
    "B1":   "bio_code_1",
    "Bio1": "bio_value_1",
    "B2":   "bio_code_2",
    "Bio2": "bio_value_2",
    "B3":   "bio_code_3",
    "Bio3": "bio_value_3",
    "B4":   "bio_code_4",
    "Bio4": "bio_value_4",    # VARCHAR in Access
    "B5":   "bio_code_5",
    "Bio5": "bio_value_5",    # VARCHAR
    "B6":   "bio_code_6",
    "Bio6": "bio_value_6",    # VARCHAR
    "B7":   "bio_code_7",
    "Bio7": "bio_value_7",    # VARCHAR
}

# Ringon-specific: station code, foreign-bird origin
_RINGON_EXTRA = {
    "Mnr":    "station_code",
    "Utland": "foreign_bird",
    "GCentr": "original_centre",
    "GRing":  "original_ring",
}

# Kontr-specific: station code, recapture timing, serial number
_KONTR_EXTRA = {
    "Mnr":     "station_code",
    "Utland":  "foreign_bird",
    "FKND":    "find_type",
    "FDT":     "find_date_type",
    "FDTA":    "find_date_accuracy",
    "Distans": "distance_km",
    "Kurs":    "bearing_deg",
    "Dagar":   "days_since_ring",
    "Timmar":  "hours_since_ring",
    "LöpNr":   "serial_number",
}

# Fynd-specific: recovery metadata, geography; note no Mnr (has Rapp instead)
_FYND_EXTRA = {
    "Rapp":      "report_number",
    "FKND":      "find_type",
    "FDT":       "find_date_type",
    "FDTA":      "find_date_accuracy",
    "Distans":   "distance_km",
    "Kurs":      "bearing_deg",
    "Dagar":     "days_since_ring",
    "Timmar":    "hours_since_ring",
    "NyCentr":   "new_centre",
    "NyRing":    "new_ring",
    "RappCentr": "report_centre",
    "RappRing":  "report_ring",
    "RappArt":   "report_species",
    "RappTyp":   "report_type",
    "Dnr":       "case_number",
    "WGS84":     "wgs84",
    "Latitud":   "latitude",
    "Longitud":  "longitude",
    "KNog":      "coordinate_accuracy",
    "Prov":      "province_code",
    "Storort":   "city",
    "Litenort":  "locality",
    "Rapportör": "reporter",
    "SigNamn":   "reporter_name",
}

# FrRing-specific: station code, origin of the foreign ring, geography
_FRRING_EXTRA = {
    "Mnr":      "station_code",
    "GCentr":   "original_centre",
    "GRing":    "original_ring",
    "WGS84":    "wgs84",
    "Latitud":  "latitude",
    "Longitud": "longitude",
    "KNog":     "coordinate_accuracy",
    "Prov":     "province_code",
    "Storort":  "city",
    "Litenort": "locality",
    "Märkare":  "original_ringer_name",
    "SigNamn":  "recovery_ringer_name",
}

_SIGNATURER = {"Mnr": "station_code", "Signatur": "ringer_code", "SigNamn": "full_name"}
_LOKALER    = {
    "Lokal":     "location",
    "Prov":      "province_code",
    "Storort":   "city",
    "Litenort":  "locality",
    "Kommentar": "comment",
    "WGS84":     "wgs84",
    "Ruta":      "grid_square",
    "RT90nord":  "rt90_north",
    "RT90ost":   "rt90_east",
    "Latitud":   "latitude",
    "Longitud":  "longitude",
    "KNog":      "coordinate_accuracy",
    "TypIN":     "location_type",
    "Mnr":       "station_code",
}


# ── helpers ──────────────────────────────────────────────────────────────────

def _open_mdb(mdb_path: Path) -> pyodbc.Connection:
    """Open the Access database with Swedish cp1252 encoding."""
    conn_str = (
        f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};"
        f"DBQ={mdb_path};"
    )
    conn = pyodbc.connect(conn_str)
    conn.setdecoding(pyodbc.SQL_CHAR,  encoding="cp1252")
    conn.setdecoding(pyodbc.SQL_WCHAR, encoding="cp1252")
    return conn


def _read_table(
    cursor: pyodbc.Cursor,
    table_name: str,
    where: str | None = None,
) -> pl.DataFrame:
    """
    Read an Access table into a Polars DataFrame.

    All string columns are stripped of leading/trailing whitespace.
    datetime columns from Access become pl.Date (time component is always 00:00).
    """
    sql = f"SELECT * FROM [{table_name}]"
    if where:
        sql += f" WHERE {where}"
    cursor.execute(sql)

    col_names   = [d[0] for d in cursor.description]
    rows        = cursor.fetchall()

    if not rows:
        # Return empty DataFrame with correct column names
        return pl.DataFrame({c: [] for c in col_names})

    # Transpose: list-of-rows → dict-of-columns
    data = {col: [] for col in col_names}
    for row in rows:
        for col, val in zip(col_names, row):
            data[col].append(val)

    # Build Polars series per column with appropriate types
    series = []
    for col, vals in data.items():
        # Detect datetime columns (Access DATETIME comes through as Python datetime)
        if any(isinstance(v, __import__("datetime").datetime) for v in vals if v is not None):
            # Keep only the date part; Access stores ringing dates as midnight datetimes
            import datetime as _dt
            date_vals = [v.date() if isinstance(v, _dt.datetime) else v for v in vals]
            series.append(pl.Series(col, date_vals, dtype=pl.Date))
        elif all(v is None or isinstance(v, str) for v in vals):
            # Strip whitespace from string columns
            cleaned = [v.strip() if isinstance(v, str) else v for v in vals]
            series.append(pl.Series(col, cleaned, dtype=pl.Utf8))
        else:
            series.append(pl.Series(col, vals))

    return pl.DataFrame(series)


def _rename(df: pl.DataFrame, *maps: dict) -> pl.DataFrame:
    """Apply one or more rename dicts sequentially."""
    combined = {}
    for m in maps:
        combined.update(m)
    # Only rename columns that actually exist in the DataFrame
    actual = {k: v for k, v in combined.items() if k in df.columns}
    return df.rename(actual)


def _clean_measurements(df: pl.DataFrame) -> pl.DataFrame:
    """
    Replace sentinel 'not measured' values with null.

    Conventions used in RingDb0016:
      wing_length = 0   → not measured
      weight      = 0.0 → not measured
      fat_score   = 10  → not measured  (valid scale: 0–9)
      partial_juv_moult = 10 → not measured  (valid scale: 0–9)
    """
    exprs = []
    if "wing_length" in df.columns:
        exprs.append(
            pl.when(pl.col("wing_length") == 0)
            .then(None)
            .otherwise(pl.col("wing_length"))
            .alias("wing_length")
        )
    if "weight" in df.columns:
        exprs.append(
            pl.when(pl.col("weight") == 0.0)
            .then(None)
            .otherwise(pl.col("weight"))
            .cast(pl.Float64)
            .alias("weight")
        )
    if "fat_score" in df.columns:
        exprs.append(
            pl.when(pl.col("fat_score") == 10)
            .then(None)
            .otherwise(pl.col("fat_score"))
            .alias("fat_score")
        )
    if "partial_juv_moult" in df.columns:
        exprs.append(
            pl.when(pl.col("partial_juv_moult") == 10)
            .then(None)
            .otherwise(pl.col("partial_juv_moult"))
            .alias("partial_juv_moult")
        )
    if exprs:
        df = df.with_columns(exprs)
    return df


# ── main conversion steps ────────────────────────────────────────────────────

def read_native_tables(mdb_path: Path) -> dict[str, pl.DataFrame]:
    """
    Read and rename all six tables from the Access database.

    Returns a dict keyed by the target DuckDB table name.
    """
    conn   = _open_mdb(mdb_path)
    cursor = conn.cursor()

    print("  Reading Ringon (location = 0016NID)…")
    ringon = _read_table(cursor, "Ringon", where="Lokal = '0016NID'")
    ringon = _rename(ringon, _CORE, _BIO, _RINGON_EXTRA)
    ringon = _clean_measurements(ringon)

    print("  Reading Kontr (location = 0016NID)…")
    kontr = _read_table(cursor, "Kontr", where="Lokal = '0016NID'")
    kontr = _rename(kontr, _CORE, _BIO, _KONTR_EXTRA)
    kontr = _clean_measurements(kontr)

    print("  Reading Fynd…")
    fynd = _read_table(cursor, "Fynd")
    fynd = _rename(fynd, _CORE, _FYND_EXTRA)
    fynd = _clean_measurements(fynd)

    print("  Reading FrRing…")
    frring = _read_table(cursor, "FrRing")
    frring = _rename(frring, _CORE, _FRRING_EXTRA)
    frring = _clean_measurements(frring)

    print("  Reading Signaturer…")
    signaturer = _read_table(cursor, "Signaturer")
    signaturer = _rename(signaturer, _SIGNATURER)

    print("  Reading Lokaler…")
    lokaler = _read_table(cursor, "Lokaler")
    lokaler = _rename(lokaler, _LOKALER)

    conn.close()

    return {
        "ringon":     ringon,
        "kontr":      kontr,
        "fynd":       fynd,
        "frring":     frring,
        "signaturer": signaturer,
        "lokaler":    lokaler,
    }


def write_native_tables(duck: duckdb.DuckDBPyConnection, tables: dict[str, pl.DataFrame]) -> None:
    """Write all native tables to DuckDB, dropping any existing versions first."""
    for table_name, df in tables.items():
        print(f"  Writing {table_name} ({len(df):,} rows, {len(df.columns)} columns)…")
        duck.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        duck.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        # Indexes for the two main tables
        if table_name in ("ringon", "kontr"):
            duck.execute(f"CREATE INDEX idx_{table_name}_date    ON {table_name}(date)")
            duck.execute(f"CREATE INDEX idx_{table_name}_species ON {table_name}(species_code)")
            duck.execute(f"CREATE INDEX idx_{table_name}_date_sp ON {table_name}(date, species_code)")
            duck.execute(f"CREATE INDEX idx_{table_name}_ring    ON {table_name}(ring_number)")


def load_artkod_lookup(duck: duckdb.DuckDBPyConnection, metadata_csv: Path) -> None:
    """
    Create the artkod_lookup table: RUBIN code (e.g. 'GÄSMY') → Swedish name.

    The combined_species_metadata.csv column 'sökträff' holds the lowercase
    RUBIN code; 'swedish_name' holds the Swedish common name.  We store the
    RUBIN code in UPPERCASE (as it appears in the MDB Artkod field).
    """
    if not metadata_csv.exists():
        print(f"  WARNING: metadata CSV not found at {metadata_csv}; skipping artkod_lookup")
        return

    meta = pl.read_csv(metadata_csv, infer_schema_length=10000)
    lookup = (
        meta
        .filter(pl.col("sökträff").is_not_null() & (pl.col("sökträff") != ""))
        .select([
            pl.col("sökträff").str.to_uppercase().alias("artkod"),
            pl.col("swedish_name"),
        ])
        .unique(subset=["artkod"])
    )

    duck.execute("DROP TABLE IF EXISTS artkod_lookup")
    duck.execute("CREATE TABLE artkod_lookup AS SELECT * FROM lookup")
    duck.execute("CREATE INDEX idx_artkod ON artkod_lookup(artkod)")
    print(f"  artkod_lookup: {len(lookup):,} species")


def load_species_metadata(duck: duckdb.DuckDBPyConnection, metadata_csv: Path) -> None:
    """Load species_metadata from the combined CSV (taxonomy from Artfakta + eBird)."""
    if not metadata_csv.exists():
        print(f"  WARNING: metadata CSV not found at {metadata_csv}; skipping species_metadata")
        return

    meta = pl.read_csv(metadata_csv, infer_schema_length=10000)
    meta = meta.filter(
        pl.col("swedish_name").is_not_null() & (pl.col("swedish_name") != "")
    )

    keep_cols = [
        "swedish_name", "species_code", "scientific_name", "english_name",
        "taxon_id", "taxon_order", "category", "order_scientific_name",
        "family_english_name", "family_scientific_name", "family_code",
        "auktor", "taxonkategori", "extinct", "extinct_year",
        "com_name_codes", "sci_name_codes", "banding_codes", "report_as",
    ]
    meta = meta.select([c for c in keep_cols if c in meta.columns])

    duck.execute("DROP TABLE IF EXISTS species_metadata")
    duck.execute("""
        CREATE TABLE species_metadata (
            swedish_name          VARCHAR PRIMARY KEY,
            species_code          VARCHAR,
            scientific_name       VARCHAR,
            english_name          VARCHAR,
            taxon_id              DOUBLE,
            taxon_order           DOUBLE,
            category              VARCHAR,
            order_scientific_name VARCHAR,
            family_english_name   VARCHAR,
            family_scientific_name VARCHAR,
            family_code           VARCHAR,
            auktor                VARCHAR,
            taxonkategori         VARCHAR,
            extinct               BOOLEAN,
            extinct_year          DOUBLE,
            com_name_codes        VARCHAR,
            sci_name_codes        VARCHAR,
            banding_codes         VARCHAR,
            report_as             VARCHAR
        )
    """)
    duck.execute("INSERT INTO species_metadata SELECT * FROM meta")
    duck.execute("CREATE INDEX idx_meta_swedish   ON species_metadata(swedish_name)")
    duck.execute("CREATE INDEX idx_meta_order     ON species_metadata(order_scientific_name)")
    duck.execute("CREATE INDEX idx_meta_family    ON species_metadata(family_scientific_name)")
    print(f"  species_metadata: {len(meta):,} rows")


def build_ring_records(duck: duckdb.DuckDBPyConnection) -> None:
    """
    Build the materialized ring_records compatibility table.

    This table unions ringon and kontr, maps columns to the schema that all
    existing query_utils.py queries expect, enriches with swedish_name /
    scientific_name / taxon_id from species_metadata via artkod_lookup, and
    appends TOTAL aggregate rows so that 'TOTAL' works as a species filter.

    New columns relative to the old schema:
        sex, trap_type, condition, mark_type_1/2, mark_text_1/2, mark_color_1/2,
        bio_code_1–7, bio_value_1–7, foreign_bird, original_centre, original_ring,
        days_since_ring, hours_since_ring, distance_km, bearing_deg,
        find_type, find_date_type, find_date_accuracy, serial_number (kontr only)
    """
    print("  Building ring_records…")

    # ------------------------------------------------------------------
    # Step 1: enriched base (ringon ∪ kontr), no TOTAL rows yet
    # ------------------------------------------------------------------
    # Columns that differ between ringon and kontr are coalesced / NULLed
    # where absent. The query builds a common projection.
    duck.execute("DROP TABLE IF EXISTS ring_records")
    duck.execute("""
        CREATE TABLE ring_records AS
        WITH base AS (
            -- ── Ringon (new ringed birds) ──────────────────────────────
            SELECT
                'C'                         AS record_type,
                ring_number,
                date,
                TRY_CAST(hour AS DOUBLE)    AS time,
                age_code,
                species_code,
                ringer_code                 AS ringer,
                age_code                    AS age,
                sex,
                wing_length,
                weight::DOUBLE              AS weight,
                fat_score,
                partial_juv_moult           AS muscle_score,
                NULL::DOUBLE                AS brood_patch,
                NULL::DOUBLE                AS moult_score,
                notes,
                trap_type,
                condition,
                mark_type_1, mark_text_1, mark_color_1,
                mark_type_2, mark_text_2, mark_color_2,
                bio_code_1, bio_value_1::DOUBLE AS bio_value_1,
                bio_code_2, bio_value_2::DOUBLE AS bio_value_2,
                bio_code_3, bio_value_3::DOUBLE AS bio_value_3,
                bio_code_4, bio_value_4,
                bio_code_5, bio_value_5,
                bio_code_6, bio_value_6,
                bio_code_7, bio_value_7,
                foreign_bird,
                original_centre, original_ring,
                NULL::VARCHAR               AS find_type,
                NULL::VARCHAR               AS find_date_type,
                NULL::VARCHAR               AS find_date_accuracy,
                NULL::INTEGER               AS distance_km,
                NULL::SMALLINT              AS bearing_deg,
                NULL::INTEGER               AS days_since_ring,
                NULL::TINYINT               AS hours_since_ring,
                NULL::INTEGER               AS serial_number
            FROM ringon

            UNION ALL

            -- ── Kontr (recaptures already ringed at Nidingen) ─────────
            SELECT
                'R'                         AS record_type,
                ring_number,
                date,
                TRY_CAST(hour AS DOUBLE)    AS time,
                age_code,
                species_code,
                ringer_code                 AS ringer,
                age_code                    AS age,
                sex,
                wing_length,
                weight::DOUBLE              AS weight,
                fat_score,
                partial_juv_moult           AS muscle_score,
                NULL::DOUBLE                AS brood_patch,
                NULL::DOUBLE                AS moult_score,
                notes,
                trap_type,
                condition,
                mark_type_1, mark_text_1, mark_color_1,
                mark_type_2, mark_text_2, mark_color_2,
                bio_code_1, bio_value_1::DOUBLE AS bio_value_1,
                bio_code_2, bio_value_2::DOUBLE AS bio_value_2,
                bio_code_3, bio_value_3::DOUBLE AS bio_value_3,
                bio_code_4, bio_value_4,
                bio_code_5, bio_value_5,
                bio_code_6, bio_value_6,
                bio_code_7, bio_value_7,
                NULL::VARCHAR               AS foreign_bird,
                NULL::VARCHAR               AS original_centre,
                NULL::VARCHAR               AS original_ring,
                find_type,
                find_date_type,
                find_date_accuracy,
                distance_km,
                bearing_deg,
                days_since_ring,
                hours_since_ring,
                serial_number
            FROM kontr
        ),
        enriched AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY b.date, b.ring_number, b.record_type) AS record_id,
                b.date,
                b.time,
                b.record_type,
                b.ring_number,
                b.age_code,
                b.species_code,
                b.ringer,
                b.age,
                b.sex,
                b.wing_length,
                b.weight,
                b.fat_score,
                b.muscle_score,
                b.brood_patch,
                b.moult_score,
                b.notes,
                b.trap_type,
                b.condition,
                b.mark_type_1, b.mark_text_1, b.mark_color_1,
                b.mark_type_2, b.mark_text_2, b.mark_color_2,
                b.bio_code_1, b.bio_value_1,
                b.bio_code_2, b.bio_value_2,
                b.bio_code_3, b.bio_value_3,
                b.bio_code_4,
                b.bio_code_5,
                b.bio_code_6,
                b.bio_code_7,
                b.bio_value_4, b.bio_value_5, b.bio_value_6, b.bio_value_7,
                b.foreign_bird,
                b.original_centre, b.original_ring,
                b.find_type, b.find_date_type, b.find_date_accuracy,
                b.distance_km, b.bearing_deg,
                b.days_since_ring, b.hours_since_ring,
                b.serial_number,
                -- taxonomy enrichment
                al.swedish_name,
                m.scientific_name,
                m.taxon_id,
                'RingDb0016'               AS data_source,
                CURRENT_TIMESTAMP          AS created_at,
                CURRENT_TIMESTAMP          AS updated_at
            FROM base b
            LEFT JOIN artkod_lookup al ON b.species_code = al.artkod
            LEFT JOIN species_metadata m ON al.swedish_name = m.swedish_name
        )
        SELECT * FROM enriched
    """)

    # ------------------------------------------------------------------
    # Step 2: TOTAL rows — copies of each record with species_code='TOTAL'
    #         Preserves all morphometric columns so aggregate queries work
    # ------------------------------------------------------------------
    print("  Appending TOTAL rows…")
    duck.execute("""
        INSERT INTO ring_records
        SELECT
            record_id + (SELECT MAX(record_id) FROM ring_records) AS record_id,
            date, time, record_type, ring_number, age_code,
            'TOTAL'         AS species_code,
            ringer, age, sex,
            wing_length, weight, fat_score, muscle_score, brood_patch, moult_score,
            notes,
            trap_type, condition,
            mark_type_1, mark_text_1, mark_color_1,
            mark_type_2, mark_text_2, mark_color_2,
            bio_code_1, bio_value_1,
            bio_code_2, bio_value_2,
            bio_code_3, bio_value_3,
            bio_code_4,
            bio_code_5,
            bio_code_6,
            bio_code_7,
            bio_value_4, bio_value_5, bio_value_6, bio_value_7,
            foreign_bird,
            original_centre, original_ring,
            find_type, find_date_type, find_date_accuracy,
            distance_km, bearing_deg,
            days_since_ring, hours_since_ring,
            serial_number,
            'Total'         AS swedish_name,
            NULL            AS scientific_name,
            NULL            AS taxon_id,
            data_source, created_at, updated_at
        FROM ring_records
    """)

    # ------------------------------------------------------------------
    # Step 3: indexes
    # ------------------------------------------------------------------
    duck.execute("CREATE INDEX idx_rr_date       ON ring_records(date)")
    duck.execute("CREATE INDEX idx_rr_species    ON ring_records(species_code)")
    duck.execute("CREATE INDEX idx_rr_date_sp    ON ring_records(date, species_code)")
    duck.execute("CREATE INDEX idx_rr_ring       ON ring_records(ring_number)")
    duck.execute("CREATE INDEX idx_rr_swedish    ON ring_records(swedish_name)")

    n = duck.execute("SELECT COUNT(*) FROM ring_records").fetchone()[0]
    print(f"  ring_records: {n:,} rows (including TOTAL duplicates)")


def initialize_weather_schemas(duck: duckdb.DuckDBPyConnection) -> None:
    """Create empty weather_data and weather_data_vinga tables if they don't exist."""
    _WEATHER_DDL = """
        CREATE TABLE IF NOT EXISTS {table} (
            observation_time       TIMESTAMPTZ NOT NULL PRIMARY KEY,
            temperature            DOUBLE,
            wind_direction         DOUBLE,
            wind_speed             DOUBLE,
            humidity               DOUBLE,
            precipitation          DOUBLE,
            pressure               DOUBLE,
            visibility             DOUBLE,
            cloud_cover            DOUBLE,
            gust_wind              DOUBLE,
            temperature_quality    VARCHAR(2),
            wind_direction_quality VARCHAR(2),
            wind_speed_quality     VARCHAR(2),
            humidity_quality       VARCHAR(2),
            precipitation_quality  VARCHAR(2),
            pressure_quality       VARCHAR(2),
            visibility_quality     VARCHAR(2),
            cloud_cover_quality    VARCHAR(2),
            gust_wind_quality      VARCHAR(2),
            station_id             INTEGER,
            station_name           VARCHAR(100),
            data_source            VARCHAR(50),
            fetched_at             TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
    """
    for table in ("weather_data", "weather_data_vinga"):
        duck.execute(_WEATHER_DDL.format(table=table))
        prefix = "weather" if table == "weather_data" else "vinga"
        duck.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{prefix}_time "
            f"ON {table}(observation_time)"
        )
        duck.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{prefix}_date "
            f"ON {table}(CAST(observation_time AS DATE))"
        )
    print("  weather_data + weather_data_vinga schemas ready (populate with fetch_smhi_weather.py)")


# ── entry point ──────────────────────────────────────────────────────────────

def convert(mdb_path: Path, db_path: Path) -> None:
    now = datetime.now().strftime("%H:%M:%S")
    print(f"\n[{now}] Starting conversion")
    print(f"  Source : {mdb_path}")
    print(f"  Target : {db_path}")
    print()

    # Ensure output directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    duck = duckdb.connect(str(db_path))
    duck.execute("SET memory_limit='4GB'")
    duck.execute("SET threads=4")

    # ── 1. native tables ──────────────────────────────────────────────
    print("[1/5] Reading Access tables…")
    tables = read_native_tables(mdb_path)

    print("[2/5] Writing native tables to DuckDB…")
    write_native_tables(duck, tables)

    # ── 2. lookup / metadata ─────────────────────────────────────────
    print("[3/5] Loading species metadata…")
    load_artkod_lookup(duck, _METADATA_CSV)
    load_species_metadata(duck, _METADATA_CSV)

    # ── 3. ring_records compatibility table ───────────────────────────
    print("[4/5] Building ring_records compatibility table…")
    build_ring_records(duck)

    # ── 4. weather schemas (empty, populate later) ───────────────────
    print("[5/5] Initializing weather schemas…")
    initialize_weather_schemas(duck)

    # ── 5. optimize ───────────────────────────────────────────────────
    print("\nOptimizing database…")
    duck.execute("ANALYZE")
    duck.execute("CHECKPOINT")

    duck.close()

    now = datetime.now().strftime("%H:%M:%S")
    size_mb = db_path.stat().st_size / 1_048_576
    print(f"\n[{now}] Done.  Database: {db_path}  ({size_mb:.1f} MB)")
    print("\nNext step: populate weather tables with")
    print("  uv run python app/src/preprocess_data/fetch_smhi_weather.py")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate RingDb0016.mdb to a DuckDB database"
    )
    parser.add_argument(
        "--mdb",
        type=Path,
        default=_DEFAULT_MDB,
        help=f"Path to the Access MDB file (default: {_DEFAULT_MDB})",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=_DEFAULT_DB,
        help=f"Path for the output DuckDB file (default: {_DEFAULT_DB})",
    )
    args = parser.parse_args()

    if not args.mdb.exists():
        print(f"ERROR: MDB file not found: {args.mdb}", file=sys.stderr)
        sys.exit(1)

    convert(args.mdb, args.db)


if __name__ == "__main__":
    main()

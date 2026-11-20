"""
Build the ``agg_*`` tables that the new FastAPI/React dashboard reads.

Run after ``convert_mdb_to_duckdb.py`` and ``fetch_smhi_weather.py``, every
time the database is rebuilt::

    uv run python app/src/build_aggregates.py [--db PATH] [--config-dir PATH]

All rules (seasons, gap years, period presets, the minimum sample for a
median, the single-season threshold) come from ``config/analysis.json``.
Country names for recoveries come from ``config/euring_countries.csv``.
Nothing here hard-codes those rules.

Re-running is safe: every table is recreated inside one transaction.

Tables written (see HANDOFF.md §4.2 and .github/copilot-instructions.md):

- ``agg_species``            one row per species code (+ ``ALLA``): names, totals,
                             season shares, single-season flag
- ``agg_counts_day``         species × date: new ringings and recaptures
- ``agg_counts_year_season`` species × year × season (``var``/``host``/``helar``),
                             dense (zeros included)
- ``agg_counts_week``        species × year × week (1–52), new ringings
- ``agg_phenology_median``   species × year × season: median and quartile day
- ``agg_phenology_species``  species × season × period: mean of yearly medians
- ``agg_trend``              species × season × period: OLS slope with 95 % CI
- ``agg_morph_species``      species × season: weight and wing summaries
- ``agg_morph_hist``         species × season × variable × bin (about 50 bins)
- ``agg_age_sex``            species × season × kind × raw category
- ``agg_recoveries``         one row per recovery (outbound) or foreign bird (inbound)
- ``agg_weather_daily``      one row per local date: SMHI weather, vector-mean
                             wind direction, flags for values filled from Vinga A
- ``euring_countries``       EURING country code → Swedish name
- ``agg_build_info``         one row: build time and source coverage

Conventions
-----------
- ``ALLA`` is the all-species total (replaces the ``TOTAL`` rows of
  ``ring_records``). It exists in the count, species and phenology tables,
  not in the morphometry tables.
- "Ringmärkta" = new ringings (``ringon``). Recaptures (``kontr``) are kept
  apart as ``n_recap``.
- ``doy`` is the day of year on a non-leap calendar (29 Feb counts as 28 Feb),
  so the same date has the same ``doy`` in every year.
- ``week`` = ``(doy - 1) // 7 + 1``, capped at 52 (the last 1–2 days of the
  year join week 52). A week's season is the season of its 4th day.
- ``season`` is ``'var'`` or ``'host'``; ``'helar'`` is the whole year.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_DIR = _SCRIPT_DIR.parent.parent
_DEFAULT_DB = _PROJECT_DIR / "data" / "bird_ringing_0016.duckdb"
_DEFAULT_CONFIG_DIR = _PROJECT_DIR / "config"

ALL_SPECIES = "ALLA"
# Codes that are not birds: "kasserad ring" (discarded ring). Left out of every aggregate.
NON_SPECIES_CODES = ("KASS",)
WHOLE_YEAR = "helar"
N_WEEKS = 52
N_HIST_BINS = 50
# Histogram bins come from data/metadata/histogram_bins.csv, made by
# app/src/preprocess_data/analyze_histogram_bins.py. Per species and variable
# it records the measurement resolution (smallest step the recorded values lie
# on) and a bin width that is a whole number of those steps, giving about
# N_HIST_BINS bins. Smallest unit of each variable: weight 0.1 g, wing 1 mm.
HIST_UNIT = {"weight": 0.1, "wing": 1.0}
# Coarser steps a species may be recorded in, as multiples of HIST_UNIT.
HIST_CANDIDATE_STEPS = {"weight": (1, 5, 10, 20, 50, 100), "wing": (1, 2, 5, 10)}
HIST_GRID_SHARE = 0.97      # a step counts as the resolution when this share of values lies on it
HIST_MIN_N_COARSE = 30      # with fewer birds, never conclude a coarser step than the finest
DEFAULT_HIST_BINS_CSV = _PROJECT_DIR / "data" / "metadata" / "histogram_bins.csv"
HIST_BINS_COLUMNS = [
    "species", "swedish_name", "variable", "n", "min_gap", "resolution", "grid_share",
    "lo", "hi", "n_steps", "step_multiple", "bin_width", "n_bins",
]
HIST_LOW_Q = 0.005
HIST_HIGH_Q = 0.995
# Weight vs wing scatter: at most this many points per species × season are stored.
MAX_SCATTER_POINTS = 5000

# Nidingen bounding box: recoveries inside it are local, not movements.
NIDINGEN_BBOX = {"lat": (57.25, 57.35), "lon": (11.85, 11.95)}

# Parameters that fetch_smhi_weather.py fills from Vinga A where Nidingen has none.
VINGA_FILLED = ("precipitation", "pressure", "visibility")
# The quality-checked SMHI archive reports visibility up to 75 km; SMHI's
# recent, unchecked feed also uses 99 999 m. Cap at the archive maximum.
MAX_VISIBILITY_M = 75000

AGG_TABLES = [
    "agg_species",
    "agg_counts_day",
    "agg_counts_year_season",
    "agg_counts_week",
    "agg_phenology_median",
    "agg_phenology_species",
    "agg_trend",
    "agg_morph_species",
    "agg_morph_hist",
    "agg_morph_points",
    "agg_morph_fit",
    "agg_age_sex",
    "agg_recoveries",
    "agg_weather_daily",
    "euring_countries",
    "agg_build_info",
]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config(config_dir: Path) -> dict[str, Any]:
    """Read analysis.json and validate the parts the builder relies on."""
    with open(config_dir / "analysis.json", encoding="utf-8") as fh:
        config = json.load(fh)
    validate_config(config)
    return config


def validate_config(config: dict[str, Any]) -> None:
    seasons = config["seasons"]
    if set(seasons) != {"var", "host"}:
        raise ValueError(f"Expected seasons 'var' and 'host', got {sorted(seasons)}")
    months = seasons["var"]["months"] + seasons["host"]["months"]
    if sorted(months) != list(range(1, 13)):
        raise ValueError("Season months must cover 1–12 exactly once")
    gap = config["trend_excluded_years"]
    if gap["from"] > gap["to"]:
        raise ValueError("trend_excluded_years.from must be <= to")
    window = config.get("weather_window") or {}
    if not 0 <= window.get("from_hour", -1) < window.get("to_hour", -1) <= 23 or not window.get("timezone"):
        raise ValueError("weather_window needs from_hour < to_hour (0–23) and a timezone")
    for key in ("median_min_birds", "single_season_threshold", "period_options"):
        if key not in config:
            raise ValueError(f"config is missing '{key}'")
    if not 0.5 < config["single_season_threshold"] < 1:
        raise ValueError("single_season_threshold must be between 0.5 and 1")


def load_countries(config_dir: Path) -> list[tuple[str, str]]:
    with open(config_dir / "euring_countries.csv", encoding="utf-8", newline="") as fh:
        return [(row["code"], row["name_sv"]) for row in csv.DictReader(fh)]


def config_digest(config: dict[str, Any], countries: list[tuple[str, str]]) -> str:
    payload = json.dumps([config, countries], sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# SQL fragments generated from config
# ---------------------------------------------------------------------------

def _season_sql(config: dict[str, Any], month_expr: str) -> str:
    var_months = ", ".join(str(m) for m in config["seasons"]["var"]["months"])
    return f"CASE WHEN {month_expr} IN ({var_months}) THEN 'var' ELSE 'host' END"


def _gap_sql(config: dict[str, Any], year_expr: str) -> str:
    gap = config["trend_excluded_years"]
    return f"({year_expr} BETWEEN {int(gap['from'])} AND {int(gap['to'])})"


def _doy_sql(date_expr: str) -> str:
    """Day of year on a non-leap calendar (29 Feb -> 28 Feb)."""
    return (
        f"dayofyear(make_date(2001, month({date_expr}), "
        f"least(day({date_expr}), CASE WHEN month({date_expr}) = 2 THEN 28 ELSE 31 END)))"
    )


def _week_sql(doy_expr: str) -> str:
    return f"least(({doy_expr} - 1) // 7 + 1, {N_WEEKS})"


def week_seasons(config: dict[str, Any]) -> list[tuple[int, str]]:
    """Season of each week 1–52: the season of the week's 4th day."""
    var_months = set(config["seasons"]["var"]["months"])
    jan1 = date(2001, 1, 1)
    out = []
    for week in range(1, N_WEEKS + 1):
        fourth_day = jan1 + timedelta(days=(week - 1) * 7 + 3)
        out.append((week, "var" if fourth_day.month in var_months else "host"))
    return out


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def ols_with_ci(xs: list[float], ys: list[float], level: float = 0.95) -> dict[str, float] | None:
    """Ordinary least squares y = a + b·x with a two-sided CI on the slope.

    Uses the exact t quantile with n − 2 degrees of freedom. Returns None
    when there are fewer than 3 points or no spread in x.
    """
    from scipy.stats import t as t_dist

    n = len(xs)
    if n < 3:
        return None
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    sxx = sum((x - x_mean) ** 2 for x in xs)
    if sxx == 0:
        return None
    sxy = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    slope = sxy / sxx
    intercept = y_mean - slope * x_mean
    sse = sum((y - (intercept + slope * x)) ** 2 for x, y in zip(xs, ys))
    residual_se = math.sqrt(sse / (n - 2))
    slope_se = residual_se / math.sqrt(sxx)
    t_crit = float(t_dist.ppf(0.5 + level / 2, n - 2))
    return {
        "slope": slope,
        "intercept": intercept,
        "slope_se": slope_se,
        "ci_low": slope - t_crit * slope_se,
        "ci_high": slope + t_crit * slope_se,
        "t_crit": t_crit,
        "residual_se": residual_se,
        "x_mean": x_mean,
        "sxx": sxx,
        "mean": y_mean,
        "n": n,
    }


# ---------------------------------------------------------------------------
# Table builders
# ---------------------------------------------------------------------------

def _build_helpers(con: duckdb.DuckDBPyConnection, config: dict[str, Any]) -> None:
    con.execute("CREATE OR REPLACE TEMP TABLE _week_season (week INTEGER, season VARCHAR)")
    con.executemany("INSERT INTO _week_season VALUES (?, ?)", week_seasons(config))

    con.execute(
        "CREATE OR REPLACE TEMP TABLE _periods "
        "(period_id VARCHAR, year_from INTEGER, year_to INTEGER)"
    )
    con.executemany(
        "INSERT INTO _periods VALUES (?, ?, ?)",
        [(p["id"], p["from"], p["to"]) for p in config["period_options"]],
    )

    excluded = ", ".join(f"'{c}'" for c in NON_SPECIES_CODES)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _events AS
        SELECT species_code, date, 'new' AS kind FROM ringon
        WHERE NULLIF(trim(species_code), '') IS NOT NULL AND date IS NOT NULL
          AND species_code NOT IN ({excluded})
        UNION ALL
        SELECT species_code, date, 'recap' AS kind FROM kontr
        WHERE NULLIF(trim(species_code), '') IS NOT NULL AND date IS NOT NULL
          AND species_code NOT IN ({excluded})
    """)


def _build_counts_day(con, config) -> None:
    con.execute(f"""
        CREATE OR REPLACE TABLE agg_counts_day AS
        WITH d AS (
            SELECT
                CASE WHEN grouping(species_code) = 1 THEN '{ALL_SPECIES}' ELSE species_code END AS species,
                date,
                count(*) FILTER (WHERE kind = 'new')   AS n_new,
                count(*) FILTER (WHERE kind = 'recap') AS n_recap
            FROM _events
            GROUP BY GROUPING SETS ((species_code, date), (date))
        )
        SELECT
            species,
            date,
            CAST(year(date) AS INTEGER)       AS year,
            CAST({_doy_sql('date')} AS INTEGER)  AS doy,
            CAST(month(date) AS INTEGER)      AS month,
            CAST({_week_sql(_doy_sql('date'))} AS INTEGER) AS week,
            {_season_sql(config, 'month(date)')} AS season,
            CAST(n_new AS INTEGER)            AS n_new,
            CAST(n_recap AS INTEGER)          AS n_recap,
            {_gap_sql(config, 'year(date)')}  AS is_gap_year
        FROM d
        ORDER BY species, date
    """)


def _build_species(con, config) -> None:
    threshold = float(config["single_season_threshold"])
    con.execute(f"""
        CREATE OR REPLACE TABLE agg_species AS
        WITH t AS (
            SELECT
                species,
                sum(n_new)   AS n_new_total,
                sum(n_recap) AS n_recap_total,
                sum(n_new) FILTER (WHERE season = 'var')  AS n_new_var,
                sum(n_new) FILTER (WHERE season = 'host') AS n_new_host,
                min(date) AS first_date,
                max(date) AS last_date
            FROM agg_counts_day
            GROUP BY species
        ),
        s AS (
            SELECT
                *,
                CASE WHEN n_new_total > 0
                     THEN coalesce(n_new_var, 0) / n_new_total END AS share_var
            FROM t
        )
        SELECT
            s.species,
            CASE WHEN s.species = '{ALL_SPECIES}' THEN 'Alla arter'
                 ELSE al.swedish_name END                  AS swedish_name,
            sm.scientific_name,
            sm.english_name,
            sm.order_scientific_name                      AS order_name,
            sm.family_scientific_name                     AS family_name,
            sm.taxon_order,
            CAST(s.n_new_total AS INTEGER)                AS n_new_total,
            CAST(s.n_recap_total AS INTEGER)              AS n_recap_total,
            CAST(coalesce(s.n_new_var, 0) AS INTEGER)     AS n_new_var,
            CAST(coalesce(s.n_new_host, 0) AS INTEGER)    AS n_new_host,
            s.share_var,
            coalesce(greatest(s.share_var, 1 - s.share_var) > {threshold}, FALSE) AS single_season,
            CASE WHEN greatest(s.share_var, 1 - s.share_var) > {threshold}
                 THEN CASE WHEN s.share_var > 0.5 THEN 'var' ELSE 'host' END
            END                                           AS single_season_season,
            s.first_date,
            s.last_date
        FROM s
        LEFT JOIN artkod_lookup al    ON al.artkod = s.species
        LEFT JOIN species_metadata sm ON lower(sm.swedish_name) = lower(al.swedish_name)
        ORDER BY s.n_new_total DESC, s.species
    """)


def _build_counts_year_season(con, config) -> None:
    con.execute(f"""
        CREATE OR REPLACE TABLE agg_counts_year_season AS
        WITH base AS (
            SELECT
                species,
                year,
                CASE WHEN grouping(season) = 1 THEN '{WHOLE_YEAR}' ELSE season END AS season,
                sum(n_new)   AS n_new,
                sum(n_recap) AS n_recap
            FROM agg_counts_day
            GROUP BY GROUPING SETS ((species, year, season), (species, year))
        ),
        years AS (
            SELECT CAST(y AS INTEGER) AS year
            -- From the first year with data to the later of the last year with
            -- data and the end of the longest period, so trends see zeros.
            FROM range((SELECT min(year) FROM agg_counts_day),
                       greatest((SELECT max(year) FROM agg_counts_day),
                                (SELECT max(year_to) FROM _periods)) + 1) r(y)
        ),
        grid AS (
            SELECT sp.species, y.year, se.season
            FROM agg_species sp
            CROSS JOIN years y
            CROSS JOIN (VALUES ('var'), ('host'), ('{WHOLE_YEAR}')) se(season)
        )
        SELECT
            g.species,
            g.year,
            g.season,
            CAST(coalesce(b.n_new, 0) AS INTEGER)   AS n_new,
            CAST(coalesce(b.n_recap, 0) AS INTEGER) AS n_recap,
            {_gap_sql(config, 'g.year')}            AS is_gap_year
        FROM grid g
        LEFT JOIN base b USING (species, year, season)
        ORDER BY g.species, g.year, g.season
    """)


def _build_counts_week(con, config) -> None:
    con.execute(f"""
        CREATE OR REPLACE TABLE agg_counts_week AS
        SELECT
            d.species,
            d.year,
            d.week,
            ws.season,
            CAST(sum(d.n_new) AS INTEGER)   AS n,
            CAST(sum(d.n_recap) AS INTEGER) AS n_recap,
            {_gap_sql(config, 'd.year')}    AS is_gap_year
        FROM agg_counts_day d
        JOIN _week_season ws USING (week)
        GROUP BY d.species, d.year, d.week, ws.season
        HAVING sum(d.n_new) > 0 OR sum(d.n_recap) > 0
        ORDER BY d.species, d.year, d.week
    """)


def _build_phenology(con, config) -> None:
    min_birds = int(config["median_min_birds"])

    # Median (and quartile) day: the first day on which the cumulative count
    # of new ringings reaches the given fraction of that season's total.
    con.execute(f"""
        CREATE OR REPLACE TABLE agg_phenology_median AS
        WITH c AS (
            SELECT
                species, year, season, doy,
                sum(n_new) OVER w_cum AS cum,
                sum(n_new) OVER w_all AS tot
            FROM agg_counts_day
            WHERE n_new > 0
            WINDOW
                w_cum AS (PARTITION BY species, year, season ORDER BY doy
                          RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                w_all AS (PARTITION BY species, year, season)
        )
        SELECT
            species,
            CAST(year AS INTEGER)                                   AS year,
            season,
            CAST(any_value(tot) AS INTEGER)                         AS n,
            CAST(min(doy) FILTER (WHERE cum >= 0.25 * tot) AS INTEGER) AS q25_doy,
            CAST(min(doy) FILTER (WHERE cum >= 0.50 * tot) AS INTEGER) AS median_doy,
            CAST(min(doy) FILTER (WHERE cum >= 0.75 * tot) AS INTEGER) AS q75_doy,
            any_value(tot) >= {min_birds}                           AS meets_min_sample,
            {_gap_sql(config, 'year')}                              AS is_gap_year
        FROM c
        GROUP BY species, year, season
        ORDER BY species, year, season
    """)

    # Species summary = mean of the yearly medians within the period,
    # excluding gap years and years below the minimum sample. Single-season
    # species only get their majority season.
    con.execute("""
        CREATE OR REPLACE TABLE agg_phenology_species AS
        WITH usable AS (
            SELECT m.*, p.period_id,
                   (NOT m.is_gap_year AND m.meets_min_sample) AS used
            FROM agg_phenology_median m
            JOIN _periods p ON m.year BETWEEN p.year_from AND p.year_to
        )
        SELECT
            u.species,
            u.season,
            u.period_id,
            avg(u.median_doy) FILTER (WHERE u.used)            AS mean_median_doy,
            avg(u.q25_doy)    FILTER (WHERE u.used)            AS mean_q25_doy,
            avg(u.q75_doy)    FILTER (WHERE u.used)            AS mean_q75_doy,
            CAST(count(*) FILTER (WHERE u.used) AS INTEGER)    AS years_used,
            CAST(sum(u.n) FILTER (WHERE u.used) AS INTEGER)    AS n_birds_used,
            s.single_season
        FROM usable u
        JOIN agg_species s USING (species)
        WHERE NOT s.single_season OR u.season = s.single_season_season
        GROUP BY u.species, u.season, u.period_id, s.single_season
        ORDER BY u.species, u.period_id, u.season
    """)


def _build_trend(con, config) -> None:
    rows = con.execute("""
        SELECT c.species, c.season, p.period_id, c.year, c.n_new
        FROM agg_counts_year_season c
        JOIN _periods p ON c.year BETWEEN p.year_from AND p.year_to
        WHERE NOT c.is_gap_year
        ORDER BY c.species, c.season, p.period_id, c.year
    """).fetchall()

    groups: dict[tuple[str, str, str], tuple[list[float], list[float]]] = {}
    for species, season, period_id, year, n in rows:
        xs, ys = groups.setdefault((species, season, period_id), ([], []))
        xs.append(float(year))
        ys.append(float(n))

    out = []
    for (species, season, period_id), (xs, ys) in groups.items():
        total = sum(ys)
        if total == 0:
            continue
        fit = ols_with_ci(xs, ys)
        if fit is None:
            continue
        out.append((
            species, season, period_id,
            fit["slope"], fit["ci_low"], fit["ci_high"], fit["n"], fit["mean"],
            fit["intercept"], fit["slope_se"], fit["t_crit"], fit["residual_se"],
            fit["x_mean"], fit["sxx"], int(total),
            int(min(xs)), int(max(xs)),
        ))

    con.execute("""
        CREATE OR REPLACE TABLE agg_trend (
            species VARCHAR, season VARCHAR, period_id VARCHAR,
            slope_per_year DOUBLE, ci_low DOUBLE, ci_high DOUBLE,
            n_years INTEGER, mean DOUBLE,
            intercept DOUBLE, slope_se DOUBLE, t_crit DOUBLE, residual_se DOUBLE,
            x_mean DOUBLE, sxx DOUBLE, total INTEGER,
            first_year INTEGER, last_year INTEGER
        )
    """)
    if out:
        con.executemany(
            "INSERT INTO agg_trend VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            out,
        )


# ---------------------------------------------------------------------------
# Histogram plan: resolution and bin width per species × variable
# ---------------------------------------------------------------------------

def _quantile_disc(sorted_counts: list[tuple[int, int]], total: int, q: float) -> int:
    """DuckDB's quantile_disc on (value, count) pairs sorted by value."""
    need = max(1, math.ceil(q * total - 1e-9))
    cum = 0
    for v, n in sorted_counts:
        cum += n
        if cum >= need:
            return v
    return sorted_counts[-1][0]


def choose_step_multiple(n_steps: int, target: int = N_HIST_BINS) -> int:
    """Whole number of measurement steps per bin that gives a bin count closest to ``target``.

    Ties go to the finer bin. A range of fewer steps than the target keeps one step per bin.
    """
    best_k, best_gap = 1, abs(n_steps - target)
    for k in range(2, max(2, n_steps) + 1):
        gap = abs(math.ceil(n_steps / k) - target)
        if gap < best_gap:
            best_k, best_gap = k, gap
    return best_k


def analyse_values(variable: str, counts: dict[int, int]) -> dict[str, Any] | None:
    """Resolution and bin width for one species × variable.

    ``counts`` maps a value, in units of ``HIST_UNIT[variable]``, to the number of birds.
    The resolution is the coarsest candidate step that at least ``HIST_GRID_SHARE`` of the
    birds lie on (a species weighed in whole grams, a wing measured to 2 mm, …). The range
    is the 0.5–99.5 % quantiles, the same range the histogram shows.
    """
    total = sum(counts.values())
    if total == 0:
        return None
    pairs = sorted(counts.items())
    values = [v for v, _ in pairs]
    min_gap = min((b - a for a, b in zip(values, values[1:])), default=0)

    steps = HIST_CANDIDATE_STEPS[variable]
    res, share = steps[0], 1.0
    for st in steps:
        on_grid = sum(n for v, n in pairs if v % st == 0) / total
        if st == steps[0]:
            share = on_grid
        elif on_grid >= HIST_GRID_SHARE and total >= HIST_MIN_N_COARSE:
            res, share = st, on_grid
    lo = _quantile_disc(pairs, total, HIST_LOW_Q)
    hi = _quantile_disc(pairs, total, HIST_HIGH_Q)
    n_steps = int(round((hi - lo) / res)) + 1
    k = choose_step_multiple(n_steps)
    unit = HIST_UNIT[variable]
    nd = 1 if variable == "weight" else 0
    return {
        "variable": variable, "n": total,
        "min_gap": round(min_gap * unit, nd), "resolution": round(res * unit, nd),
        "grid_share": round(share, 4),
        "lo": round(lo * unit, nd), "hi": round(hi * unit, nd),
        "n_steps": n_steps, "step_multiple": k,
        "bin_width": round(k * res * unit, nd), "n_bins": math.ceil(n_steps / k),
    }


def derive_hist_plan(con) -> list[dict[str, Any]]:
    """The plan for every species × variable in ``ringon`` (all years, whole year)."""
    excluded = ", ".join(f"'{c}'" for c in NON_SPECIES_CODES)
    rows = con.execute(f"""
        SELECT species_code, 'weight' AS variable, CAST(round(weight * 10) AS BIGINT) AS v, count(*) AS n
        FROM ringon WHERE weight IS NOT NULL AND date IS NOT NULL AND species_code NOT IN ({excluded})
              AND NULLIF(trim(species_code), '') IS NOT NULL GROUP BY ALL
        UNION ALL
        SELECT species_code, 'wing', CAST(wing_length AS BIGINT), count(*)
        FROM ringon WHERE wing_length IS NOT NULL AND date IS NOT NULL AND species_code NOT IN ({excluded})
              AND NULLIF(trim(species_code), '') IS NOT NULL GROUP BY ALL
    """).fetchall()
    names = dict(con.execute("SELECT artkod, swedish_name FROM artkod_lookup").fetchall()) \
        if _has_table(con, "artkod_lookup") else {}
    grouped: dict[tuple[str, str], dict[int, int]] = {}
    for species, variable, v, n in rows:
        grouped.setdefault((species, variable), {})[v] = n
    plan = []
    for (species, variable), counts in sorted(grouped.items()):
        row = analyse_values(variable, counts)
        if row:
            plan.append({"species": species, "swedish_name": names.get(species) or "", **row})
    return plan


def _has_table(con, name: str) -> bool:
    return con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [name]).fetchone()[0] > 0


def write_hist_plan(path: Path, plan: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=HIST_BINS_COLUMNS, lineterminator="\n")
        w.writeheader()
        w.writerows(plan)


def read_hist_plan(path: Path) -> list[dict[str, Any]]:
    """Rows of the metadata CSV. Only species, variable, resolution, lo and bin_width are used."""
    out = []
    with open(path, encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            out.append({**r, "resolution": float(r["resolution"]), "lo": float(r["lo"]),
                        "bin_width": float(r["bin_width"])})
    return out



def _build_morphometrics(con, config, hist_plan: list[dict[str, Any]] | None = None) -> None:
    season = _season_sql(config, "month(date)")
    excluded = ", ".join(f"'{c}'" for c in NON_SPECIES_CODES)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _birds AS
        SELECT species_code AS species, {season} AS season,
               hash(ring_number, date) AS h,
               round(CAST(weight AS DOUBLE), 1) AS weight, CAST(wing_length AS DOUBLE) AS wing,
               NULLIF(trim(age_code), '') AS age_code, NULLIF(trim(sex), '') AS sex
        FROM ringon
        WHERE NULLIF(trim(species_code), '') IS NOT NULL AND date IS NOT NULL
          AND species_code NOT IN ({excluded})
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE agg_morph_species AS
        WITH m AS (
            SELECT
                species,
                CASE WHEN grouping(season) = 1 THEN '{WHOLE_YEAR}' ELSE season END AS season,
                count(*)                      AS n,
                count(weight)                 AS weight_n,
                avg(weight)                   AS weight_mean,
                quantile_cont(weight, 0.25)   AS weight_q25,
                quantile_cont(weight, 0.50)   AS weight_med,
                quantile_cont(weight, 0.75)   AS weight_q75,
                count(wing)                   AS wing_n,
                avg(wing)                     AS wing_mean,
                quantile_cont(wing, 0.25)     AS wing_q25,
                quantile_cont(wing, 0.50)     AS wing_med,
                quantile_cont(wing, 0.75)     AS wing_q75
            FROM _birds
            GROUP BY GROUPING SETS ((species, season), (species))
        )
        SELECT
            m.species, m.season,
            CAST(m.n AS INTEGER) AS n,
            CAST(m.weight_n AS INTEGER) AS weight_n,
            m.weight_mean, m.weight_q25, m.weight_med, m.weight_q75,
            CAST(m.wing_n AS INTEGER) AS wing_n,
            m.wing_mean, m.wing_q25, m.wing_med, m.wing_q75,
            s.n_new_total AS n_total_since_1980
        FROM m
        JOIN agg_species s USING (species)
        ORDER BY m.species, m.season
    """)

    # The bin plan: derived from the data, then overridden row by row by the metadata
    # CSV (so a hand-edited bin width wins, and a species missing from the CSV still works).
    plan = {(r["species"], r["variable"]): r for r in derive_hist_plan(con)}
    for r in hist_plan or []:
        plan[(r["species"], r["variable"])] = r
    con.execute("CREATE OR REPLACE TEMP TABLE _hist_plan "
                "(species VARCHAR, variable VARCHAR, res DOUBLE, width DOUBLE, anchor DOUBLE)")
    con.executemany("INSERT INTO _hist_plan VALUES (?, ?, ?, ?, ?)",
                    [(sp, var, r["resolution"], r["bin_width"], r["lo"]) for (sp, var), r in plan.items()])

    # Histograms between the 0.5 % and 99.5 % quantiles (actual data values) of each
    # species × season × variable; values outside that range are dropped.
    # The bin width is a whole number of the species' measurement steps (see the
    # bin plan above), so no bin holds more recorded values than its neighbours.
    con.execute(f"""
        CREATE OR REPLACE TABLE agg_morph_hist AS
        WITH long AS (
            SELECT species, season, 'weight' AS variable, weight AS v FROM _birds WHERE weight IS NOT NULL
            UNION ALL
            SELECT species, season, 'wing' AS variable, wing AS v FROM _birds WHERE wing IS NOT NULL
        ),
        long_all AS (
            SELECT species, season, variable, v FROM long
            UNION ALL
            SELECT species, '{WHOLE_YEAR}' AS season, variable, v FROM long
        ),
        q AS (
            SELECT species, season, variable,
                   quantile_disc(v, {HIST_LOW_Q})  AS lo,
                   quantile_disc(v, {HIST_HIGH_Q}) AS hi
            FROM long_all
            GROUP BY species, season, variable
        ),
        edges AS (
            -- One bin grid per species × variable (anchored half a step below the whole-year
            -- low end), so every season uses the same bin edges; a season shows the bins
            -- between its own low and high end.
            SELECT q.species, q.season, q.variable, q.lo, q.hi,
                   p.anchor - p.res / 2 AS start,
                   p.width,
                   CAST(floor((q.lo - (p.anchor - p.res / 2)) / p.width) AS INTEGER) AS first_bin,
                   CAST(floor((q.hi - (p.anchor - p.res / 2)) / p.width) AS INTEGER) AS last_bin
            FROM q JOIN _hist_plan p USING (species, variable)
        ),
        binned AS (
            SELECT l.species, l.season, l.variable,
                   least(greatest(CAST(floor((l.v - e.start) / e.width) AS INTEGER) - e.first_bin, 0),
                         e.last_bin - e.first_bin) AS bin
            FROM long_all l
            JOIN edges e USING (species, season, variable)
            WHERE l.v BETWEEN e.lo AND e.hi
        ),
        counts AS (
            SELECT species, season, variable, bin, count(*) AS n
            FROM binned GROUP BY ALL
        ),
        grid AS (
            SELECT e.*, CAST(i AS INTEGER) AS bin
            FROM edges e, range(512) r(i)
            WHERE i <= e.last_bin - e.first_bin
        )
        SELECT
            g.species, g.season, g.variable, g.bin,
            g.start + g.width * (g.first_bin + g.bin)     AS bin_low,
            g.start + g.width * (g.first_bin + g.bin + 1) AS bin_high,
            CAST(coalesce(c.n, 0) AS INTEGER) AS n
        FROM grid g
        LEFT JOIN counts c USING (species, season, variable, bin)
        ORDER BY g.species, g.season, g.variable, g.bin
    """)

    # Weight vs wing scatter. Points: a deterministic sample (ordered by a hash of
    # the ring and date) of at most MAX_SCATTER_POINTS per species × season.
    # Fit: OLS of weight on wing over *all* birds with both values, not the sample.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _pairs AS
        SELECT species, season, h, wing, weight FROM _birds
        WHERE weight IS NOT NULL AND wing IS NOT NULL
        UNION ALL
        SELECT species, '{WHOLE_YEAR}' AS season, h, wing, weight FROM _birds
        WHERE weight IS NOT NULL AND wing IS NOT NULL
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE agg_morph_points AS
        SELECT species, season,
               CAST(row_number() OVER (PARTITION BY species, season ORDER BY h, wing, weight) AS INTEGER) AS rk,
               CAST(wing AS INTEGER) AS wing, weight
        FROM _pairs
        QUALIFY rk <= {MAX_SCATTER_POINTS}
        ORDER BY species, season, rk
    """)
    con.execute("""
        CREATE OR REPLACE TABLE agg_morph_fit (
            species VARCHAR, season VARCHAR, n INTEGER,
            slope DOUBLE, intercept DOUBLE, x_mean DOUBLE, sxx DOUBLE,
            residual_se DOUBLE, t_crit DOUBLE)
    """)
    rows = []
    cur_key, xs, ys = None, [], []

    def flush():
        fit = ols_with_ci(xs, ys) if cur_key else None
        if fit:
            rows.append((cur_key[0], cur_key[1], fit["n"], fit["slope"], fit["intercept"],
                         fit["x_mean"], fit["sxx"], fit["residual_se"], fit["t_crit"]))

    for species, season, wing, weight in con.execute(
        "SELECT species, season, wing, weight FROM _pairs ORDER BY species, season"
    ).fetchall():
        if (species, season) != cur_key:
            flush()
            cur_key, xs, ys = (species, season), [], []
        xs.append(wing)
        ys.append(weight)
    flush()
    con.executemany("INSERT INTO agg_morph_fit VALUES (?,?,?,?,?,?,?,?,?)", rows)

    # Raw age codes and sex codes, never grouped. NULL = missing ('' or NULL).
    con.execute(f"""
        CREATE OR REPLACE TABLE agg_age_sex AS
        WITH long AS (
            SELECT species, season, 'age' AS kind, age_code AS category FROM _birds
            UNION ALL
            SELECT species, season, 'sex' AS kind, sex AS category FROM _birds
        )
        SELECT
            species,
            CASE WHEN grouping(season) = 1 THEN '{WHOLE_YEAR}' ELSE season END AS season,
            kind,
            category,
            CAST(count(*) AS INTEGER) AS n
        FROM long
        GROUP BY GROUPING SETS ((species, season, kind, category), (species, kind, category))
        ORDER BY species, season, kind, n DESC
    """)


def _build_recoveries(con, countries: list[tuple[str, str]]) -> None:
    con.execute("CREATE OR REPLACE TABLE euring_countries (code VARCHAR PRIMARY KEY, name_sv VARCHAR)")
    con.executemany("INSERT INTO euring_countries VALUES (?, ?)", countries)

    (lat_lo, lat_hi), (lon_lo, lon_hi) = NIDINGEN_BBOX["lat"], NIDINGEN_BBOX["lon"]

    def outside(lat: str, lon: str) -> str:
        return (f"NOT ({lat} BETWEEN {lat_lo} AND {lat_hi} "
                f"AND {lon} BETWEEN {lon_lo} AND {lon_hi})")

    # Outbound: one row per report of a Nidingen-ringed bird found elsewhere
    # (the point is the find site). Inbound: one row per foreign-ringed bird
    # met at Nidingen, at its first encounter (the point is the ringing site).
    con.execute(f"""
        CREATE OR REPLACE TABLE agg_recoveries AS
        WITH ringing AS (
            -- A handful of rings appear twice in ringon; use the first ringing.
            SELECT ring_number, centre,
                   min(date) AS date, arg_min(species_code, date) AS species_code
            FROM ringon GROUP BY ring_number, centre
        ),
        outbound AS (
            SELECT
                'out'              AS direction,
                r.species_code     AS species,
                f.ring_number,
                f.case_number,
                r.date             AS ring_date,
                f.date             AS find_date,
                f.days_since_ring  AS days,
                f.latitude         AS lat,
                f.longitude        AS lon,
                COALESCE(NULLIF(trim(f.city), ''), NULLIF(trim(f.locality), '')) AS place,
                f.province_code,
                f.distance_km,
                f.bearing_deg,
                NULLIF(trim(f.find_type), '') AS find_type
            FROM fynd f
            JOIN ringing r ON r.ring_number = f.ring_number AND r.centre = f.centre
            WHERE f.latitude IS NOT NULL AND f.longitude IS NOT NULL
              AND {outside('f.latitude', 'f.longitude')}
        ),
        first_encounter AS (
            SELECT * FROM fynd
            WHERE ring_number IN (SELECT ring_number FROM frring)
            QUALIFY row_number() OVER (PARTITION BY ring_number ORDER BY date, case_number) = 1
        ),
        inbound AS (
            SELECT
                'in'               AS direction,
                fr.species_code    AS species,
                fr.ring_number,
                fj.case_number,
                fr.date            AS ring_date,
                fj.date            AS find_date,
                fj.days_since_ring AS days,
                fr.latitude        AS lat,
                fr.longitude       AS lon,
                COALESCE(NULLIF(trim(fr.city), ''), NULLIF(trim(fr.locality), '')) AS place,
                fr.province_code,
                fj.distance_km,
                fj.bearing_deg,
                NULLIF(trim(fj.find_type), '') AS find_type
            FROM frring fr
            JOIN first_encounter fj ON fj.ring_number = fr.ring_number
            WHERE fr.latitude IS NOT NULL AND fr.longitude IS NOT NULL
              AND {outside('fr.latitude', 'fr.longitude')}
        ),
        both_dirs AS (
            SELECT * FROM outbound UNION ALL SELECT * FROM inbound
        )
        SELECT
            b.direction, b.species, b.ring_number, b.case_number,
            b.ring_date, b.find_date,
            CAST(year(b.find_date) AS INTEGER) AS find_year,
            CAST(b.days AS INTEGER)            AS days,
            b.lat, b.lon, b.place, b.province_code,
            left(b.province_code, 2)           AS country_code,
            c.name_sv                          AS country,
            CAST(b.distance_km AS INTEGER)     AS distance_km,
            CAST(b.bearing_deg AS INTEGER)     AS bearing_deg,
            b.find_type
        FROM both_dirs b
        LEFT JOIN euring_countries c ON c.code = left(b.province_code, 2)
        ORDER BY b.direction, b.find_date, b.ring_number
    """)


def _table_exists(con, name: str) -> bool:
    return bool(con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [name]
    ).fetchone()[0])


def _build_weather_daily(con, config) -> None:
    """Daily weather from the hourly Nidingen A table (already gap-filled).

    Only the hours when birds are caught count: ``weather_window`` in
    config (03:00–12:00 Swedish time). Instantaneous values and means use the
    observations from ``from_hour`` to ``to_hour`` inclusive. Precipitation is
    a one-hour sum ending at the timestamp, so it uses the hours ending after
    ``from_hour`` up to ``to_hour`` (04:00–12:00 covers 03–12). Every date
    still gets a row; metrics are NULL when no hour falls in the window.

    ``weather_daily`` cannot be used for this: it covers whole days, groups
    hours by the build machine's time zone, averages wind direction
    arithmetically, and its ``vinga_gap_fill_used`` flag is always FALSE.
    Here a value counts as "from Vinga" when most of that day's window values
    equal Vinga A's and most of that year's do too (single matches on capped
    visibility in years before the fill would otherwise count).
    """
    columns = """
        date DATE, year INTEGER, doy INTEGER, season VARCHAR,
        temp_mean DOUBLE, temp_min DOUBLE, temp_max DOUBLE,
        wind_speed DOUBLE, wind_dir DOUBLE, gust_max DOUBLE,
        humidity DOUBLE, precipitation DOUBLE, pressure DOUBLE,
        visibility_km DOUBLE, cloud_cover DOUBLE, n_obs INTEGER,
        precipitation_from_vinga BOOLEAN, pressure_from_vinga BOOLEAN,
        visibility_from_vinga BOOLEAN, is_gap_year BOOLEAN
    """
    if not (_table_exists(con, "weather_data") and _table_exists(con, "weather_data_vinga")):
        con.execute(f"CREATE OR REPLACE TABLE agg_weather_daily ({columns})")
        print("  note: weather_data / weather_data_vinga missing; agg_weather_daily is empty")
        return

    window = config["weather_window"]
    tz = window["timezone"]
    t_from = f"TIME '{int(window['from_hour']):02d}:00'"
    t_to = f"TIME '{int(window['to_hour']):02d}:00'"
    # Which hours count for each metric (see the docstring).
    in_window = {p: "in_window" for p in VINGA_FILLED}
    in_window["precipitation"] = "in_precip_window"
    same = ",\n".join(
        f"count({p}) FILTER (WHERE {in_window[p]}) AS n_{p}, "
        f"count(*) FILTER (WHERE {in_window[p]} AND {p} = v_{p}) AS same_{p}"
        for p in VINGA_FILLED
    )
    flags = ",\n".join(
        f"(n_{p} > 0 AND same_{p} * 2 > n_{p} "
        f"AND sum(same_{p}) OVER yr * 2 > sum(n_{p}) OVER yr) AS {p}_from_vinga"
        for p in VINGA_FILLED
    )
    con.execute(f"""
        CREATE OR REPLACE TABLE agg_weather_daily AS
        WITH local AS (
            SELECT timezone('{tz}', n.observation_time) AS t, n.*,
                   v.precipitation AS v_precipitation, v.pressure AS v_pressure,
                   v.visibility AS v_visibility
            FROM weather_data n
            LEFT JOIN weather_data_vinga v USING (observation_time)
        ),
        h AS (
            SELECT CAST(t AS DATE) AS date,
                   CAST(t AS TIME) BETWEEN {t_from} AND {t_to} AS in_window,
                   CAST(t AS TIME) > {t_from} AND CAST(t AS TIME) <= {t_to} AS in_precip_window,
                   temperature, wind_speed, wind_direction, gust_wind,
                   humidity, precipitation, pressure, visibility, cloud_cover,
                   v_precipitation, v_pressure, v_visibility
            FROM local
        ),
        d AS (
            SELECT
                date,
                avg(temperature) FILTER (WHERE in_window) AS temp_mean,
                min(temperature) FILTER (WHERE in_window) AS temp_min,
                max(temperature) FILTER (WHERE in_window) AS temp_max,
                avg(wind_speed)  FILTER (WHERE in_window) AS wind_speed,
                -- Speed-weighted vector mean, so 350° and 10° average to 0°, not 180°.
                avg(wind_speed * sin(radians(wind_direction))) FILTER (WHERE in_window) AS u,
                avg(wind_speed * cos(radians(wind_direction))) FILTER (WHERE in_window) AS w,
                max(gust_wind)   FILTER (WHERE in_window) AS gust_max,
                avg(humidity)    FILTER (WHERE in_window) AS humidity,
                sum(precipitation) FILTER (WHERE in_precip_window) AS precipitation,
                avg(pressure)    FILTER (WHERE in_window) AS pressure,
                avg(least(visibility, {MAX_VISIBILITY_M})) FILTER (WHERE in_window) / 1000 AS visibility_km,
                -- Values above 100 are SMHI codes for an obscured sky.
                avg(least(cloud_cover, 100)) FILTER (WHERE in_window) AS cloud_cover,
                count(temperature) FILTER (WHERE in_window) AS n_obs,
                {same}
            FROM h
            GROUP BY date
        )
        SELECT
            date,
            CAST(year(date) AS INTEGER) AS year,
            CAST({_doy_sql('date')} AS INTEGER) AS doy,
            {_season_sql(config, 'month(date)')} AS season,
            temp_mean, temp_min, temp_max, wind_speed,
            CASE WHEN u IS NULL OR (u = 0 AND w = 0) THEN NULL
                 ELSE (degrees(atan2(u, w)) + 360) % 360 END AS wind_dir,
            gust_max, humidity, precipitation, pressure, visibility_km, cloud_cover,
            CAST(n_obs AS INTEGER) AS n_obs,
            {flags},
            {_gap_sql(config, 'year(date)')} AS is_gap_year
        FROM d
        WINDOW yr AS (PARTITION BY year(date))
        ORDER BY date
    """)


def _build_info(con, config, countries) -> None:
    has_weather = bool(con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'weather_daily'"
    ).fetchone()[0])
    weather_max = "(SELECT max(date) FROM weather_daily)" if has_weather else "CAST(NULL AS DATE)"
    con.execute(f"""
        CREATE OR REPLACE TABLE agg_build_info AS
        SELECT
            CAST(timezone('UTC', get_current_timestamp()) AS TIMESTAMP) AS built_at_utc,
            (SELECT min(date) FROM ringon)          AS ringing_min_date,
            (SELECT max(date) FROM ringon)          AS ringing_max_date,
            (SELECT max(date) FROM fynd)            AS recoveries_max_date,
            {weather_max}                           AS weather_max_date,
            CAST(? AS VARCHAR)                      AS config_digest
    """, [config_digest(config, countries)])


def build_all(
    con: duckdb.DuckDBPyConnection,
    config: dict[str, Any],
    countries: list[tuple[str, str]],
    hist_plan: list[dict[str, Any]] | None = None,
) -> None:
    """(Re)create every agg_* table in one transaction.

    ``hist_plan`` = rows of ``data/metadata/histogram_bins.csv`` (see ``read_hist_plan``).
    """
    validate_config(config)
    con.execute("BEGIN TRANSACTION")
    try:
        _build_helpers(con, config)
        _build_counts_day(con, config)
        _build_species(con, config)
        _build_counts_year_season(con, config)
        _build_counts_week(con, config)
        _build_phenology(con, config)
        _build_trend(con, config)
        _build_morphometrics(con, config, hist_plan)
        _build_recoveries(con, countries)
        _build_weather_daily(con, config)
        _build_info(con, config, countries)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def report(con: duckdb.DuckDBPyConnection) -> None:
    for table in AGG_TABLES:
        n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"  {table:<24} {n:>9,}")
    unmapped = con.execute(
        "SELECT count(*) FROM agg_recoveries WHERE country IS NULL"
    ).fetchone()[0]
    if unmapped:
        codes = con.execute(
            "SELECT string_agg(DISTINCT country_code, ', ') FROM agg_recoveries WHERE country IS NULL"
        ).fetchone()[0]
        print(f"  note: {unmapped} recoveries have no country name (codes: {codes}); "
              "add them to config/euring_countries.csv")
    n_single = con.execute(
        f"SELECT count(*) FROM agg_species WHERE single_season AND species <> '{ALL_SPECIES}'"
    ).fetchone()[0]
    print(f"  single-season species: {n_single}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=_DEFAULT_DB,
                        help=f"DuckDB database to update (default: {_DEFAULT_DB})")
    parser.add_argument("--bins-csv", type=Path, default=DEFAULT_HIST_BINS_CSV,
                        help=f"Histogram bin plan from analyze_histogram_bins.py (default: {DEFAULT_HIST_BINS_CSV}); "
                             "species missing from it are derived from the data")
    parser.add_argument("--config-dir", type=Path, default=_DEFAULT_CONFIG_DIR,
                        help=f"Directory with analysis.json and euring_countries.csv (default: {_DEFAULT_CONFIG_DIR})")
    args = parser.parse_args()

    if not args.db.exists():
        sys.exit(f"Database not found: {args.db}")
    config = load_config(args.config_dir)
    countries = load_countries(args.config_dir)

    hist_plan = None
    if args.bins_csv.exists():
        hist_plan = read_hist_plan(args.bins_csv)
        print(f"Histogram bins: {len(hist_plan)} rows from {args.bins_csv}")
    else:
        print(f"Histogram bins: {args.bins_csv} not found, deriving from the data "
              "(run app/src/preprocess_data/analyze_histogram_bins.py to create it)")
    print(f"Building aggregates in {args.db}")
    con = duckdb.connect(str(args.db))
    try:
        build_all(con, config, countries, hist_plan)
        con.execute("CHECKPOINT")
        report(con)
    finally:
        con.close()
    print("Done.")


if __name__ == "__main__":
    main()

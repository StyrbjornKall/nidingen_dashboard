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
- ``agg_morph_hist``         species × season × variable × bin (50 bins)
- ``agg_age_sex``            species × season × kind × raw category
- ``agg_recoveries``         one row per recovery (outbound) or foreign bird (inbound)
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
WHOLE_YEAR = "helar"
N_WEEKS = 52
N_HIST_BINS = 50
# Measurement resolution: weight is recorded to 0.1 g, wing to 1 mm.
HIST_RESOLUTION = {"weight": 0.1, "wing": 1.0}
HIST_LOW_Q = 0.005
HIST_HIGH_Q = 0.995

# Nidingen bounding box: recoveries inside it are local, not movements.
NIDINGEN_BBOX = {"lat": (57.25, 57.35), "lon": (11.85, 11.95)}

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
    "agg_age_sex",
    "agg_recoveries",
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

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _events AS
        SELECT species_code, date, 'new' AS kind FROM ringon
        WHERE NULLIF(trim(species_code), '') IS NOT NULL AND date IS NOT NULL
        UNION ALL
        SELECT species_code, date, 'recap' AS kind FROM kontr
        WHERE NULLIF(trim(species_code), '') IS NOT NULL AND date IS NOT NULL
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


def _build_morphometrics(con, config) -> None:
    season = _season_sql(config, "month(date)")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _birds AS
        SELECT species_code AS species, {season} AS season,
               round(CAST(weight AS DOUBLE), 1) AS weight, CAST(wing_length AS DOUBLE) AS wing,
               NULLIF(trim(age_code), '') AS age_code, NULLIF(trim(sex), '') AS sex
        FROM ringon
        WHERE NULLIF(trim(species_code), '') IS NOT NULL AND date IS NOT NULL
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

    # Histograms between the 0.5 % and 99.5 % quantiles (actual data values) of each
    # species × season × variable; values outside that range are dropped.
    # 50 equal-width bins, unless the range spans fewer than 50 measurement
    # steps (1 mm, 0.1 g): then one bin per possible value, so integer wing
    # lengths don't leave empty bins between them.
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
                   quantile_disc(v, {HIST_HIGH_Q}) AS hi,
                   CASE variable WHEN 'weight' THEN {HIST_RESOLUTION['weight']}
                                 ELSE {HIST_RESOLUTION['wing']} END AS res
            FROM long_all
            GROUP BY species, season, variable
        ),
        bounds AS (
            SELECT *,
                   CAST(round((hi - lo) / res) AS INTEGER) + 1 < {N_HIST_BINS} AS per_value
            FROM q
        ),
        edges AS (
            SELECT species, season, variable, lo, hi,
                   CASE WHEN per_value THEN lo - res / 2 ELSE lo END AS start,
                   CASE WHEN per_value THEN res ELSE (hi - lo) / {N_HIST_BINS} END AS width,
                   CASE WHEN per_value THEN CAST(round((hi - lo) / res) AS INTEGER) + 1
                        ELSE {N_HIST_BINS} END AS n_bins
            FROM bounds
        ),
        binned AS (
            SELECT l.species, l.season, l.variable,
                   least(greatest(CAST(floor((l.v - e.start) / e.width) AS INTEGER), 0), e.n_bins - 1) AS bin
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
            FROM edges e, range({N_HIST_BINS}) r(i)
            WHERE i < e.n_bins
        )
        SELECT
            g.species, g.season, g.variable, g.bin,
            g.start + g.width * g.bin       AS bin_low,
            g.start + g.width * (g.bin + 1) AS bin_high,
            CAST(coalesce(c.n, 0) AS INTEGER) AS n
        FROM grid g
        LEFT JOIN counts c USING (species, season, variable, bin)
        ORDER BY g.species, g.season, g.variable, g.bin
    """)

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
        WITH outbound AS (
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
            JOIN ringon r ON r.ring_number = f.ring_number AND r.centre = f.centre
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
) -> None:
    """(Re)create every agg_* table in one transaction."""
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
        _build_morphometrics(con, config)
        _build_recoveries(con, countries)
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
    parser.add_argument("--config-dir", type=Path, default=_DEFAULT_CONFIG_DIR,
                        help=f"Directory with analysis.json and euring_countries.csv (default: {_DEFAULT_CONFIG_DIR})")
    args = parser.parse_args()

    if not args.db.exists():
        sys.exit(f"Database not found: {args.db}")
    config = load_config(args.config_dir)
    countries = load_countries(args.config_dir)

    print(f"Building aggregates in {args.db}")
    con = duckdb.connect(str(args.db))
    try:
        build_all(con, config, countries)
        con.execute("CHECKPOINT")
        report(con)
    finally:
        con.close()
    print("Done.")


if __name__ == "__main__":
    main()

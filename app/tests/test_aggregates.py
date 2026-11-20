"""
Tests for app/src/build_aggregates.py (HANDOFF.md §4.4).

Two parts:

1. Fixture tests: a tiny in-memory database with hand-made birds, where every
   expected value is known in advance (median rule, gap years, minimum
   sample, single-season rule, OLS with exact t, recoveries, histograms).
2. Reconciliation against the real database: totals match the source tables
   and the gap-year and median rules hold. Skipped when the database or its
   agg_* tables are missing (run build_aggregates.py first).

Run from the repo root:
    uv run pytest app/tests/test_aggregates.py -v
"""

import json
import os
from pathlib import Path

import duckdb
import pytest
from scipy import stats

from app.src import build_aggregates as ba
from app.tests.fixture_db import COUNTRIES, FIXTURE_CONFIG, d, populate

REPO = Path(__file__).resolve().parents[2]
REAL_DB = Path(os.environ.get("DUCKDB_PATH", REPO / "data" / "bird_ringing_0016.duckdb"))


# ---------------------------------------------------------------------------
# Fixture database (see fixture_db.py)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def fx():
    """Build aggregates on the hand-made database. Returns the connection."""
    con = duckdb.connect(":memory:")
    populate(con)
    ba.build_all(con, FIXTURE_CONFIG, COUNTRIES)
    yield con
    con.close()


def one(con, sql, params=None):
    return con.execute(sql, params or []).fetchone()


# ---------------------------------------------------------------------------
# Statistics helpers
# ---------------------------------------------------------------------------

def test_ols_matches_scipy_with_exact_t():
    xs = [1980, 1981, 1983, 1986, 1990, 1995, 2000]
    ys = [12, 15, 11, 20, 22, 21, 30]
    fit = ba.ols_with_ci([float(x) for x in xs], [float(y) for y in ys])
    ref = stats.linregress(xs, ys)
    t = stats.t.ppf(0.975, len(xs) - 2)
    assert fit["slope"] == pytest.approx(ref.slope)
    assert fit["intercept"] == pytest.approx(ref.intercept)
    assert fit["slope_se"] == pytest.approx(ref.stderr)
    assert fit["t_crit"] == pytest.approx(t)
    assert fit["ci_low"] == pytest.approx(ref.slope - t * ref.stderr)
    assert fit["ci_high"] == pytest.approx(ref.slope + t * ref.stderr)


def test_ols_needs_three_points():
    assert ba.ols_with_ci([1.0, 2.0], [1.0, 2.0]) is None
    assert ba.ols_with_ci([1.0, 1.0, 1.0], [1.0, 2.0, 3.0]) is None


def test_week_season_boundary():
    seasons = dict(ba.week_seasons(FIXTURE_CONFIG))
    assert len(seasons) == 52
    assert seasons[1] == "var"
    assert seasons[26] == "var"   # 4th day = 28 June
    assert seasons[27] == "host"  # 4th day = 5 July
    assert seasons[52] == "host"


def test_config_validation_rejects_bad_seasons():
    bad = json.loads(json.dumps(FIXTURE_CONFIG))
    bad["seasons"]["var"]["months"] = [1, 2, 3, 4, 5, 6, 7]
    with pytest.raises(ValueError):
        ba.validate_config(bad)


def test_real_config_is_valid():
    config = ba.load_config(REPO / "config")
    countries = ba.load_countries(REPO / "config")
    assert config["median_min_birds"] == 10
    assert config["single_season_threshold"] == 0.9
    assert len({c for c, _ in countries}) == len(countries)


# ---------------------------------------------------------------------------
# Fixture: counts
# ---------------------------------------------------------------------------

def test_counts_reconcile_with_source(fx):
    mismatches = one(fx, """
        SELECT count(*) FROM (
            SELECT species_code AS species, count(*) AS n FROM ringon WHERE species_code <> 'KASS' GROUP BY 1
        ) src
        FULL JOIN (
            SELECT species, sum(n_new) AS n FROM agg_counts_year_season
            WHERE season = 'helar' AND species <> 'ALLA' AND n_new > 0 GROUP BY 1
        ) agg USING (species)
        WHERE src.n IS DISTINCT FROM agg.n
    """)[0]
    assert mismatches == 0
    assert one(fx, "SELECT sum(n_new), sum(n_recap) FROM agg_counts_day WHERE species = 'ALLA'") == (
        one(fx, "SELECT count(*) FROM ringon WHERE species_code <> 'KASS'")[0], one(fx, "SELECT count(*) FROM kontr")[0])


def test_alla_equals_sum_of_species(fx):
    diff = one(fx, """
        SELECT count(*) FROM (
            SELECT year, season, sum(n_new) s FROM agg_counts_year_season
            WHERE species <> 'ALLA' GROUP BY ALL
        ) a JOIN agg_counts_year_season t USING (year, season)
        WHERE t.species = 'ALLA' AND t.n_new <> a.s
    """)[0]
    assert diff == 0


def test_season_parts_sum_to_whole_year(fx):
    diff = one(fx, """
        SELECT count(*) FROM agg_counts_year_season w
        JOIN (SELECT species, year, sum(n_new) s FROM agg_counts_year_season
              WHERE season IN ('var', 'host') GROUP BY ALL) p USING (species, year)
        WHERE w.season = 'helar' AND w.n_new <> p.s
    """)[0]
    assert diff == 0


def test_year_season_is_dense_with_zeros(fx):
    years = 1995 - 1985 + 1   # first data year to the end of the longest period
    n_species = one(fx, "SELECT count(*) FROM agg_species")[0]
    assert one(fx, "SELECT count(*) FROM agg_counts_year_season")[0] == years * n_species * 3
    assert one(fx, "SELECT n_new FROM agg_counts_year_season WHERE species='AAA' AND year=1988 AND season='var'")[0] == 0


def test_gap_flag(fx):
    flags = dict(fx.execute(
        "SELECT DISTINCT year, is_gap_year FROM agg_counts_year_season").fetchall())
    assert flags[1990] and flags[1991]
    assert not flags[1989] and not flags[1992]


def test_doy_ignores_leap_years(fx):
    rows = dict(fx.execute(
        "SELECT date, doy FROM agg_counts_day WHERE species='BBB' AND year=1988").fetchall())
    assert rows[d("1988-03-01")] == 60      # same as 1 March in a non-leap year
    assert rows[d("1988-02-29")] == 59      # counted as 28 February


def test_recapture_only_species_exists(fx):
    row = one(fx, "SELECT n_new_total, n_recap_total, single_season FROM agg_species WHERE species='ZZZ'")
    assert row == (0, 1, False)


# ---------------------------------------------------------------------------
# Fixture: phenology
# ---------------------------------------------------------------------------

def test_median_rule_known_answer(fx):
    row = one(fx, """SELECT n, q25_doy, median_doy, q75_doy, meets_min_sample
                     FROM agg_phenology_median WHERE species='AAA' AND year=1985 AND season='var'""")
    assert row == (10, 100 + 10, 110, 120, True)


def test_min_sample_flag(fx):
    assert one(fx, """SELECT meets_min_sample FROM agg_phenology_median
                      WHERE species='AAA' AND year=1986""")[0] is False


def test_species_median_is_mean_of_usable_yearly_medians(fx):
    # Usable AAA years in 1985–1995: 1985 (110), 1987 (130), 1993 (120).
    # 1986 is below the minimum sample, 1990 is a gap year.
    row = one(fx, """SELECT mean_median_doy, years_used FROM agg_phenology_species
                     WHERE species='AAA' AND season='var' AND period_id='all'""")
    assert row == (pytest.approx(120.0), 3)
    row = one(fx, """SELECT mean_median_doy, years_used FROM agg_phenology_species
                     WHERE species='AAA' AND season='var' AND period_id='late'""")
    assert row == (pytest.approx(120.0), 1)


def test_single_season_rule(fx):
    sp = dict((r[0], r[1:]) for r in fx.execute(
        "SELECT species, single_season, single_season_season FROM agg_species").fetchall())
    assert sp["AAA"] == (True, "var")      # 100 % spring
    assert sp["CCC"] == (True, "host")     # 95 % autumn
    assert sp["BBB"] == (False, None)      # 85 % autumn: both seasons
    seasons = lambda s: {r[0] for r in fx.execute(
        "SELECT DISTINCT season FROM agg_phenology_species WHERE species = ?", [s]).fetchall()}
    assert seasons("CCC") == {"host"}
    assert seasons("BBB") == {"var", "host"}


# ---------------------------------------------------------------------------
# Fixture: trends
# ---------------------------------------------------------------------------

def test_trend_excludes_gap_years_and_includes_zeros(fx):
    row = one(fx, """SELECT slope_per_year, ci_low, ci_high, n_years, first_year, last_year
                     FROM agg_trend WHERE species='AAA' AND season='var' AND period_id='all'""")
    years = [y for y in range(1985, 1996) if y not in (1990, 1991)]
    counts = {1985: 10, 1986: 9, 1987: 10, 1993: 12}
    ys = [counts.get(y, 0) for y in years]
    ref = stats.linregress(years, ys)
    t = stats.t.ppf(0.975, len(years) - 2)
    assert row[3] == len(years) == 9
    assert (row[4], row[5]) == (1985, 1995)
    assert row[0] == pytest.approx(ref.slope)
    assert row[1] == pytest.approx(ref.slope - t * ref.stderr)
    assert row[2] == pytest.approx(ref.slope + t * ref.stderr)


def test_no_gap_year_in_any_trend(fx):
    # Every trend uses exactly the non-gap years of its period.
    bad = one(fx, """
        SELECT count(*) FROM agg_trend t JOIN _periods p USING (period_id)
        WHERE t.n_years <> (p.year_to - p.year_from + 1)
              - (SELECT count(*) FROM range(p.year_from, p.year_to + 1) r(y) WHERE y BETWEEN 1990 AND 1991)
    """)[0]
    assert bad == 0


# ---------------------------------------------------------------------------
# Fixture: morphometrics
# ---------------------------------------------------------------------------

def test_morph_summary(fx):
    row = one(fx, """SELECT n, weight_n, weight_med, wing_med, n_total_since_1980
                     FROM agg_morph_species WHERE species='DDD' AND season='helar'""")
    assert row[0] == 100 and row[1] == 100 and row[4] == 100
    assert row[2] == pytest.approx(14.95)
    assert one(fx, "SELECT count(*) FROM agg_morph_species WHERE species='ALLA'")[0] == 0


def test_histograms(fx):
    weight = fx.execute("""SELECT bin, bin_low, bin_high, n FROM agg_morph_hist
                           WHERE species='DDD' AND season='helar' AND variable='weight' ORDER BY bin""").fetchall()
    wing = fx.execute("""SELECT bin, bin_low, bin_high, n FROM agg_morph_hist
                         WHERE species='DDD' AND season='helar' AND variable='wing' ORDER BY bin""").fetchall()
    # Weight spans ~100 steps of 0.1 g -> 50 bins, each exactly two steps (0.2 g) wide.
    assert len(weight) == 50
    assert all(r[2] - r[1] == pytest.approx(0.2) for r in weight)
    assert all(a[2] == pytest.approx(b[1]) for a, b in zip(weight, weight[1:]))
    assert sum(r[3] for r in weight) <= 100
    # Wing spans 11 whole millimetres -> one bin per value, centred on it.
    assert len(wing) == 11
    assert [(r[1], r[2]) for r in wing][:2] == [(59.5, 60.5), (60.5, 61.5)]
    assert all(r[3] > 0 for r in wing)
    assert sum(r[3] for r in wing) == 100


def test_age_and_sex_are_raw_codes(fx):
    ages = dict(fx.execute("""SELECT category, n FROM agg_age_sex
                              WHERE species='DDD' AND season='helar' AND kind='age'""").fetchall())
    assert ages == {"10": 34, "2+": 33, None: 33}
    sexes = dict(fx.execute("""SELECT category, n FROM agg_age_sex
                               WHERE species='DDD' AND season='host' AND kind='sex'""").fetchall())
    assert sexes == {"M": 34, "F": 33, None: 33}


# ---------------------------------------------------------------------------
# Fixture: recoveries
# ---------------------------------------------------------------------------

def test_recoveries(fx):
    rows = fx.execute("""SELECT direction, ring_number, find_date, lat, country, place, days
                         FROM agg_recoveries ORDER BY direction, find_date""").fetchall()
    assert rows == [
        # One inbound row per foreign bird, at its first encounter; the point is its ringing site.
        ("in", "N1", d("1989-09-01"), 60.0, "Norge", "Lista", 10),
        # Outbound: Spain kept; the find on Nidingen and the other-scheme ring are excluded.
        ("out", "R000003", d("1986-01-15"), 40.0, "Spanien", "MADRID", 270),
    ]


def test_weather_daily_uses_the_catch_window(fx):
    rows = {r[0]: r[1:] for r in fx.execute("""
        SELECT date, wind_dir, temp_mean, cloud_cover, visibility_km, precipitation,
               precipitation_from_vinga, pressure_from_vinga, n_obs, season, temp_max
        FROM agg_weather_daily ORDER BY date""").fetchall()}
    day1, day2, day3 = d("1994-08-01"), d("1994-08-02"), d("1994-08-03")
    wind_dir, temp, cloud, vis, prec, prec_v, pres_v, n_obs, season, temp_max = rows[day1]
    assert min(wind_dir, 360 - wind_dir) == pytest.approx(0, abs=1e-9)   # not 180, and 16:00 ignored
    assert temp == pytest.approx(16.0) and temp_max == 17.0                 # the 30 °C hour is outside
    assert cloud == pytest.approx(75.0)          # 110 is capped at 100
    assert vis == pytest.approx(25.0)            # metres -> km
    assert prec == pytest.approx(0.6)            # 04:00 + 05:00; not 03:00 (02–03) or 16:00
    assert (prec_v, pres_v, n_obs, season) == (True, False, 3, "host")
    assert rows[day2][7] == 1 and rows[day2][5] is True
    # Day 3: the 02:30 hour (40 °C) is outside; 03:00 is inside for temperature
    # but its precipitation fell 02–03, so only the 12:00 hour is summed.
    assert rows[day3][1] == pytest.approx(12.5) and rows[day3][7] == 2
    assert rows[day3][4] == pytest.approx(2.0) and rows[day3][5] is False
    assert rows[day3][3] == pytest.approx((10000 + 75000) / 2 / 1000)   # 99 999 m capped at 75 km


def test_weather_window_is_required():
    bad = json.loads(json.dumps(FIXTURE_CONFIG))
    bad["weather_window"]["to_hour"] = 2
    with pytest.raises(ValueError):
        ba.validate_config(bad)


def test_recovery_of_a_ring_ringed_twice_is_counted_once():
    # A few rings appear twice in ringon; their recoveries must not be duplicated.
    con = duckdb.connect(":memory:")
    populate(con)
    con.execute("INSERT INTO ringon SELECT species_code, date + 5, ring_number, centre, weight, "
                "wing_length, age_code, sex FROM ringon WHERE ring_number = 'R000003'")
    ba.build_all(con, FIXTURE_CONFIG, COUNTRIES)
    rows = con.execute("SELECT ring_date FROM agg_recoveries WHERE direction = 'out'").fetchall()
    assert rows == [(d("1985-04-20"),)]      # one row, with the first ringing date
    con.close()


def test_build_info(fx):
    row = one(fx, "SELECT ringing_min_date, ringing_max_date, weather_max_date, config_digest FROM agg_build_info")
    assert row[0] == d("1985-04-10") and row[1] == d("1994-08-01")
    assert row[2] is None
    assert len(row[3]) == 16


def test_rebuild_is_idempotent(fx):
    before = fx.execute("SELECT * FROM agg_trend ORDER BY ALL").fetchall()
    counts = {t: one(fx, f"SELECT count(*) FROM {t}")[0] for t in ba.AGG_TABLES}
    ba.build_all(fx, FIXTURE_CONFIG, COUNTRIES)
    assert fx.execute("SELECT * FROM agg_trend ORDER BY ALL").fetchall() == before
    assert {t: one(fx, f"SELECT count(*) FROM {t}")[0] for t in ba.AGG_TABLES} == counts


# ---------------------------------------------------------------------------
# Reconciliation against the real database
# ---------------------------------------------------------------------------

def _real_con():
    if not REAL_DB.exists():
        pytest.skip(f"real database not found at {REAL_DB}")
    con = duckdb.connect(str(REAL_DB), read_only=True)
    has = one(con, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'agg_build_info'")[0]
    if not has:
        con.close()
        pytest.skip("agg_* tables missing; run app/src/build_aggregates.py")
    return con


@pytest.fixture(scope="module")
def real():
    con = _real_con()
    yield con
    con.close()


@pytest.fixture(scope="module")
def real_config():
    return ba.load_config(REPO / "config")


def test_real_totals_match_source(real):
    skip = ", ".join(f"'{c}'" for c in ba.NON_SPECIES_CODES)
    n_ringon, n_kontr = one(real, f"""SELECT (SELECT count(*) FROM ringon WHERE species_code NOT IN ({skip})),
                                             (SELECT count(*) FROM kontr WHERE species_code NOT IN ({skip}))""")
    assert one(real, "SELECT n_new_total, n_recap_total FROM agg_species WHERE species='ALLA'") == (n_ringon, n_kontr)
    assert one(real, """SELECT sum(n_new), sum(n_recap) FROM agg_counts_year_season
                        WHERE species <> 'ALLA' AND season = 'helar'""") == (n_ringon, n_kontr)
    assert one(real, "SELECT sum(n) FROM agg_counts_week WHERE species <> 'ALLA'")[0] == n_ringon


def test_real_per_species_counts_match_ringon(real):
    mismatches = one(real, """
        SELECT count(*) FROM (SELECT species_code AS species, count(*) n FROM ringon
                              WHERE species_code <> 'KASS' GROUP BY 1) src
        FULL JOIN (SELECT species, n_new_total AS n FROM agg_species
                   WHERE species <> 'ALLA' AND n_new_total > 0) agg USING (species)
        WHERE src.n IS DISTINCT FROM agg.n
    """)[0]
    assert mismatches == 0


def test_real_trends_never_use_gap_years(real, real_config):
    gap = real_config["trend_excluded_years"]
    for p in real_config["period_options"]:
        overlap = max(0, min(p["to"], gap["to"]) - max(p["from"], gap["from"]) + 1)
        expected = p["to"] - p["from"] + 1 - overlap
        bad = one(real, "SELECT count(*) FROM agg_trend WHERE period_id = ? AND n_years <> ?",
                  [p["id"], expected])[0]
        assert bad == 0, p["id"]


def test_real_species_median_is_mean_of_yearly_medians(real, real_config):
    gap = real_config["trend_excluded_years"]
    for p in real_config["period_options"]:
        bad = one(real, """
            SELECT count(*) FROM agg_phenology_species s
            LEFT JOIN (
                SELECT species, season, avg(median_doy) m, count(*) n
                FROM agg_phenology_median
                WHERE year BETWEEN ? AND ? AND NOT (year BETWEEN ? AND ?) AND n >= ?
                GROUP BY ALL
            ) r USING (species, season)
            WHERE s.period_id = ?
              AND (coalesce(s.years_used, 0) <> coalesce(r.n, 0)
                   OR abs(coalesce(s.mean_median_doy, -1) - coalesce(r.m, -1)) > 1e-9)
        """, [p["from"], p["to"], gap["from"], gap["to"], real_config["median_min_birds"], p["id"]])[0]
        assert bad == 0, p["id"]


def test_real_single_season_species_have_one_season(real):
    bad = one(real, """
        SELECT count(*) FROM agg_phenology_species p JOIN agg_species s USING (species)
        WHERE s.single_season AND p.season <> s.single_season_season
    """)[0]
    assert bad == 0


def test_real_recoveries(real):
    n_out = one(real, """
        SELECT count(*) FROM fynd f
        WHERE EXISTS (SELECT 1 FROM ringon r WHERE r.ring_number = f.ring_number AND r.centre = f.centre)
          AND NOT (f.latitude BETWEEN 57.25 AND 57.35 AND f.longitude BETWEEN 11.85 AND 11.95)
    """)[0]
    n_in = one(real, """
        SELECT count(*) FROM frring fr
        WHERE EXISTS (SELECT 1 FROM fynd f WHERE f.ring_number = fr.ring_number)
          AND NOT (fr.latitude BETWEEN 57.25 AND 57.35 AND fr.longitude BETWEEN 11.85 AND 11.95)
    """)[0]
    got = dict(real.execute("SELECT direction, count(*) FROM agg_recoveries GROUP BY 1").fetchall())
    assert got == {"out": n_out, "in": n_in}
    assert one(real, "SELECT count(*) - count(DISTINCT case_number) FROM agg_recoveries")[0] == 0
    assert one(real, "SELECT count(*) FROM agg_recoveries WHERE country IS NULL")[0] == 0


def test_non_species_codes_are_left_out(fx):
    assert one(fx, "SELECT count(*) FROM agg_species WHERE species = 'KASS'")[0] == 0
    assert one(fx, "SELECT count(*) FROM agg_counts_day WHERE species = 'KASS'")[0] == 0
    alla = one(fx, "SELECT n_new_total FROM agg_species WHERE species = 'ALLA'")[0]
    real_birds = one(fx, "SELECT count(*) FROM ringon WHERE species_code <> 'KASS'")[0]
    assert alla == real_birds


def test_histogram_bins_are_whole_measurement_steps(fx):
    # 100 weights, 0.1 g apart: bin width must be a multiple of 0.1 g, never 0.132-style.
    widths = fx.execute("""SELECT DISTINCT round((bin_high - bin_low) / 0.1, 6) FROM agg_morph_hist
                           WHERE variable = 'weight'""").fetchall()
    assert all(float(w[0]).is_integer() for w in widths)


def test_scatter_points_and_fit(fx):
    pts = fx.execute("""SELECT wing, weight FROM agg_morph_points
                        WHERE species='DDD' AND season='helar' ORDER BY rk""").fetchall()
    assert len(pts) == 100
    again = fx.execute("""SELECT wing, weight FROM agg_morph_points
                          WHERE species='DDD' AND season='helar' ORDER BY rk""").fetchall()
    assert pts == again
    n, slope, intercept = one(fx, """SELECT n, slope, intercept FROM agg_morph_fit
                                     WHERE species='DDD' AND season='helar'""")
    xs = [60 + i % 11 for i in range(100)]
    ys = [10.0 + i / 10 for i in range(100)]
    xm, ym = sum(xs) / 100, sum(ys) / 100
    sxx = sum((x - xm) ** 2 for x in xs)
    expect = sum((x - xm) * (y - ym) for x, y in zip(xs, ys)) / sxx
    assert n == 100 and slope == pytest.approx(expect) and intercept == pytest.approx(ym - expect * xm)


def test_scatter_points_are_capped(fx, monkeypatch):
    monkeypatch.setattr(ba, "MAX_SCATTER_POINTS", 10)
    ba._build_morphometrics(fx, ba.load_config(REPO / "config"))
    assert one(fx, "SELECT count(*) FROM agg_morph_points WHERE species='DDD' AND season='helar'")[0] == 10
    # the fit still uses every bird
    assert one(fx, "SELECT n FROM agg_morph_fit WHERE species='DDD' AND season='helar'")[0] == 100


# ---------------------------------------------------------------------------
# Histogram bin plan (resolution per species)
# ---------------------------------------------------------------------------

def test_choose_step_multiple_targets_about_fifty_bins():
    assert ba.choose_step_multiple(20) == 1          # narrow range: one step per bin
    assert ba.choose_step_multiple(66) == 1          # 66 vs 33 bins: 66 is closer to 50 by 16 vs 17
    assert ba.choose_step_multiple(100) == 2         # 50 bins
    assert ba.choose_step_multiple(120) == 2         # 60 vs 40 bins: tie -> finer
    assert ba.choose_step_multiple(500) == 10        # 50 bins


def test_analyse_values_finds_the_measurement_resolution():
    # Weighed in whole grams only (units are 0.1 g): values 100, 110, ..., 590 g
    whole = {v * 10: 5 for v in range(100, 160)}
    r = ba.analyse_values("weight", whole)
    assert r["resolution"] == 1.0 and r["min_gap"] == 1.0 and r["grid_share"] == 1.0
    assert r["bin_width"] == r["step_multiple"] * 1.0
    # 0.1 g species: every tenth present
    tenths = {v: 5 for v in range(100, 200)}
    r = ba.analyse_values("weight", tenths)
    assert r["resolution"] == 0.1 and r["min_gap"] == 0.1
    assert r["n_steps"] == r["n_bins"] * r["step_multiple"] or r["n_bins"] == -(-r["n_steps"] // r["step_multiple"])
    # A few off-grid values (< 3 %) do not change the resolution
    mixed = {v * 10: 100 for v in range(20, 80)}
    mixed[123] = 2
    assert ba.analyse_values("weight", mixed)["resolution"] == 1.0
    # Too few birds: stay at the finest step
    assert ba.analyse_values("weight", {100: 5, 200: 5})["resolution"] == 0.1
    # Wing length every 2 mm
    assert ba.analyse_values("wing", {w: 10 for w in range(60, 100, 2)})["resolution"] == 2.0


def test_histogram_plan_csv_roundtrip_and_override(fx, tmp_path):
    plan = ba.derive_hist_plan(fx)
    ddd = next(r for r in plan if r["species"] == "DDD" and r["variable"] == "weight")
    assert ddd["resolution"] == 0.1 and ddd["bin_width"] == 0.2 and ddd["n_bins"] == 50
    assert not any(r["species"] == "KASS" for r in plan)
    path = tmp_path / "bins.csv"
    ba.write_hist_plan(path, plan)
    back = ba.read_hist_plan(path)
    assert [(r["species"], r["variable"], r["bin_width"]) for r in back] == \
           [(r["species"], r["variable"], r["bin_width"]) for r in plan]
    # a hand-edited bin width wins
    for r in back:
        if r["species"] == "DDD" and r["variable"] == "weight":
            r["bin_width"] = 0.5
    ba._build_morphometrics(fx, ba.load_config(REPO / "config"), back)
    widths = fx.execute("""SELECT DISTINCT round(bin_high - bin_low, 6) FROM agg_morph_hist
                           WHERE species='DDD' AND variable='weight'""").fetchall()
    assert widths == [(0.5,)]
    ba._build_morphometrics(fx, ba.load_config(REPO / "config"))   # restore for later tests


def test_seasons_share_bin_edges(fx):
    edges = fx.execute("""SELECT season, min(bin_low) FROM agg_morph_hist
                          WHERE species='DDD' AND variable='weight' GROUP BY season""").fetchall()
    assert len({round(e[1], 6) for e in edges}) == 1   # DDD is all autumn: same grid as helar

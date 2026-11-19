"""
Tests for the FastAPI app (app/api) on the hand-made fixture database.

Expected values are worked out by hand from app/tests/fixture_db.py, so a
failure means the API (or the aggregates beneath it) changed behaviour.

Run from the repo root:
    uv run pytest app/tests/test_api.py -v
"""

import csv
import json
import math
import statistics

import duckdb
import pytest
from fastapi.testclient import TestClient
from scipy import stats

from app.api.main import create_app
from app.api.params import normalized_doy, week_dates
from app.api.store import DataStore, Settings
from app.src import build_aggregates as ba
from app.tests.fixture_db import COUNTRIES, FIXTURE_CONFIG, d, populate


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    root = tmp_path_factory.mktemp("api")
    db_path, config_dir = root / "fixture.duckdb", root / "config"
    config_dir.mkdir()
    (config_dir / "analysis.json").write_text(json.dumps(FIXTURE_CONFIG, ensure_ascii=False), encoding="utf-8")
    with open(config_dir / "euring_countries.csv", "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["code", "name_sv", "note"])
        writer.writerows([(c, n, "") for c, n in COUNTRIES])

    con = duckdb.connect(str(db_path))
    populate(con)
    ba.build_all(con, FIXTURE_CONFIG, COUNTRIES)
    con.close()

    store = DataStore(Settings(db_path=db_path, config_dir=config_dir))
    with TestClient(create_app(store)) as c:
        yield c
    store.close()


def get(client, url, status=200):
    r = client.get(url)
    assert r.status_code == status, r.text
    return r.json()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def test_week_dates():
    assert week_dates(1, 2025) == (d("2025-01-01"), d("2025-01-07"))
    assert week_dates(12, 2025) == (d("2025-03-19"), d("2025-03-25"))
    assert week_dates(52, 2025) == (d("2025-12-24"), d("2025-12-31"))
    assert week_dates(9, 2024) == (d("2024-02-26"), d("2024-03-04"))   # 8 days in a leap year


def test_normalized_doy():
    assert normalized_doy(d("2024-02-29")) == 59
    assert normalized_doy(d("2024-03-01")) == normalized_doy(d("2025-03-01")) == 60
    assert normalized_doy(d("2025-12-31")) == 365


# ---------------------------------------------------------------------------
# Meta and summary
# ---------------------------------------------------------------------------

def test_meta(client):
    m = get(client, "/api/meta")
    codes = [s["code"] for s in m["species"]]
    assert codes[0] == "ALLA"
    assert set(codes) == {"ALLA", "AAA", "BBB", "CCC", "DDD", "ZZZ"}
    names = {s["code"]: s["name"] for s in m["species"]}
    assert names["AAA"] == "Aart" and names["ZZZ"] == "ZZZ"          # unmapped code keeps its code
    assert [p["id"] for p in m["periods"]] == ["all", "late"]
    assert m["gap_years"] == {"year_from": 1990, "year_to": 1991, "reason": "Delvis bemannad."}
    assert (m["first_year"], m["current_year"], m["complete_through"]) == (1985, 1994, 1995)
    assert [s["id"] for s in m["seasons"]] == ["var", "host", "helar"]
    assert m["weather_window"] == {"from_hour": 3, "to_hour": 12,
                                   "timezone": "Europe/Stockholm", "note": "Väder kl. 03–12."}
    single = {s["code"]: (s["single_season"], s["single_season_season"]) for s in m["species"]}
    assert single["AAA"] == (True, "var") and single["BBB"] == (False, None)
    # Catches in the reference years run from 29 Feb/1 Mar 1988 (week 9) to
    # 1 Oct 1989 (week 40); the 1990 gap-year birds (week 25) don't count.
    assert m["active_weeks"] == {"week_from": 9, "week_to": 40}


def test_kpis(client):
    k = get(client, "/api/summary/kpis")
    assert k == {"n_new": 231, "n_recap": 3, "n_recoveries": 5, "n_species": 4,
                 "n_species_named": 4, "first_year": 1985, "last_year": 1994}


def test_timeseries_year(client):
    t = get(client, "/api/summary/timeseries?species=AAA,ALLA&grain=year")
    assert [b["year"] for b in t["buckets"]] == list(range(1985, 1996))
    assert [b["is_gap_year"] for b in t["buckets"]][5:7] == [True, True]
    aaa, alla = t["series"]
    assert aaa["species"] == "AAA"
    assert aaa["new"] == [10, 9, 10, 0, 0, 50, 0, 0, 12, 0, 0]       # gap year 1990 still shown
    assert aaa["recap"][0] == 1 and aaa["recap"][8] == 1
    assert aaa["total_new"] == 91
    assert alla["total_new"] == 231


def test_timeseries_week_and_day(client):
    w = get(client, "/api/summary/timeseries?species=AAA&grain=week")
    assert len(w["buckets"]) == 11 * 52
    i = next(i for i, b in enumerate(w["buckets"]) if b["year"] == 1985 and b["index"] == 15)
    assert w["buckets"][i]["start"] == "1985-04-09"                    # doy 99–105
    assert w["series"][0]["new"][i] == 2                               # 10 April
    assert w["series"][0]["total_new"] == 91

    day = get(client, "/api/summary/timeseries?species=AAA&grain=day&year=1985")
    assert len(day["buckets"]) == 365
    assert day["series"][0]["new"][99] == 2                             # 10 April = day 100
    get(client, "/api/summary/timeseries?species=AAA&grain=day", status=422)
    get(client, "/api/summary/timeseries?species=AAA&period=all&year=1985", status=422)


def test_top_species_selected_first(client):
    t = get(client, "/api/summary/top-species?species=CCC")
    assert t["total"] == 231 and t["n_species"] == 4
    assert [(r["species"], r["rank"], r["selected"]) for r in t["rows"]] == [
        ("CCC", 4, True), ("DDD", 1, False), ("AAA", 2, False), ("BBB", 3, False)]
    assert math.isclose(sum(r["share"] for r in t["rows"]), 1, abs_tol=1e-4)


# ---------------------------------------------------------------------------
# Trends
# ---------------------------------------------------------------------------

def test_trend_fit_and_band(client):
    t = get(client, "/api/trends?species=AAA&season=var&period=all")
    assert t["excluded_years"] == [1990, 1991]
    s = t["species"][0]
    assert len(s["years"]) == 11 and s["years"][5] == {"year": 1990, "n": 50, "is_gap_year": True}

    years = [y for y in range(1985, 1996) if y not in (1990, 1991)]
    ys = [{1985: 10, 1986: 9, 1987: 10, 1993: 12}.get(y, 0) for y in years]
    ref = stats.linregress(years, ys)
    n = len(years)
    t_crit = stats.t.ppf(0.975, n - 2)
    fit = s["fit"]
    assert fit["slope"] == pytest.approx(ref.slope)
    assert fit["ci_low"] == pytest.approx(ref.slope - t_crit * ref.stderr)
    assert fit["n_years"] == 9
    assert fit["verdict"] == "none"

    # Band = CI of the mean line at each year.
    x_mean = sum(years) / n
    sxx = sum((x - x_mean) ** 2 for x in years)
    resid = [y - (ref.intercept + ref.slope * x) for x, y in zip(years, ys)]
    s_e = math.sqrt(sum(r * r for r in resid) / (n - 2))
    point = next(p for p in fit["band"] if p["year"] == 1993)
    expected_fit = ref.intercept + ref.slope * 1993
    half = t_crit * s_e * math.sqrt(1 / n + (1993 - x_mean) ** 2 / sxx)
    assert point["fit"] == pytest.approx(expected_fit, abs=1e-3)
    assert point["hi"] - point["fit"] == pytest.approx(half, abs=2e-3)
    assert len(fit["band"]) == 11


def test_trend_overview(client):
    o = get(client, "/api/trends/overview?season=var&period=all&species=BBB")
    codes = [r["species"] for r in o["rows"]]
    assert codes[0] == "BBB" and "ALLA" not in codes and "DDD" not in codes
    assert set(codes) == {"AAA", "BBB", "CCC"}
    assert get(client, "/api/trends/overview?season=var&period=all&min_total=5")["rows"][0]["species"] == "AAA"


# ---------------------------------------------------------------------------
# Morphometrics
# ---------------------------------------------------------------------------

def test_morph_means(client):
    m = get(client, "/api/morphometrics/means?season=host")
    assert [r["species"] for r in m["rows"]] == ["DDD"]
    assert m["rows"][0]["weight_median"] == pytest.approx(14.95)


def test_morph_detail(client):
    m = get(client, "/api/morphometrics/detail?species=DDD,AAA&season=helar")
    ddd, aaa = m["species"]
    assert len(ddd["weight"]["bins"]) == 50 and len(ddd["wing"]["bins"]) == 11
    assert ddd["wing"]["bins"][0] == {"low": 59.5, "high": 60.5, "n": 10}
    assert [(c["code"], c["n"]) for c in ddd["age"]] == [("10", 34), ("2+", 33), (None, 33)]
    assert aaa["weight"] is None and aaa["n"] == 91
    get(client, "/api/morphometrics/detail?species=ALLA", status=422)


# ---------------------------------------------------------------------------
# Phenology
# ---------------------------------------------------------------------------

def test_calendar_average(client):
    c = get(client, "/api/phenology/calendar?top=5")
    assert c["mode"] == "avg" and len(c["reference_years"]) == 9
    # Order: spring species by spring median, then autumn-only by autumn median.
    assert [r["species"] for r in c["rows"]] == ["AAA", "DDD", "BBB", "CCC"]
    aaa = c["rows"][0]
    assert aaa["values"][17] == pytest.approx(17 / 9, abs=0.01)        # week 18: 5 (1985) + 12 (1993)
    assert aaa["scaled"][17] == 1.0                                    # single season: one peak
    assert aaa["scaled"][18] == pytest.approx(10 / 17, abs=1e-3)       # week 19: 10 (1987)
    assert aaa["total"] == pytest.approx(41 / 9, abs=0.01)             # gap year 1990 left out


def test_calendar_year_and_selection(client):
    c = get(client, "/api/phenology/calendar?year=1994&species=CCC&top=all")
    assert c["mode"] == "year" and c["rows"][0]["species"] == "CCC" and c["rows"][0]["selected"]
    ddd = next(r for r in c["rows"] if r["species"] == "DDD")
    assert ddd["total"] == 100
    get(client, "/api/phenology/calendar?year=1990", status=422)       # gap year not offered


def test_ridges(client):
    r = get(client, "/api/phenology/ridges?species=AAA,BBB&period=all")
    aaa, bbb = r["species"]
    assert aaa["host"] is None and aaa["var"]["median_doy"] == 120.0 and aaa["var"]["years_used"] == 3
    v = aaa["var"]
    assert v["doy_from"] == 1 and len(v["mean"]) == 181                # Jan–Jun, a day each
    assert v["mean"][120 - 1] == pytest.approx(17 / 9, abs=0.001)      # 30 Apr: 5 (1985) + 12 (1993)
    assert v["mean"][121 - 1] == 0 and v["latest"][120 - 1] == 0
    assert len(bbb["host"]["mean"]) == 184 and bbb["host"]["doy_from"] == 182   # Jul–Dec
    assert r["data_through_doy"] == 365
    assert r["season_days"] == {"var": {"doy_from": 1, "doy_to": 181}, "host": {"doy_from": 182, "doy_to": 365}}
    assert bbb["var"] is not None and bbb["host"] is not None
    assert bbb["var"]["median_doy"] is None                           # 3 birds < minimum sample
    assert r["latest_year"] == 1995


def test_season_to_date(client):
    s = get(client, "/api/phenology/season-to-date?species=AAA&year=1993")
    assert s["reference_years"] == [1985, 1986, 1987, 1988, 1989, 1992]
    assert len(s["mean"]) == 365 and s["data_through_doy"] == 365
    totals = [10, 9, 10, 0, 0, 0]
    assert s["summary"]["current_total"] == 12
    assert s["summary"]["mean_annual_total"] == pytest.approx(sum(totals) / 6, abs=0.05)
    assert s["summary"]["sd_annual_total"] == pytest.approx(statistics.stdev(totals), abs=0.05)
    assert s["summary"]["diff"] == pytest.approx(12 - sum(totals) / 6, abs=0.05)
    assert (s["top"]["year"], s["worst"]["year"]) == (1985, 1988)
    assert s["mean"][-1] == pytest.approx(sum(totals) / 6, abs=0.05)
    assert all(lo <= m <= hi for lo, m, hi in zip(s["sd_low"], s["mean"], s["sd_high"]))
    # The current year stops at the last ringing date.
    cur = get(client, "/api/phenology/season-to-date?species=DDD")
    assert cur["year"] == 1994 and cur["data_through_doy"] == normalized_doy(d("1994-08-01"))
    assert cur["current"][cur["data_through_doy"]] is None


def test_phenology_table(client):
    t = get(client, "/api/phenology/table?period=all&species=CCC")
    assert t["latest_year"] == 1995
    assert t["avg_years"] == [1985, 1986, 1987, 1988, 1989, 1992, 1993, 1994]
    assert [r["species"] for r in t["rows"]] == ["CCC", "DDD", "AAA", "BBB"]
    aaa = t["rows"][2]
    assert aaa["avg_n"] == pytest.approx(41 / 8, abs=0.06)
    assert aaa["median_var_doy"] == 120.0 and aaa["median_host_doy"] is None
    assert len(aaa["spark"]) == 11 and t["spark_gap"][5]


# ---------------------------------------------------------------------------
# Recoveries and weather
# ---------------------------------------------------------------------------

def test_recoveries(client):
    r = get(client, "/api/recoveries")
    assert r["summary"] == {"n_out": 1, "n_in": 1, "n_points": 2, "longest_km": 2400, "median_km": 1375.0}
    assert r["countries"] == [{"country": "Norge", "n": 1}, {"country": "Spanien", "n": 1}]
    out = get(client, "/api/recoveries?direction=out")
    assert [p["country"] for p in out["points"]] == ["Spanien"]
    assert out["summary"]["n_in"] == 1                                 # tiles ignore direction
    assert get(client, "/api/recoveries?species=AAA")["summary"]["n_in"] == 0
    assert get(client, "/api/recoveries?period=late")["points"] == []


def test_weather(client):
    w = get(client, "/api/weather?season=host&year=1994&species=DDD")
    assert [x["date"] for x in w["days"]] == ["1994-08-01", "1994-08-02", "1994-08-03"]
    day1 = w["days"][0]
    assert day1["catch"] == 100 and day1["active"] and day1["precipitation_from_vinga"]
    assert w["days"][1]["active"] is False
    north = w["wind_rose"][0]
    assert (north["label"], north["days"], north["mean_catch"]) == ("N", 1, 100.0)
    assert [x["date"] for x in w["best_days"]] == ["1994-08-01"]
    get(client, "/api/weather?season=host&year=1980", status=422)


# ---------------------------------------------------------------------------
# Validation, caching, compression
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("url", [
    "/api/trends?species=FOO",
    "/api/trends?species=A,B,C,D,E,F,G",
    "/api/trends?species=AAA&period=nope",
    "/api/trends?species=AAA&season=sommar",
    "/api/phenology/season-to-date?species=AAA,BBB",
])
def test_bad_requests(client, url):
    get(client, url, status=422)


def test_etag_and_cache(client):
    r = client.get("/api/summary/timeseries?species=AAA&grain=year")
    etag = r.headers["etag"]
    assert r.headers["cache-control"].startswith("public")
    assert client.get("/api/summary/timeseries?species=AAA&grain=year",
                      headers={"If-None-Match": etag}).status_code == 304
    # Parameter order doesn't matter for the cache.
    assert client.get("/api/summary/timeseries?grain=year&species=AAA").content == r.content


def test_gzip(client):
    r = client.get("/api/summary/timeseries?species=AAA&grain=week", headers={"Accept-Encoding": "gzip"})
    assert r.headers.get("content-encoding") == "gzip"


# ---------------------------------------------------------------------------
# Serving the built frontend
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def site(client, tmp_path_factory):
    """The same fixture store, plus a fake frontend build."""
    dist = tmp_path_factory.mktemp("dist")
    (dist / "assets").mkdir()
    (dist / "index.html").write_text("<!doctype html><title>app</title>", encoding="utf-8")
    (dist / "favicon.png").write_bytes(b"png")
    (dist / "assets" / "index-abc123.js").write_text("console.log(1)", encoding="utf-8")
    store = client.app.state.store
    with TestClient(create_app(store, web_dist=dist)) as c:
        yield c


def test_frontend_routes_get_index(site):
    for path in ("/", "/trender", "/fenologi?arter=R%C3%96HAK", "/no/such/page"):
        r = site.get(path)
        assert r.status_code == 200 and "<title>app</title>" in r.text, path
        assert r.headers["cache-control"] == "no-cache"


def test_frontend_files_and_assets(site):
    assert site.get("/favicon.png").content == b"png"
    r = site.get("/assets/index-abc123.js")
    assert r.status_code == 200 and "immutable" in r.headers["cache-control"]
    assert site.get("/assets/missing.js").status_code == 404


def test_frontend_does_not_shadow_the_api(site):
    assert site.get("/api/meta").status_code == 200
    r = site.get("/api/nope")
    assert r.status_code == 404 and r.headers["content-type"].startswith("application/json")


def test_frontend_blocks_path_traversal(site):
    r = site.get("/..%2F..%2Fetc%2Fpasswd")
    assert r.status_code == 200 and "<title>app</title>" in r.text


def test_openapi_file_matches_the_api():
    """web/openapi.json (and the TypeScript types made from it) must follow the API."""
    from pathlib import Path
    committed = Path(__file__).resolve().parents[2] / "web" / "openapi.json"
    if not committed.exists():
        pytest.skip("web/openapi.json not generated")
    assert json.loads(committed.read_text(encoding="utf-8")) == create_app().openapi(), \
        "run: npm --prefix web run gen:api"

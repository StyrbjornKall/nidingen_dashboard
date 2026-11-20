"""
Parity checks between the new API and the current Dash queries, on the real
database (HANDOFF.md §9, phase 3). Skipped when the database or its agg_*
tables are missing.

Where the numbers differ on purpose, the test pins the exact difference:

- "Ringmärkta" counts new ringings only; Dash's time series also counted
  recaptures (ring_records = ringon ∪ kontr). We compare new + recap.
- Outbound recoveries join ring number *and* centre; Dash joined on ring
  number only. Inbound recoveries are one row per bird; Dash had one per report.
- The Dash heatmap uses ISO weeks; the new site uses 7-day periods from
  1 January, so the two are not compared.

Run from the repo root:
    uv run pytest app/tests/test_api_parity.py -v
"""

import pytest
from fastapi.testclient import TestClient

from app.api.main import create_app
from app.api.store import DataStore, get_settings
from app.src.query_utils import BirdRingingQueries

BBOX = "latitude BETWEEN 57.25 AND 57.35 AND longitude BETWEEN 11.85 AND 11.95"


@pytest.fixture(scope="module")
def store():
    settings = get_settings()
    if not settings.db_path.exists():
        pytest.skip(f"real database not found at {settings.db_path}")
    try:
        s = DataStore(settings)
    except RuntimeError as exc:
        pytest.skip(str(exc))
    yield s
    s.close()


@pytest.fixture(scope="module")
def client(store):
    with TestClient(create_app(store)) as c:
        yield c


def q(store, sql, params=None):
    cur = store.cursor()
    try:
        return cur.execute(sql, params or []).fetchall()
    finally:
        cur.close()


def test_kpis_match_dash_summary_cards(client, store):
    # Same SQL as the Dash "summary-stats" callback over the full date range.
    ((ringon, kontr, fynd, species, min_year, max_year),) = q(store, """
        SELECT (SELECT COUNT(*) FROM ringon WHERE species_code <> 'KASS'), (SELECT COUNT(*) FROM kontr),
               (SELECT COUNT(*) FROM fynd), (SELECT COUNT(DISTINCT species_code) FROM ringon WHERE species_code <> 'KASS'),
               (SELECT MIN(EXTRACT(YEAR FROM date)) FROM ringon),
               (SELECT MAX(EXTRACT(YEAR FROM date)) FROM ringon)
    """)
    k = client.get("/api/summary/kpis").json()
    assert (k["n_new"], k["n_recap"], k["n_recoveries"], k["n_species"]) == (ringon, kontr, fynd, species)
    assert (k["first_year"], k["last_year"]) == (min_year, max_year)


@pytest.mark.parametrize("code", ["RÖHAK", "LÖSÅN", "KUFÅG"])
def test_yearly_series_match_dash(client, store, code):
    sql = BirdRingingQueries.get_species_time_series(
        start_date="1980-01-01", end_date="2025-12-31", species_codes=[code], aggregation="yearly")
    dash = {row[0].year: row[3] for row in q(store, sql)}
    t = client.get(f"/api/summary/timeseries?species={code}&grain=year&period=all").json()
    series = t["series"][0]
    ours = {b["year"]: n + r for b, n, r in zip(t["buckets"], series["new"], series["recap"]) if n + r}
    assert ours == dash


def test_top_species_match_dash_bar(store):
    # The Dash bar ranks species by all-time new ringings (ringon); KASS (discarded rings) is not a species here.
    dash = q(store, """
        SELECT species_code, COUNT(*) AS total FROM ringon
        WHERE species_code IS NOT NULL AND species_code <> 'KASS'
            GROUP BY species_code ORDER BY total DESC, species_code LIMIT 100
    """)
    ours = sorted(((c, s["n_new_total"]) for c, s in store.species.items()
                   if c != "ALLA" and s["n_new_total"] > 0), key=lambda x: (-x[1], x[0]))[:100]
    assert ours == dash


def test_recoveries_differ_from_dash_only_as_intended(client, store):
    sql = BirdRingingQueries.get_rediscoveries_map_data(direction="outbound")
    dash_out = q(store, f"SELECT count(*) FROM ({sql})")[0][0]
    sql = BirdRingingQueries.get_rediscoveries_map_data(direction="inbound")
    dash_in_rows, dash_in_rings = q(store, f"SELECT count(*), count(DISTINCT ring_number) FROM ({sql})")[0]

    # Dash joins outbound on ring number only, so it also counts reports whose
    # ring belongs to another scheme (and duplicates them per ring_records row).
    (centre_mismatch,) = q(store, f"""
        SELECT count(*) FROM fynd f
        JOIN (SELECT DISTINCT ring_number FROM ring_records) rr USING (ring_number)
        WHERE NOT ({BBOX})
          AND NOT EXISTS (SELECT 1 FROM ringon r WHERE r.ring_number = f.ring_number AND r.centre = f.centre)
    """)[0]

    everything = client.get("/api/recoveries?period=all").json()
    n_out_all, n_in_all = q(store, """
        SELECT count(*) FILTER (WHERE direction = 'out'), count(*) FILTER (WHERE direction = 'in')
        FROM agg_recoveries
    """)[0]
    assert everything["summary"]["n_out"] <= n_out_all
    assert dash_out - centre_mismatch == n_out_all
    assert dash_in_rings == n_in_all <= dash_in_rows

"""
A tiny hand-made bird-ringing database with known answers, shared by
test_aggregates.py and test_api.py. Every count, date and coordinate is
chosen so that expected results can be worked out by hand; see the
comments next to each group of birds.
"""

from __future__ import annotations

from datetime import date

import duckdb

FIXTURE_CONFIG = {
    "seasons": {
        "var": {"label_sv": "Vår", "months": [1, 2, 3, 4, 5, 6]},
        "host": {"label_sv": "Höst", "months": [7, 8, 9, 10, 11, 12]},
    },
    "trend_excluded_years": {"from": 1990, "to": 1991, "reason_sv": "Delvis bemannad."},
    "median_min_birds": 10,
    "single_season_threshold": 0.9,
    "weather_window": {"from_hour": 3, "to_hour": 12, "timezone": "Europe/Stockholm",
                       "note_sv": "Väder kl. 03–12."},
    "max_compare_species": 6,
    "default_selection": ["AAA"],
    "period_options": [
        {"id": "all", "label_sv": "1985–1995", "from": 1985, "to": 1995},
        {"id": "late", "label_sv": "1992–1995", "from": 1992, "to": 1995},
    ],
}

COUNTRIES = [("SV", "Sverige"), ("ES", "Spanien"), ("NO", "Norge")]

SCHEMA = """
CREATE TABLE ringon (species_code VARCHAR, date DATE, ring_number VARCHAR, centre VARCHAR,
                     weight DOUBLE, wing_length BIGINT, age_code VARCHAR, sex VARCHAR);
CREATE TABLE kontr (species_code VARCHAR, date DATE, ring_number VARCHAR);
CREATE TABLE fynd (ring_number VARCHAR, centre VARCHAR, species_code VARCHAR, date DATE,
                   latitude DOUBLE, longitude DOUBLE, city VARCHAR, locality VARCHAR,
                   province_code VARCHAR, distance_km BIGINT, bearing_deg BIGINT,
                   days_since_ring BIGINT, find_type VARCHAR, case_number VARCHAR);
CREATE TABLE frring (ring_number VARCHAR, species_code VARCHAR, date DATE,
                     latitude DOUBLE, longitude DOUBLE, city VARCHAR, locality VARCHAR,
                     province_code VARCHAR);
CREATE TABLE artkod_lookup (artkod VARCHAR, swedish_name VARCHAR);
CREATE TABLE weather_data (observation_time TIMESTAMPTZ, temperature DOUBLE, wind_speed DOUBLE,
                           wind_direction DOUBLE, gust_wind DOUBLE, humidity DOUBLE,
                           precipitation DOUBLE, pressure DOUBLE, visibility DOUBLE, cloud_cover DOUBLE);
CREATE TABLE weather_data_vinga AS SELECT * FROM weather_data;
CREATE TABLE species_metadata (swedish_name VARCHAR, scientific_name VARCHAR, english_name VARCHAR,
                               order_scientific_name VARCHAR, family_scientific_name VARCHAR,
                               taxon_order DOUBLE);
"""


class Birds:
    """Adds ringon rows with unique ring numbers."""

    def __init__(self, con):
        self.con = con
        self.next_ring = 1

    def ring(self, species, day, n=1, weight=None, wing=None, age="", sex=""):
        rows = []
        for _ in range(n):
            rows.append((species, day, f"R{self.next_ring:06d}", "SVS", weight, wing, age, sex))
            self.next_ring += 1
        self.con.executemany("INSERT INTO ringon VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
        return [r[2] for r in rows]


def d(text: str) -> date:
    return date.fromisoformat(text)



def populate(con: duckdb.DuckDBPyConnection) -> None:
    """Create the source tables and fill them with the fixture birds."""
    con.execute(SCHEMA)
    con.executemany("INSERT INTO artkod_lookup VALUES (?, ?)",
                    [("AAA", "aart"), ("BBB", "bart"), ("CCC", "cart"), ("DDD", "dart")])
    con.execute("INSERT INTO species_metadata VALUES ('aart', 'Avis a', 'A bird', 'Passeriformes', 'Aidae', 1)")
    b = Birds(con)

    # AAA, spring only.
    # 1985: 2 + 3 + 5 birds on doy 100, 110, 120 -> q25 110, median 110, q75 120.
    b.ring("AAA", d("1985-04-10"), 2)
    b.ring("AAA", d("1985-04-20"), 3)
    b.ring("AAA", d("1985-04-30"), 5)
    # 1986: 9 birds -> below the minimum sample of 10.
    b.ring("AAA", d("1986-05-20"), 9)
    # 1987: 10 birds on doy 130.
    b.ring("AAA", d("1987-05-10"), 10)
    # 1990 is a gap year: many birds and an extreme median must not leak in.
    b.ring("AAA", d("1990-06-20"), 50)
    # 1993: 12 birds on doy 120.
    b.ring("AAA", d("1993-04-30"), 12)

    # BBB: 17 autumn + 3 spring = 85 % autumn -> two seasons.
    b.ring("BBB", d("1988-09-01"), 17)
    b.ring("BBB", d("1988-03-01"), 2)    # 1988 is a leap year: 1 March is still doy 60
    b.ring("BBB", d("1988-02-29"), 1)    # leap day, counted as 28 February

    # CCC: 19 autumn + 1 spring = 95 % autumn -> single season (höst).
    b.ring("CCC", d("1989-10-01"), 19)
    b.ring("CCC", d("1989-05-01"), 1)

    # DDD: morphometrics. Weights 10.0 .. 19.9 g, wings 60 .. 70 mm.
    for i in range(100):
        b.ring("DDD", d("1994-08-01"), 1, weight=10.0 + i / 10, wing=60 + i % 11,
               age=["10", "2+", ""][i % 3], sex=["M", "F", None][i % 3])

    # KASS = "kasserad ring": not a bird, must stay out of every aggregate.
    b.ring("KASS", d("1994-07-30"), 3)

    # Recaptures.
    con.executemany("INSERT INTO kontr VALUES (?, ?, ?)", [
        ("AAA", d("1985-04-21"), "R000001"),
        ("AAA", d("1993-05-01"), "R000001"),
        ("ZZZ", d("1994-07-07"), "X1"),      # species seen only as a recapture
    ])

    # Recoveries.
    out_ring = "R000003"
    con.executemany("INSERT INTO fynd VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
        # Outbound, found in Spain.
        (out_ring, "SVS", "AAA", d("1986-01-15"), 40.0, -3.7, "MADRID", "", "ES65", 2400, 220, 270, "1", "1986:1"),
        # Outbound, found back on Nidingen: inside the box, excluded.
        (out_ring, "SVS", "AAA", d("1987-04-01"), 57.30, 11.90, "Nidingen", "", "SVNHA", 0, 0, 720, "8", "1987:1"),
        # Same ring number under another scheme: not a Nidingen bird, excluded.
        ("R000004", "NOS", "AAA", d("1988-01-01"), 59.0, 10.0, "OSLO", "", "NO02", 300, 0, 100, "1", "1988:1"),
        # Inbound: a Norwegian bird met twice at Nidingen -> one row, first encounter.
        ("N1", "NOS", "BBB", d("1990-09-01"), 57.30, 11.90, "Nidingen", "", "SVNHA", 350, 180, 30, "8", "1990:2"),
        ("N1", "NOS", "BBB", d("1989-09-01"), 57.30, 11.90, "Nidingen", "", "SVNHA", 350, 180, 10, "8", "1989:2"),
    ])
    con.execute("INSERT INTO frring VALUES ('N1', 'BBB', DATE '1989-08-22', 60.0, 10.5, '', 'Lista', 'NO42')")

    # Weather (UTC timestamps; Swedish summer time is UTC+2). Only 03:00–12:00
    # local time counts; precipitation only for hours ending 04:00–12:00.
    # Day 1: in-window winds from 0°, 350° and 10° -> vector mean 0°; the
    #   16:00 hour (30 °C, wind from 180°, 5 mm) is outside and ignored, and
    #   the 03:00 hour's 9 mm fell 02–03, so it is not summed.
    # Day 2: one hour at 12:00 local (inside).
    # Day 3: 02:30 local (outside), 03:00 and 12:00 local (inside).
    # Precipitation equals Vinga's on days 1–2 (filled), not on day 3.
    wx = [
        # time,                     temp, ws,  wd,  gust, hum, prec, pres, vis,   cloud | vinga prec
        ("1994-08-01 01:00:00+00", 16.0, 5.0, 0,   7.0, 75, 9.0, None, 25000, 75,  9.0),
        ("1994-08-01 04:00:00+00", 15.0, 5.0, 350, 8.0, 80, 0.4, None, 20000, 110, 0.4),
        ("1994-08-01 05:00:00+00", 17.0, 5.0, 10,  9.0, 70, 0.2, None, 30000, 50,  0.2),
        ("1994-08-01 14:00:00+00", 30.0, 9.0, 180, 12.0, 40, 5.0, None, 50000, 0,  5.0),
        ("1994-08-02 10:00:00+00", 14.0, 3.0, 180, 5.0, 90, 1.0, None, 10000, 100, 1.0),
        ("1994-08-03 00:30:00+00", 40.0, 2.0, 180, 4.0, 95, 0.5, None, 10000, 100, 0.5),
        ("1994-08-03 01:00:00+00", 12.0, 2.0, 90,  4.0, 95, 0.5, None, 10000, 100, 0.5),
        ("1994-08-03 10:00:00+00", 13.0, 4.0, 90,  6.0, 85, 2.0, None, 99999, 80,  7.0),
    ]
    for t, temp, ws, wd, gust, hum, prec, pres, vis, cloud, vprec in wx:
        con.execute("INSERT INTO weather_data VALUES (CAST(? AS TIMESTAMPTZ), ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [t, temp, ws, wd, gust, hum, prec, pres, vis, cloud])
        con.execute("INSERT INTO weather_data_vinga (observation_time, precipitation) "
                    "VALUES (CAST(? AS TIMESTAMPTZ), ?)", [t, vprec])


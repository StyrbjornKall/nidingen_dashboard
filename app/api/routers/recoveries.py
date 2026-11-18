"""Återfynd: recovery points for the globe, with summary, countries and longest finds.

The period filters on the find year. Outbound points are find sites of
Nidingen birds; inbound points are the ringing sites of foreign birds.
"""

from __future__ import annotations

import statistics
from typing import Literal, Optional

import duckdb
from fastapi import APIRouter, Depends, Query

from app.src.build_aggregates import ALL_SPECIES
from ..params import get_cursor, get_store, parse_period, parse_species, rnd
from ..schemas import Recoveries
from ..store import DataStore

router = APIRouter()

POINT_COLUMNS = ("direction, species, ring_date, find_date, days, lat, lon, "
                 "place, country, distance_km, find_type")


@router.get("/recoveries", response_model=Recoveries)
def recoveries(
    species: Optional[str] = Query(None, description="One species code; omit or ALLA for all"),
    period: Optional[str] = None,
    direction: Literal["both", "out", "in"] = "both",
    store: DataStore = Depends(get_store),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    codes = parse_species(store, species, max_n=1, required=False)
    code = codes[0] if codes and codes[0] != ALL_SPECIES else None
    p = parse_period(store, period)

    res = cur.execute(f"""
        SELECT {POINT_COLUMNS} FROM agg_recoveries
        WHERE find_year BETWEEN ? AND ? AND (? IS NULL OR species = ?)
        ORDER BY find_date, ring_number
    """, [p.year_from, p.year_to, code, code])
    cols = [c[0] for c in res.description]
    all_points = [dict(zip(cols, r)) for r in res.fetchall()]
    points = [pt for pt in all_points if direction == "both" or pt["direction"] == direction]

    km = sorted(pt["distance_km"] for pt in points if pt["distance_km"] is not None)
    countries: dict[str, int] = {}
    for pt in points:
        key = pt["country"] or "Okänt land"
        countries[key] = countries.get(key, 0) + 1

    return {
        "period": p.__dict__,
        "direction": direction,
        "species": code,
        "points": points,
        "summary": {
            "n_out": sum(1 for pt in all_points if pt["direction"] == "out"),
            "n_in": sum(1 for pt in all_points if pt["direction"] == "in"),
            "n_points": len(points),
            "longest_km": km[-1] if km else None,
            "median_km": rnd(statistics.median(km), 1) if km else None,
        },
        "countries": [{"country": c, "n": n}
                      for c, n in sorted(countries.items(), key=lambda kv: (-kv[1], kv[0]))],
        "longest": sorted(points, key=lambda pt: -(pt["distance_km"] or 0))[:5],
    }

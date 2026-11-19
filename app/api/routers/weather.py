"""Väder: daily weather and catch for one season of one year, a wind rose and the best days.

Seasons are the site-wide ones (var = januari–juni, host = juli–december).
Daily weather covers only the catch window from config (03:00–12:00 Swedish
time), as built into agg_weather_daily; /api/meta carries the note to show.
The wind rose averages the catch over days when the station was active
(any ringing or recapture that day), so unstaffed days don't pull it down.
"""

from __future__ import annotations

from typing import Optional

import duckdb
from fastapi import APIRouter, Depends, Query

from app.src.build_aggregates import ALL_SPECIES
from ..params import SeasonId, bad_request, get_cursor, get_store, parse_species, rnd
from ..schemas import Weather
from ..store import DataStore

router = APIRouter()

SECTORS = ["N", "NO", "Ö", "SO", "S", "SV", "V", "NV"]
ROUNDED = ("temp_mean", "temp_min", "temp_max", "wind_speed", "gust_max", "pressure",
           "precipitation", "visibility_km", "cloud_cover", "humidity")


def sector_of(degrees: float) -> int:
    """8 compass sectors of 45°, centred on N (337.5°–22.5° = 0)."""
    return int(((degrees + 22.5) % 360) // 45)


@router.get("/weather", response_model=Weather)
def weather(
    season: SeasonId = "host",
    year: Optional[int] = Query(None, description="Defaults to the last complete year"),
    species: str = Query(ALL_SPECIES, description="Species whose catch is shown"),
    store: DataStore = Depends(get_store),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    (code,) = parse_species(store, species, max_n=1)
    year = store.complete_through if year is None else year
    if store.weather_years is None:
        raise bad_request("no weather data in the database")
    first, last = store.weather_years
    if not first <= year <= last:
        raise bad_request(f"year must be between {first} and {last}")

    res = cur.execute(f"""
        SELECT w.date, w.doy,
               coalesce(c.n_new, 0) AS catch,
               coalesce(a.n_new + a.n_recap, 0) > 0 AS active,
               w.temp_mean, w.temp_min, w.temp_max, w.wind_speed, w.wind_dir, w.gust_max,
               w.pressure, w.precipitation, w.visibility_km, w.cloud_cover, w.humidity,
               w.precipitation_from_vinga, w.pressure_from_vinga, w.visibility_from_vinga
        FROM agg_weather_daily w
        LEFT JOIN agg_counts_day c ON c.date = w.date AND c.species = ?
        LEFT JOIN agg_counts_day a ON a.date = w.date AND a.species = '{ALL_SPECIES}'
        WHERE w.year = ? AND w.season = ?
        ORDER BY w.date
    """, [code, year, season])
    cols = [c[0] for c in res.description]
    days = []
    for r in res.fetchall():
        day = dict(zip(cols, r))
        for k in ROUNDED:
            day[k] = rnd(day[k], 1)
        day["wind_dir"] = rnd(day["wind_dir"], 0)
        days.append(day)

    sectors = [{"sector": i, "label": label, "days": 0, "total": 0} for i, label in enumerate(SECTORS)]
    for day in days:
        if day["active"] and day["wind_dir"] is not None:
            s = sectors[sector_of(day["wind_dir"])]
            s["days"] += 1
            s["total"] += day["catch"]
    wind_rose = [{"sector": s["sector"], "label": s["label"], "days": s["days"],
                  "mean_catch": rnd(s["total"] / s["days"], 1) if s["days"] else None}
                 for s in sectors]

    best = sorted((d for d in days if d["catch"] > 0), key=lambda d: (-d["catch"], d["date"]))[:6]
    return {"year": year, "season": season, "species": code, "is_gap_year": store.is_gap(year),
            "days": days, "wind_rose": wind_rose, "best_days": best}

"""Sammanfattning: key figures, counts over time, most ringed species."""

from __future__ import annotations

import calendar
from datetime import date, timedelta
from typing import Literal, Optional

import duckdb
from fastapi import APIRouter, Depends, Query

from app.src.build_aggregates import ALL_SPECIES, N_WEEKS, WHOLE_YEAR
from ..params import bad_request, get_cursor, get_store, parse_species, period_or_year, week_dates
from ..schemas import Kpis, TimeSeries, TopSpecies
from ..store import DataStore

router = APIRouter(prefix="/summary")


@router.get("/kpis", response_model=Kpis)
def kpis(store: DataStore = Depends(get_store), cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    alla = store.species[ALL_SPECIES]
    n_species, n_named = cur.execute(f"""
        SELECT count(*), count(swedish_name) FROM agg_species
        WHERE species <> '{ALL_SPECIES}' AND n_new_total > 0
    """).fetchone()
    return {
        "n_new": alla["n_new_total"],
        "n_recap": alla["n_recap_total"],
        "n_recoveries": cur.execute("SELECT count(*) FROM fynd").fetchone()[0],
        "n_species": n_species,
        "n_species_named": n_named,
        "first_year": store.first_year,
        "last_year": store.current_year,
    }


def _buckets(grain: str, year_from: int, year_to: int, store: DataStore) -> list[dict]:
    out = []
    for year in range(year_from, year_to + 1):
        gap = store.is_gap(year)
        if grain == "year":
            out.append({"start": date(year, 1, 1), "end": date(year, 12, 31),
                        "year": year, "index": year, "is_gap_year": gap})
        elif grain == "month":
            for m in range(1, 13):
                last = calendar.monthrange(year, m)[1]
                out.append({"start": date(year, m, 1), "end": date(year, m, last),
                            "year": year, "index": m, "is_gap_year": gap})
        elif grain == "week":
            for w in range(1, N_WEEKS + 1):
                start, end = week_dates(w, year)
                out.append({"start": start, "end": end, "year": year, "index": w, "is_gap_year": gap})
        else:  # day
            d = date(year, 1, 1)
            while d.year == year:
                out.append({"start": d, "end": d, "year": year,
                            "index": d.timetuple().tm_yday, "is_gap_year": gap})
                d += timedelta(days=1)
    return out


@router.get("/timeseries", response_model=TimeSeries)
def timeseries(
    species: str = Query(..., description="Comma-separated codes, e.g. RÖHAK,LÖSÅN (ALLA = all)"),
    grain: Literal["day", "week", "month", "year"] = "year",
    period: Optional[str] = Query(None, description="Period id from /api/meta"),
    year: Optional[int] = Query(None, description="A single year instead of a period"),
    store: DataStore = Depends(get_store),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    codes = parse_species(store, species)
    p = period_or_year(store, period, year)
    if grain == "day" and p.year_from != p.year_to:
        raise bad_request("grain=day needs a single year")

    if grain == "year":
        sql = f"""SELECT species, year, year AS idx, n_new, n_recap FROM agg_counts_year_season
                  WHERE season = '{WHOLE_YEAR}' AND list_contains(?, species) AND year BETWEEN ? AND ?"""
    else:
        idx = {"month": "month", "week": "week", "day": "dayofyear(date)"}[grain]
        sql = f"""SELECT species, year, {idx} AS idx, sum(n_new), sum(n_recap) FROM agg_counts_day
                  WHERE list_contains(?, species) AND year BETWEEN ? AND ? GROUP BY ALL"""
    rows = cur.execute(sql, [codes, p.year_from, p.year_to]).fetchall()

    buckets = _buckets(grain, p.year_from, p.year_to, store)
    position = {(b["year"], b["index"]): i for i, b in enumerate(buckets)}
    series = {c: {"species": c, "new": [0] * len(buckets), "recap": [0] * len(buckets)} for c in codes}
    for sp, yr, idx, n_new, n_recap in rows:
        i = position[(yr, idx)]
        series[sp]["new"][i] = int(n_new)
        series[sp]["recap"][i] = int(n_recap)
    for s in series.values():
        s["total_new"] = sum(s["new"])
        s["total_recap"] = sum(s["recap"])
    return {"grain": grain, "period": p.__dict__, "buckets": buckets,
            "series": [series[c] for c in codes]}


@router.get("/top-species", response_model=TopSpecies)
def top_species(
    period: Optional[str] = None,
    year: Optional[int] = None,
    limit: int = Query(100, ge=1, le=500),
    species: Optional[str] = Query(None, description="Selected species, listed first"),
    store: DataStore = Depends(get_store),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    selected = parse_species(store, species, required=False)
    p = period_or_year(store, period, year)
    rows = cur.execute(f"""
        SELECT species, sum(n_new) AS n FROM agg_counts_year_season
        WHERE season = '{WHOLE_YEAR}' AND year BETWEEN ? AND ? AND species <> '{ALL_SPECIES}'
        GROUP BY species HAVING sum(n_new) > 0
        ORDER BY n DESC, species
    """, [p.year_from, p.year_to]).fetchall()
    total = sum(int(n) for _, n in rows)
    ranked = [
        {"rank": i + 1, "species": sp, "name": store.name(sp), "n": int(n),
         "share": round(int(n) / total, 5) if total else 0.0, "selected": sp in selected}
        for i, (sp, n) in enumerate(rows)
    ]
    by_code = {r["species"]: r for r in ranked}
    first = [by_code[c] for c in selected if c in by_code]
    rest = [r for r in ranked if not r["selected"]][:limit]
    return {"period": p.__dict__, "total": total, "n_species": len(ranked), "rows": first + rest}

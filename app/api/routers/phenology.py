"""Fenologi: migration calendar, ridgelines, season-to-date and the species table.

Multi-year averages divide by the number of reference years (gap years
removed), so years without birds count as zeros.
"""

from __future__ import annotations

from typing import Literal, Optional

import duckdb
import numpy as np
from fastapi import APIRouter, Depends, Query

from app.src.build_aggregates import ALL_SPECIES, N_WEEKS, WHOLE_YEAR
from ..params import (
    bad_request, doy_to_date, get_cursor, get_store, normalized_doy, parse_period, parse_species, period_or_year, rnd,
    selected_first, verdict, week_infos,
)
from ..schemas import Calendar, PhenologyTable, Ridges, SeasonToDate
from ..store import DataStore

router = APIRouter(prefix="/phenology")

DAYS = 365


def _weekly(cur, codes: list[str], years: list[int]) -> dict[str, list[float]]:
    """Summed weekly new ringings over the given years, per species (52 values)."""
    out = {c: [0.0] * N_WEEKS for c in codes}
    if not codes or not years:
        return out
    for sp, week, n in cur.execute("""
        SELECT species, week, sum(n) FROM agg_counts_week
        WHERE list_contains(?, species) AND list_contains(?, year) GROUP BY ALL
    """, [codes, years]).fetchall():
        out[sp][week - 1] = float(n)
    return out


def _medians(cur, period_id: str) -> dict[tuple[str, str], dict]:
    rows = cur.execute("""
        SELECT species, season, mean_median_doy, mean_q25_doy, mean_q75_doy, years_used
        FROM agg_phenology_species WHERE period_id = ?
    """, [period_id]).fetchall()
    return {(sp, se): {"median": m, "q25": q25, "q75": q75, "years_used": y}
            for sp, se, m, q25, q75, y in rows}


@router.get("/calendar", response_model=Calendar)
def migration_calendar(
    year: str = Query("avg", description="'avg' (mean over the period's non-gap years) or a year"),
    period: Optional[str] = Query(None, description="Period for 'avg' (default: the first preset)"),
    top: Literal["5", "10", "15", "all"] = "10",
    species: Optional[str] = Query(None, description="Selected species, listed first"),
    store: DataStore = Depends(get_store),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    selected = [c for c in parse_species(store, species, required=False) if c != ALL_SPECIES]
    base = store.periods[0]
    p = parse_period(store, period) if period else base
    ref = store.reference_years(p.year_from, p.year_to)
    if year == "avg":
        mode, the_year, years, divisor = "avg", None, ref, len(ref)
    else:
        try:
            the_year = int(year)
        except ValueError:
            raise bad_request("year must be 'avg' or a year") from None
        if not store.first_year <= the_year <= store.current_year:
            raise bad_request(f"year must be between {store.first_year} and {store.current_year}")
        if store.is_gap(the_year):
            raise bad_request(f"{the_year} is a partly staffed year and is not offered")
        mode, years, divisor = "year", [the_year], 1
        # A single year keeps the ranking of the first preset, so the species
        # list doesn't change when another year is picked.
        ref = store.reference_years(base.year_from, base.year_to)

    # Rank by mean annual total over the reference years.
    ranking = cur.execute(f"""
        SELECT species, sum(n_new) / ? AS mean_total FROM agg_counts_year_season
        WHERE season = '{WHOLE_YEAR}' AND list_contains(?, year) AND species <> '{ALL_SPECIES}'
        GROUP BY species HAVING sum(n_new) > 0 ORDER BY mean_total DESC, species
    """, [len(ref), ref]).fetchall()
    ranked = [sp for sp, _ in ranking]
    top_codes = ranked if top == "all" else ranked[: int(top)]

    medians = _medians(cur, base.id)

    def timing_key(code: str) -> tuple:
        var = (medians.get((code, "var")) or {}).get("median")
        host = (medians.get((code, "host")) or {}).get("median")
        if var is not None:
            return (0, var, code)
        if host is not None:
            return (1, host, code)
        return (2, 0, code)

    others = sorted([c for c in top_codes if c not in selected], key=timing_key)
    codes = selected + others
    sums = _weekly(cur, codes, years)
    week_season = [w["season"] for w in week_infos(store)]

    rows, max_value = [], 0.0
    for code in codes:
        values = [v / divisor for v in sums[code]]
        info = store.species[code]
        if info["single_season"]:
            peaks = {s: max(values) for s in ("var", "host")}
        else:
            peaks = {s: max((v for v, ws in zip(values, week_season) if ws == s), default=0.0)
                     for s in ("var", "host")}
        scaled = [min(1.0, v / peaks[ws]) if peaks[ws] > 0 else 0.0
                  for v, ws in zip(values, week_season)]
        max_value = max(max_value, max(values))
        rows.append({
            "species": code, "name": store.name(code), "selected": code in selected,
            "total": rnd(sum(values)), "values": [rnd(v) for v in values],
            "scaled": [rnd(v, 4) for v in scaled],
        })
    return {"mode": mode, "year": the_year, "reference_years": ref if mode == "avg" else [],
            "weeks": week_infos(store), "max_value": rnd(max_value), "rows": rows}


def _year_medians(cur, year: int) -> dict[tuple[str, str], dict]:
    """The medians of one year, as `_medians` gives them for a period."""
    rows = cur.execute("""
        SELECT species, season, median_doy, q25_doy, q75_doy, meets_min_sample
        FROM agg_phenology_median WHERE year = ?
    """, [year]).fetchall()
    return {(sp, se): {"median": m if ok else None, "q25": q25 if ok else None,
                       "q75": q75 if ok else None, "years_used": 1 if ok else 0}
            for sp, se, m, q25, q75, ok in rows}


def _daily(cur, codes: list[str], years: list[int]) -> dict[str, list[float]]:
    """Summed daily new ringings over the given years, per species (365 values)."""
    out = {c: [0.0] * DAYS for c in codes}
    if not codes or not years:
        return out
    for sp, doy, n in cur.execute("""
        SELECT species, doy, sum(n_new) FROM agg_counts_day
        WHERE list_contains(?, species) AND list_contains(?, year) GROUP BY ALL
    """, [codes, years]).fetchall():
        out[sp][doy - 1] = float(n)
    return out


@router.get("/ridges", response_model=Ridges)
def ridges(
    species: str = Query(..., description="Comma-separated codes (max 6)"),
    period: Optional[str] = None,
    year: Optional[int] = Query(None, description="One year instead of a period; the last complete year stays alongside"),
    store: DataStore = Depends(get_store),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    """Mean daily catch per season (day resolution), the latest year alongside.

    With `year` the "mean" is that single year and the dashed "latest" line is
    still the last complete year.
    """
    codes = parse_species(store, species)
    if year is not None:
        if period is not None:
            raise bad_request("give either period or year, not both")
        if not store.first_year <= year <= store.complete_through:
            raise bad_request(f"year must be between {store.first_year} and {store.complete_through}")
        if store.is_gap(year):
            raise bad_request(f"{year} is a partly staffed year and is not offered")
    p = period_or_year(store, period, year)
    ref = store.reference_years(p.year_from, p.year_to)
    latest_year = store.complete_through if year is not None else p.year_to
    sums = _daily(cur, codes, ref)
    latest = _daily(cur, codes, [latest_year])
    medians = _year_medians(cur, year) if year is not None else _medians(cur, p.id)
    ringing_max = store.build["ringing_max_date"]
    data_through = normalized_doy(ringing_max) if latest_year == ringing_max.year else DAYS

    # Day ranges of the seasons, from the configured months (not hard-coded).
    month_season = {m: se for se, cfg in store.config["seasons"].items() for m in cfg["months"]}
    doy_season = [month_season[doy_to_date(d).month] for d in range(1, DAYS + 1)]

    out = []
    for code in codes:
        info = store.species[code]
        mean = [v / len(ref) if ref else 0.0 for v in sums[code]]
        seasons, y_max = {}, 0.0
        for season in ("var", "host"):
            if info["single_season"] and info["single_season_season"] != season:
                seasons[season] = None
                continue
            idx = [i for i, se in enumerate(doy_season) if se == season]
            m = medians.get((code, season)) or {}
            s_mean = [mean[i] for i in idx]
            s_latest = [int(latest[code][i]) if i < data_through else None for i in idx]
            y_max = max([y_max, *s_mean, *(v for v in s_latest if v is not None)])
            seasons[season] = {
                "doy_from": idx[0] + 1,
                "mean": [rnd(v, 3) for v in s_mean],
                "latest": s_latest,
                "median_doy": rnd(m.get("median")),
                "q25_doy": rnd(m.get("q25")),
                "q75_doy": rnd(m.get("q75")),
                "years_used": m.get("years_used") or 0,
            }
        out.append({"species": code, "single_season": info["single_season"],
                    "single_season_season": info["single_season_season"],
                    "y_max": rnd(y_max), **seasons})
    return {"period": p.__dict__, "reference_years": ref, "latest_year": latest_year,
            "data_through_doy": data_through, "species": out,
            "season_days": {se: {"doy_from": d.index(se) + 1, "doy_to": len(d) - d[::-1].index(se)}
                            for se in ("var", "host") for d in [doy_season]}}


@router.get("/season-to-date", response_model=SeasonToDate)
def season_to_date(
    species: str = Query(..., description="One species code (ALLA allowed)"),
    year: Optional[int] = Query(None, description="Defaults to the current year"),
    store: DataStore = Depends(get_store),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    (code,) = parse_species(store, species, max_n=1)
    year = store.current_year if year is None else year
    if not store.first_year < year <= store.current_year:
        raise bad_request(f"year must be between {store.first_year + 1} and {store.current_year}")

    # Earlier complete years, gap years removed.
    ref = store.reference_years(store.first_year, min(year - 1, store.complete_through))
    rows = cur.execute("""
        SELECT year, doy, sum(n_new) FROM agg_counts_day
        WHERE species = ? AND (list_contains(?, year) OR year = ?) GROUP BY ALL
    """, [code, ref, year]).fetchall()

    row_of = {y: i for i, y in enumerate(ref)}
    daily = np.zeros((len(ref), DAYS))
    this_year = np.zeros(DAYS)
    for y, doy, n in rows:
        if y == year:
            this_year[doy - 1] += n
        else:
            daily[row_of[y], doy - 1] += n
    cum = daily.cumsum(axis=1)
    current = this_year.cumsum()

    ringing_max = store.build["ringing_max_date"]
    data_through = normalized_doy(ringing_max) if year == ringing_max.year else DAYS

    if len(ref):
        mean = cum.mean(axis=0)
        sd = cum.std(axis=0, ddof=1) if len(ref) > 1 else np.zeros(DAYS)
        totals = cum[:, -1]
        top_i, worst_i = int(np.argmax(totals)), int(np.argmin(totals))
        top = {"year": ref[top_i], "total": int(totals[top_i]), "cumulative": [int(v) for v in cum[top_i]]}
        worst = {"year": ref[worst_i], "total": int(totals[worst_i]), "cumulative": [int(v) for v in cum[worst_i]]}
        sd_total = float(np.std(totals, ddof=1)) if len(ref) > 1 else None
        mean_total = float(totals.mean())
    else:
        mean = sd = np.zeros(DAYS)
        top = worst = None
        sd_total, mean_total = None, 0.0

    current_total = int(current[data_through - 1])
    normal = float(mean[data_through - 1])
    return {
        "species": code,
        "year": year,
        "data_through_doy": data_through,
        "reference_years": ref,
        "mean": [rnd(v, 1) for v in mean],
        "sd_low": [rnd(max(0.0, m - s), 1) for m, s in zip(mean, sd)],
        "sd_high": [rnd(m + s, 1) for m, s in zip(mean, sd)],
        "current": [int(v) if i < data_through else None for i, v in enumerate(current)],
        "top": top,
        "worst": worst,
        "summary": {
            "current_total": current_total,
            "normal_at_date": rnd(normal, 1),
            "diff": rnd(current_total - normal, 1),
            "mean_annual_total": rnd(mean_total, 1),
            "sd_annual_total": rnd(sd_total, 1),
        },
    }


@router.get("/table", response_model=PhenologyTable)
def species_table(
    period: Optional[str] = None,
    species: Optional[str] = Query(None, description="Selected species, listed first"),
    store: DataStore = Depends(get_store),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    selected = parse_species(store, species, required=False)
    p = parse_period(store, period)
    latest_year = p.year_to
    avg_years = store.reference_years(latest_year - 10, latest_year - 1)
    spark_years = list(range(p.year_from, p.year_to + 1))
    first = min(p.year_from, latest_year - 10)

    counts: dict[str, dict[int, int]] = {}
    for sp, y, n in cur.execute(f"""
        SELECT species, year, n_new FROM agg_counts_year_season
        WHERE season = '{WHOLE_YEAR}' AND year BETWEEN ? AND ? AND species <> '{ALL_SPECIES}'
    """, [first, p.year_to]).fetchall():
        counts.setdefault(sp, {})[y] = int(n)

    trend = {r[0]: r[1:] for r in cur.execute(f"""
        SELECT species, slope_per_year, ci_low, ci_high FROM agg_trend
        WHERE season = '{WHOLE_YEAR}' AND period_id = ?
    """, [p.id]).fetchall()}
    medians = _medians(cur, p.id)

    rows = {}
    for sp, by_year in counts.items():
        spark = [by_year.get(y, 0) for y in spark_years]
        total = sum(spark)
        if total == 0 and sp not in selected:
            continue
        slope, lo, hi = trend.get(sp, (None, None, None))
        info = store.species[sp]
        rows[sp] = {
            "species": sp, "name": store.name(sp), "name_sci": info["scientific_name"],
            "selected": sp in selected,
            "latest_n": by_year.get(latest_year, 0),
            "avg_n": rnd(sum(by_year.get(y, 0) for y in avg_years) / len(avg_years), 1) if avg_years else None,
            "total": total,
            "trend_slope": rnd(slope, 3), "trend_ci_low": rnd(lo, 3), "trend_ci_high": rnd(hi, 3),
            "verdict": verdict(lo, hi),
            "median_var_doy": rnd((medians.get((sp, "var")) or {}).get("median")),
            "median_host_doy": rnd((medians.get((sp, "host")) or {}).get("median")),
            "single_season": info["single_season"],
            "single_season_season": info["single_season_season"],
            "spark": spark,
        }
    by_total = sorted(rows, key=lambda c: (-rows[c]["total"], c))
    order = selected_first(by_total, selected)
    return {"period": p.__dict__, "latest_year": latest_year, "avg_years": avg_years,
            "spark_years": spark_years, "spark_gap": [store.is_gap(y) for y in spark_years],
            "rows": [rows[c] for c in order]}

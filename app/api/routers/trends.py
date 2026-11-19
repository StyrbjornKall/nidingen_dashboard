"""Trender: yearly counts with a linear fit and 95 % band, and the overview.

The fit itself is precomputed in ``agg_trend`` (gap years removed, exact t);
here we only draw the band: ŷ ± t·s·√(1/n + (x − x̄)²/Sxx).
"""

from __future__ import annotations

import math
from typing import Optional

import duckdb
from fastapi import APIRouter, Depends, Query

from app.src.build_aggregates import ALL_SPECIES
from ..params import SeasonOrYear, get_cursor, get_store, parse_period, parse_species, verdict
from ..schemas import TrendOverview, Trends
from ..store import DataStore

router = APIRouter(prefix="/trends")


def band(fit: dict, years: list[int]) -> list[dict]:
    out = []
    for x in years:
        y = fit["intercept"] + fit["slope_per_year"] * x
        half = fit["t_crit"] * fit["residual_se"] * math.sqrt(
            1 / fit["n_years"] + (x - fit["x_mean"]) ** 2 / fit["sxx"])
        out.append({"year": x, "fit": round(y, 3), "lo": round(y - half, 3), "hi": round(y + half, 3)})
    return out


def _excluded(store: DataStore, year_from: int, year_to: int) -> list[int]:
    return [y for y in range(year_from, year_to + 1) if store.is_gap(y)]


@router.get("", response_model=Trends)
def trends(
    species: str = Query(..., description="Comma-separated codes (max 6)"),
    season: SeasonOrYear = "host",
    period: Optional[str] = None,
    store: DataStore = Depends(get_store),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    codes = parse_species(store, species)
    p = parse_period(store, period)
    counts = cur.execute("""
        SELECT species, year, n_new, is_gap_year FROM agg_counts_year_season
        WHERE list_contains(?, species) AND season = ? AND year BETWEEN ? AND ?
        ORDER BY species, year
    """, [codes, season, p.year_from, p.year_to]).fetchall()
    fits_rows = cur.execute("""
        SELECT * FROM agg_trend WHERE list_contains(?, species) AND season = ? AND period_id = ?
    """, [codes, season, p.id])
    cols = [c[0] for c in fits_rows.description]
    fits = {r[0]: dict(zip(cols, r)) for r in fits_rows.fetchall()}

    years = list(range(p.year_from, p.year_to + 1))
    out = []
    for code in codes:
        fit = fits.get(code)
        out.append({
            "species": code,
            "years": [{"year": y, "n": int(n), "is_gap_year": g}
                      for sp, y, n, g in counts if sp == code],
            "fit": None if fit is None else {
                "slope": fit["slope_per_year"],
                "ci_low": fit["ci_low"],
                "ci_high": fit["ci_high"],
                "n_years": fit["n_years"],
                "mean": fit["mean"],
                "verdict": verdict(fit["ci_low"], fit["ci_high"]),
                "band": band(fit, years),
            },
        })
    return {"season": season, "period": p.__dict__,
            "excluded_years": _excluded(store, p.year_from, p.year_to), "species": out}


@router.get("/overview", response_model=TrendOverview)
def overview(
    season: SeasonOrYear = "host",
    period: Optional[str] = None,
    species: Optional[str] = Query(None, description="Selected species, listed first"),
    min_total: int = Query(0, ge=0, description="Leave out species with fewer birds in the period"),
    store: DataStore = Depends(get_store),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    selected = parse_species(store, species, required=False)
    p = parse_period(store, period)
    rows = cur.execute(f"""
        SELECT species, slope_per_year, ci_low, ci_high, n_years, mean, total
        FROM agg_trend
        WHERE season = ? AND period_id = ? AND species <> '{ALL_SPECIES}'
          AND (total >= ? OR list_contains(?, species))
        ORDER BY slope_per_year DESC, species
    """, [season, p.id, min_total, selected]).fetchall()
    items = {
        r[0]: {"species": r[0], "name": store.name(r[0]), "slope": r[1], "ci_low": r[2],
               "ci_high": r[3], "n_years": r[4], "mean": r[5], "total": r[6],
               "verdict": verdict(r[2], r[3]), "selected": r[0] in selected}
        for r in rows
    }
    order = [c for c in selected if c in items] + [c for c in items if c not in selected]
    return {"season": season, "period": p.__dict__,
            "excluded_years": _excluded(store, p.year_from, p.year_to),
            "rows": [items[c] for c in order]}

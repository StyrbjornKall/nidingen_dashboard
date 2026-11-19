"""Morfometri: species means for the bubble chart, and per-species detail."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, Query

from ..params import SeasonOrYear, get_cursor, get_store, parse_species, rnd
from ..schemas import MorphDetails, MorphMeans
from ..store import DataStore

router = APIRouter(prefix="/morphometrics")


@router.get("/means", response_model=MorphMeans)
def means(
    season: SeasonOrYear = "host",
    min_n: int = Query(10, ge=1, description="Minimum weighed and measured birds per species"),
    store: DataStore = Depends(get_store),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    rows = cur.execute("""
        SELECT species, n, n_total_since_1980, weight_mean, weight_med, wing_mean, wing_med
        FROM agg_morph_species
        WHERE season = ? AND weight_n >= ? AND wing_n >= ?
        ORDER BY n_total_since_1980 DESC, species
    """, [season, min_n, min_n]).fetchall()
    return {
        "season": season,
        "min_n": min_n,
        "rows": [
            {"species": sp, "name": store.name(sp), "n": n, "n_total_since_1980": total,
             "weight_mean": rnd(wm), "weight_median": rnd(wmed),
             "wing_mean": rnd(gm), "wing_median": rnd(gmed)}
            for sp, n, total, wm, wmed, gm, gmed in rows
        ],
    }


def _categories(rows: list[tuple]) -> list[dict]:
    total = sum(n for _, n in rows)
    return [{"code": code, "n": n, "share": round(n / total, 4) if total else 0.0} for code, n in rows]


@router.get("/detail", response_model=MorphDetails)
def detail(
    species: str = Query(..., description="Comma-separated codes (max 6, not ALLA)"),
    season: SeasonOrYear = "host",
    store: DataStore = Depends(get_store),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    codes = parse_species(store, species, allow_all=False)
    summary = cur.execute("SELECT * FROM agg_morph_species WHERE list_contains(?, species) AND season = ?",
                          [codes, season])
    cols = [c[0] for c in summary.description]
    summaries = {r[0]: dict(zip(cols, r)) for r in summary.fetchall()}
    bins = cur.execute("""
        SELECT species, variable, bin_low, bin_high, n FROM agg_morph_hist
        WHERE list_contains(?, species) AND season = ? ORDER BY species, variable, bin
    """, [codes, season]).fetchall()
    cats = cur.execute("""
        SELECT species, kind, category, n FROM agg_age_sex
        WHERE list_contains(?, species) AND season = ? ORDER BY species, kind, n DESC, category
    """, [codes, season]).fetchall()

    def measure(code: str, var: str) -> dict | None:
        s = summaries.get(code)
        if not s or not s[f"{var}_n"]:
            return None
        return {
            "n": s[f"{var}_n"],
            "mean": rnd(s[f"{var}_mean"]),
            "q25": rnd(s[f"{var}_q25"]),
            "median": rnd(s[f"{var}_med"]),
            "q75": rnd(s[f"{var}_q75"]),
            "bins": [{"low": rnd(lo, 3), "high": rnd(hi, 3), "n": n}
                     for sp, v, lo, hi, n in bins if sp == code and v == var],
        }

    out = []
    for code in codes:
        s = summaries.get(code) or {}
        out.append({
            "species": code,
            "n": s.get("n", 0),
            "n_total_since_1980": store.species[code]["n_new_total"],
            "weight": measure(code, "weight"),
            "wing": measure(code, "wing"),
            "age": _categories([(c, n) for sp, k, c, n in cats if sp == code and k == "age"]),
            "sex": _categories([(c, n) for sp, k, c, n in cats if sp == code and k == "sex"]),
        })
    return {"season": season, "species": out}

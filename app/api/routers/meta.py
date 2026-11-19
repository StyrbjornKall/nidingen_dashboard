"""GET /api/meta — everything the frontend needs before the first chart."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from app.src.build_aggregates import ALL_SPECIES, WHOLE_YEAR
from ..params import get_store
from ..schemas import Meta
from ..store import DataStore

router = APIRouter()


def _sort_key(row: dict) -> tuple:
    """ALLA first, then taxonomic order, unmapped codes last."""
    if row["species"] == ALL_SPECIES:
        return (0, 0.0, "")
    taxon = row.get("taxon_order")
    return (1 if taxon is not None else 2, taxon or 0.0, row["species"])


@router.get("/meta", response_model=Meta)
def meta(store: DataStore = Depends(get_store)) -> dict:
    cfg = store.config
    species = [
        {
            "code": r["species"],
            "name": store.name(r["species"]),
            "name_sv": r["swedish_name"],
            "name_sci": r["scientific_name"],
            "name_en": r["english_name"],
            "order": r["order_name"],
            "family": r["family_name"],
            "n_new_total": r["n_new_total"],
            "n_recap_total": r["n_recap_total"],
            "share_var": r["share_var"],
            "single_season": r["single_season"],
            "single_season_season": r["single_season_season"],
        }
        for r in sorted(store.species.values(), key=_sort_key)
    ]
    seasons = [
        {"id": sid, "label": s["label_sv"], "months": s["months"]}
        for sid, s in cfg["seasons"].items()
    ] + [{"id": WHOLE_YEAR, "label": "Helår", "months": list(range(1, 13))}]
    gap = cfg["trend_excluded_years"]
    return {
        "species": species,
        "seasons": seasons,
        "periods": [p.__dict__ for p in store.periods],
        "gap_years": {"year_from": gap["from"], "year_to": gap["to"], "reason": gap["reason_sv"]},
        "first_year": store.first_year,
        "current_year": store.current_year,
        "complete_through": store.complete_through,
        "max_compare_species": cfg["max_compare_species"],
        "default_selection": cfg["default_selection"],
        "median_min_birds": cfg["median_min_birds"],
        "single_season_threshold": cfg["single_season_threshold"],
        "weather_window": {
            "from_hour": cfg["weather_window"]["from_hour"],
            "to_hour": cfg["weather_window"]["to_hour"],
            "timezone": cfg["weather_window"]["timezone"],
            "note": cfg["weather_window"].get("note_sv", ""),
        },
        "active_weeks": {"week_from": store.active_weeks[0], "week_to": store.active_weeks[1]},
        "build": store.build,
    }

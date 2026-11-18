"""Request parameter parsing and small shared helpers for the routers."""

from __future__ import annotations

import math
from collections.abc import Iterator
from datetime import date, timedelta
from typing import Literal

import duckdb
from fastapi import Depends, HTTPException, Request

from app.src.build_aggregates import ALL_SPECIES, N_WEEKS, week_seasons
from .store import DataStore, Period

SeasonId = Literal["var", "host"]
SeasonOrYear = Literal["var", "host", "helar"]


def get_store(request: Request) -> DataStore:
    return request.app.state.store


def get_cursor(store: DataStore = Depends(get_store)) -> Iterator[duckdb.DuckDBPyConnection]:
    cur = store.cursor()
    try:
        yield cur
    finally:
        cur.close()


def bad_request(message: str) -> HTTPException:
    return HTTPException(status_code=422, detail=message)


def parse_species(
    store: DataStore,
    raw: str | None,
    *,
    max_n: int | None = None,
    allow_all: bool = True,
    required: bool = True,
) -> list[str]:
    """Parse ``RÖHAK,LÖSÅN`` into a validated, de-duplicated list (order kept)."""
    max_n = max_n or int(store.config["max_compare_species"])
    codes: list[str] = []
    for part in (raw or "").split(","):
        code = part.strip().upper()
        if code and code not in codes:
            codes.append(code)
    if required and not codes:
        raise bad_request("species is required")
    if len(codes) > max_n:
        raise bad_request(f"at most {max_n} species")
    unknown = [c for c in codes if c not in store.species]
    if unknown:
        raise bad_request(f"unknown species: {', '.join(unknown)}")
    if not allow_all and ALL_SPECIES in codes:
        raise bad_request(f"{ALL_SPECIES} is not allowed here")
    return codes


def parse_period(store: DataStore, period_id: str | None) -> Period:
    try:
        return store.period(period_id)
    except KeyError:
        ids = ", ".join(p.id for p in store.periods)
        raise bad_request(f"unknown period '{period_id}' (use one of: {ids})") from None


def period_or_year(store: DataStore, period_id: str | None, year: int | None) -> Period:
    """A preset period, or a single year (the design's single-year option)."""
    if year is not None:
        if period_id is not None:
            raise bad_request("give either period or year, not both")
        if not store.first_year <= year <= store.current_year:
            raise bad_request(f"year must be between {store.first_year} and {store.current_year}")
        return Period(str(year), str(year), year, year)
    return parse_period(store, period_id)


def verdict(ci_low: float | None, ci_high: float | None) -> str:
    """'up' / 'down' if the 95 % CI excludes 0, otherwise 'none'."""
    if ci_low is None or ci_high is None:
        return "none"
    if ci_low > 0:
        return "up"
    if ci_high < 0:
        return "down"
    return "none"


def rnd(value: float | None, digits: int = 2) -> float | None:
    if value is None:
        return None
    value = float(value)
    if math.isnan(value) or math.isinf(value):
        return None
    return round(value, digits)


# -- 7-day weeks -----------------------------------------------------------

_JAN1 = date(2001, 1, 1)   # a non-leap year: doy matches the aggregates


def week_dates(week: int, year: int) -> tuple[date, date]:
    """First and last date of a 7-day week (week 1 = 1–7 January) in a year.

    Week 52 runs to 31 December. In leap years 29 February belongs to week 9
    together with 28 February, so week 9 has 8 days.
    """
    start = _JAN1 + timedelta(days=(week - 1) * 7)
    end = date(2001, 12, 31) if week == N_WEEKS else start + timedelta(days=6)
    return date(year, start.month, start.day), date(year, end.month, end.day)


def week_infos(store: DataStore) -> list[dict]:
    seasons = dict(week_seasons(store.config))
    out = []
    for week in range(1, N_WEEKS + 1):
        start, end = week_dates(week, 2001)
        out.append({
            "week": week,
            "start_month": start.month, "start_day": start.day,
            "end_month": end.month, "end_day": end.day,
            "season": seasons[week],
        })
    return out


def normalized_doy(d: date) -> int:
    """Day of year on a non-leap calendar (29 Feb -> 28 Feb), as in the aggregates."""
    day = min(d.day, 28) if d.month == 2 else d.day
    return date(2001, d.month, day).timetuple().tm_yday


def doy_to_date(doy: int, year: int = 2001) -> date:
    """Inverse of the aggregates' non-leap day of year."""
    d = _JAN1 + timedelta(days=int(doy) - 1)
    return date(year, d.month, d.day)


def selected_first(codes: list[str], selected: list[str]) -> list[str]:
    """Selected species first (in selection order), then the rest in given order."""
    chosen = [c for c in selected if c in codes]
    return chosen + [c for c in codes if c not in chosen]

"""
Settings and the shared read-only data store.

The API reads only the small ``agg_*`` tables built by
``app/src/build_aggregates.py`` (plus a count from ``fynd``). It opens the
DuckDB file once per process, read-only, and hands out one cursor per
request. Restart the API after the database is rebuilt or replaced.
"""

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
from dotenv import load_dotenv

from app.src.build_aggregates import (
    ALL_SPECIES,
    N_WEEKS,
    AGG_TABLES,
    config_digest,
    load_config,
    load_countries,
)

log = logging.getLogger("nidingen.api")

REPO = Path(__file__).resolve().parents[2]

# Bump when response shapes change, so browsers drop cached responses.
API_VERSION = "5"


# Share of the mean annual catch a week needs to fall inside the station's
# active weeks (see DataStore._active_weeks).
ACTIVE_WEEK_SHARE = 0.001


@dataclass(frozen=True)
class Settings:
    db_path: Path
    config_dir: Path
    web_dist: Path = REPO / "web" / "dist"


def get_settings() -> Settings:
    load_dotenv(REPO / ".env")
    return Settings(
        db_path=Path(os.getenv("DUCKDB_PATH", str(REPO / "data" / "bird_ringing_0016.duckdb"))),
        config_dir=Path(os.getenv("NIDINGEN_CONFIG_DIR", str(REPO / "config"))),
        web_dist=Path(os.getenv("WEB_DIST", str(REPO / "web" / "dist"))),
    )


@dataclass(frozen=True)
class Period:
    id: str
    label: str
    year_from: int
    year_to: int


class DataStore:
    """Read-only connection plus everything that is constant per build."""

    def __init__(self, settings: Settings):
        if not settings.db_path.exists():
            raise FileNotFoundError(
                f"Database not found: {settings.db_path}. Set DUCKDB_PATH to the "
                "bird_ringing_0016.duckdb file."
            )
        self.con = duckdb.connect(str(settings.db_path), read_only=True)
        tables = {r[0] for r in self.con.execute("SHOW TABLES").fetchall()}
        missing = [t for t in AGG_TABLES if t not in tables]
        if missing:
            self.con.close()
            raise RuntimeError(
                f"Aggregate tables missing ({', '.join(missing)}). "
                "Run: uv run python app/src/build_aggregates.py"
            )

        self.config: dict[str, Any] = load_config(settings.config_dir)
        countries = load_countries(settings.config_dir)

        cur = self.con.cursor()
        row = cur.execute("SELECT * FROM agg_build_info").fetchone()
        cols = [c[0] for c in cur.description]
        self.build: dict[str, Any] = dict(zip(cols, row))
        if self.build["config_digest"] != config_digest(self.config, countries):
            log.warning("config/ has changed since the aggregates were built; "
                        "re-run build_aggregates.py")

        species_rows = cur.execute("SELECT * FROM agg_species").fetchall()
        cols = [c[0] for c in cur.description]
        self.species: dict[str, dict[str, Any]] = {
            r[0]: dict(zip(cols, r)) for r in species_rows
        }
        cur.close()

        gap = self.config["trend_excluded_years"]
        self.gap_from, self.gap_to = int(gap["from"]), int(gap["to"])
        self.periods = [
            Period(p["id"], p["label_sv"], int(p["from"]), int(p["to"]))
            for p in self.config["period_options"]
        ]
        self.complete_through = max(p.year_to for p in self.periods)
        self.first_year = int(self.build["ringing_min_date"].year)
        self.current_year = int(self.build["ringing_max_date"].year)

        self.active_weeks = self._active_weeks()
        cur = self.con.cursor()
        # Years with weather: the range the Väder year picker offers.
        self.weather_years: tuple[int, int] | None = None
        first, last = cur.execute("SELECT min(year), max(year) FROM agg_weather_daily").fetchone()
        if first is not None:
            self.weather_years = (int(first), int(last))
        cur.close()

        stamp = f"{self.build['built_at_utc']}|{self.build['config_digest']}|{API_VERSION}"
        self.etag = 'W/"' + hashlib.sha256(stamp.encode()).hexdigest()[:20] + '"'

    # -- helpers -----------------------------------------------------------

    def cursor(self) -> duckdb.DuckDBPyConnection:
        return self.con.cursor()

    def close(self) -> None:
        self.con.close()

    def is_gap(self, year: int) -> bool:
        return self.gap_from <= year <= self.gap_to

    def reference_years(self, year_from: int, year_to: int) -> list[int]:
        """Years in the range that count for statistics (gap years removed)."""
        return [y for y in range(year_from, year_to + 1) if not self.is_gap(y)]

    def period(self, period_id: str | None) -> Period:
        if period_id is None:
            return self.periods[0]
        for p in self.periods:
            if p.id == period_id:
                return p
        raise KeyError(period_id)

    def _active_weeks(self) -> tuple[int, int]:
        """First and last week in which the station normally catches birds.

        A week counts when the all-species mean catch in it is at least
        ACTIVE_WEEK_SHARE of the mean annual total, over the first period's
        reference years. A display window for week charts (weeks 10–47
        today), not a season boundary.
        """
        base = self.periods[0]
        ref = self.reference_years(base.year_from, base.year_to)
        rows = self.con.execute(
            "SELECT week, sum(n) FROM agg_counts_week WHERE species = ? AND list_contains(?, year) GROUP BY week",
            [ALL_SPECIES, ref]).fetchall()
        total = sum(n for _, n in rows)
        active = sorted(w for w, n in rows if total and n / total >= ACTIVE_WEEK_SHARE)
        return (active[0], active[-1]) if active else (1, N_WEEKS)

    def name(self, code: str) -> str:
        """Display name: Swedish name with a capital letter, or the code."""
        if code == ALL_SPECIES:
            return "Alla arter"
        sv = (self.species.get(code) or {}).get("swedish_name")
        return sv[:1].upper() + sv[1:] if sv else code


def as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    return value

"""
Response models. They document the API (see /api/docs) and are the
contract for the React frontend; keep them in step with HANDOFF.md §6.

Conventions: species are RUBIN codes (``ALLA`` = all species); seasons are
``var`` (januari–juni), ``host`` (juli–december) and ``helar`` (whole year);
``doy`` is the day of year on a non-leap calendar; ``week`` is a 7-day period
counted from 1 January (week 1 = 1–7 January), not an ISO week.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel

Verdict = Literal["up", "down", "none"]


# -- meta --------------------------------------------------------------------

class SpeciesInfo(BaseModel):
    code: str
    name: str                      # display name: Swedish name, else the code
    name_sv: Optional[str]
    name_sci: Optional[str]
    name_en: Optional[str]
    order: Optional[str]
    family: Optional[str]
    n_new_total: int
    n_recap_total: int
    share_var: Optional[float]
    single_season: bool
    single_season_season: Optional[str]


class PeriodInfo(BaseModel):
    id: str
    label: str
    year_from: int
    year_to: int


class SeasonInfo(BaseModel):
    id: str
    label: str
    months: list[int]


class GapYears(BaseModel):
    year_from: int
    year_to: int
    reason: str


class WeatherWindow(BaseModel):
    from_hour: int
    to_hour: int
    timezone: str
    note: str                     # Swedish note for Väder and Om


class BuildInfo(BaseModel):
    built_at_utc: datetime
    ringing_min_date: date
    ringing_max_date: date
    recoveries_max_date: Optional[date]
    weather_max_date: Optional[date]


class ActiveWeeks(BaseModel):
    week_from: int
    week_to: int


class WeatherYears(BaseModel):
    year_from: int
    year_to: int


class Meta(BaseModel):
    species: list[SpeciesInfo]
    seasons: list[SeasonInfo]
    periods: list[PeriodInfo]
    gap_years: GapYears
    first_year: int
    current_year: int
    complete_through: int
    max_compare_species: int
    default_selection: list[str]
    median_min_birds: int
    single_season_threshold: float
    weather_window: WeatherWindow
    active_weeks: ActiveWeeks     # display window for week charts
    weather_years: Optional[WeatherYears]   # years with weather (Väder year picker)
    build: BuildInfo


# -- summary -----------------------------------------------------------------

class Kpis(BaseModel):
    n_new: int
    n_recap: int
    n_recoveries: int
    n_species: int
    n_species_named: int
    first_year: int
    last_year: int


class Bucket(BaseModel):
    start: date
    end: date
    year: int
    index: int                    # year, month (1–12), week (1–52) or doy
    is_gap_year: bool


class Series(BaseModel):
    species: str
    new: list[int]
    recap: list[int]
    total_new: int
    total_recap: int


class TimeSeries(BaseModel):
    grain: Literal["day", "week", "month", "year"]
    period: PeriodInfo
    buckets: list[Bucket]
    series: list[Series]


class TopSpeciesRow(BaseModel):
    rank: int
    species: str
    name: str
    n: int
    share: float
    selected: bool


class TopSpecies(BaseModel):
    period: PeriodInfo
    total: int
    n_species: int
    rows: list[TopSpeciesRow]


# -- trends ------------------------------------------------------------------

class YearCount(BaseModel):
    year: int
    n: int
    is_gap_year: bool


class BandPoint(BaseModel):
    year: int
    fit: float
    lo: float
    hi: float


class Fit(BaseModel):
    slope: float
    ci_low: float
    ci_high: float
    n_years: int
    mean: float
    verdict: Verdict
    band: list[BandPoint]


class TrendSeries(BaseModel):
    species: str
    years: list[YearCount]
    fit: Optional[Fit]


class Trends(BaseModel):
    season: str
    period: PeriodInfo
    excluded_years: list[int]
    species: list[TrendSeries]


class TrendRow(BaseModel):
    species: str
    name: str
    slope: float
    ci_low: float
    ci_high: float
    n_years: int
    mean: float
    total: int
    verdict: Verdict
    selected: bool


class TrendOverview(BaseModel):
    season: str
    period: PeriodInfo
    excluded_years: list[int]
    rows: list[TrendRow]


# -- morphometrics -----------------------------------------------------------

class MorphMean(BaseModel):
    species: str
    name: str
    n: int
    n_total_since_1980: int
    weight_mean: float
    weight_median: float
    wing_mean: float
    wing_median: float


class MorphMeans(BaseModel):
    season: str
    min_n: int
    rows: list[MorphMean]


class HistBin(BaseModel):
    low: float
    high: float
    n: int


class Measure(BaseModel):
    n: int
    mean: float
    q25: float
    median: float
    q75: float
    bins: list[HistBin]


class Category(BaseModel):
    code: Optional[str]           # raw age_code / sex; null = missing ("Okänd")
    n: int
    share: float


class MorphDetail(BaseModel):
    species: str
    n: int
    n_total_since_1980: int
    weight: Optional[Measure]
    wing: Optional[Measure]
    age: list[Category]
    sex: list[Category]


class MorphDetails(BaseModel):
    season: str
    species: list[MorphDetail]


# -- phenology ---------------------------------------------------------------

class WeekInfo(BaseModel):
    week: int
    start_month: int
    start_day: int
    end_month: int
    end_day: int
    season: str


class CalendarRow(BaseModel):
    species: str
    name: str
    selected: bool
    total: float                  # mean annual total (avg) or the year's total
    values: list[float]           # 52 weekly values
    scaled: list[float]           # 0–1, normalised per season ("Per säsong")


class Calendar(BaseModel):
    mode: Literal["avg", "year"]
    year: Optional[int]
    reference_years: list[int]
    weeks: list[WeekInfo]
    max_value: float
    rows: list[CalendarRow]


class RidgeSeason(BaseModel):
    doy_from: int                 # day of year (non-leap) of the first value
    mean: list[float]             # mean daily catch, one value per day of the season
    latest: list[Optional[int]]   # latest year; null after the last ringing date
    median_doy: Optional[float]
    q25_doy: Optional[float]
    q75_doy: Optional[float]
    years_used: int


class RidgeSpecies(BaseModel):
    species: str
    single_season: bool
    single_season_season: Optional[str]
    y_max: float
    var: Optional[RidgeSeason]
    host: Optional[RidgeSeason]


class DayRange(BaseModel):
    doy_from: int
    doy_to: int


class Ridges(BaseModel):
    period: PeriodInfo
    reference_years: list[int]
    latest_year: int
    data_through_doy: int         # the latest year has data up to this day
    season_days: dict[str, DayRange]   # first and last day of each season (var, host)
    species: list[RidgeSpecies]


class YearCurve(BaseModel):
    year: int
    total: int
    cumulative: list[int]


class SeasonToDateSummary(BaseModel):
    current_total: int
    normal_at_date: float
    diff: float
    mean_annual_total: float
    sd_annual_total: Optional[float]


class SeasonToDate(BaseModel):
    species: str
    year: int
    data_through_doy: int
    reference_years: list[int]
    mean: list[float]             # 365 values, cumulative mean by doy
    sd_low: list[float]
    sd_high: list[float]
    current: list[Optional[int]]  # null after data_through_doy
    top: Optional[YearCurve]
    worst: Optional[YearCurve]
    summary: SeasonToDateSummary


class TableRow(BaseModel):
    species: str
    name: str
    name_sci: Optional[str]
    selected: bool
    latest_n: int
    avg_n: Optional[float]
    total: int
    trend_slope: Optional[float]
    trend_ci_low: Optional[float]
    trend_ci_high: Optional[float]
    verdict: Verdict
    median_var_doy: Optional[float]
    median_host_doy: Optional[float]
    single_season: bool
    single_season_season: Optional[str]
    spark: list[int]


class PhenologyTable(BaseModel):
    period: PeriodInfo
    latest_year: int
    avg_years: list[int]
    spark_years: list[int]
    spark_gap: list[bool]
    rows: list[TableRow]


# -- recoveries --------------------------------------------------------------

class RecoveryPoint(BaseModel):
    direction: Literal["out", "in"]
    species: str
    ring_date: Optional[date]
    find_date: date
    days: Optional[int]
    lat: float
    lon: float
    place: Optional[str]
    country: Optional[str]
    distance_km: Optional[int]
    find_type: Optional[str]


class RecoverySummary(BaseModel):
    n_out: int                    # ignores the direction filter
    n_in: int                     # ignores the direction filter
    n_points: int
    longest_km: Optional[int]
    median_km: Optional[float]


class CountryCount(BaseModel):
    country: str
    n: int


class SpeciesCount(BaseModel):
    species: str
    n: int


class Recoveries(BaseModel):
    period: PeriodInfo
    direction: Literal["both", "out", "in"]
    species: Optional[str]
    points: list[RecoveryPoint]
    summary: RecoverySummary
    countries: list[CountryCount]
    species_options: list[SpeciesCount]   # species with recoveries in the period and direction
    longest: list[RecoveryPoint]


# -- weather -----------------------------------------------------------------

class WeatherDay(BaseModel):
    # Weather values cover only the catch window (meta.weather_window,
    # 03:00–12:00 Swedish time); precipitation is the sum for that window.
    date: date
    doy: int
    catch: int                    # new ringings of the chosen species
    active: bool                  # any ringing or recapture that day (station staffed)
    temp_mean: Optional[float]
    temp_min: Optional[float]
    temp_max: Optional[float]
    wind_speed: Optional[float]
    wind_dir: Optional[float]     # degrees the wind blows from (vector mean)
    gust_max: Optional[float]
    pressure: Optional[float]
    precipitation: Optional[float]
    visibility_km: Optional[float]
    cloud_cover: Optional[float]
    humidity: Optional[float]
    precipitation_from_vinga: bool
    pressure_from_vinga: bool
    visibility_from_vinga: bool


class WindSector(BaseModel):
    sector: int                   # 0 = N, clockwise in 45° steps
    label: str
    days: int
    mean_catch: Optional[float]


class Weather(BaseModel):
    year: int
    season: str
    species: str
    is_gap_year: bool
    days: list[WeatherDay]
    wind_rose: list[WindSector]
    best_days: list[WeatherDay]

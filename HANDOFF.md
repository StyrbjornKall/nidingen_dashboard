# Handoff: from Dash to a React dashboard

This document is the plan for replacing the Plotly Dash app in this repository with a FastAPI backend and a React frontend, following the approved design in `design/`. Read it together with:

- `config/analysis.json`: seasons, years left out of trends, period options, the single-season threshold. This is the **single source of truth** for these rules; both backend and frontend read it.
- `design/README.md`: how to read the design files, and where they are out of date.
- `.github/copilot-instructions.md`: column-level schema, join paths and data quirks of the existing DuckDB database.

Where this document and the design files disagree, **this document wins**.

---

## 1. Decisions already made

| Topic | Decision |
| --- | --- |
| Seasons | **Vår = januari–juni, höst = juli–december**, by calendar month: `season = 'var' if month <= 6 else 'host'`. |
| Gap years | **1991–2004** were only partly staffed. They are **excluded from all trends and multi-year statistics** (see §5.1) but still shown in raw time series, visually marked as "delvis bemannad". |
| Single-season species | A species with **more than 90 %** of its new ringings (all years) in one season gets **one** median passage day, for that season only. All other species get one median per season. The threshold is `single_season_threshold` in `config/analysis.json`; there is no hand-made list. |
| Trend units | Trends are absolute, never percent: **birds per year** for counts, **days** for timing. |
| Charts with several species | Show **raw counts**. Each species gets its **own y axis** where species are drawn separately (ridgelines). |
| Ordering | Wherever a chart or list shows more species than the selected ones, **selected species come first**, in selection order. |
| Language | The UI is **Swedish only**. Code, comments and commit messages are in English. |
| Colours and type | The "Ejder" palette in `design/tokens.json`, with Hanken Grotesk and IBM Plex Mono. |
| Species comparison | Up to **6** species, each with a fixed colour by selection order. The selection is shared by Sammanfattning, Trender, Morfometri and Fenologi, and is kept in the URL (`?arter=RÖHAK,LÖSÅN`). |
| Own controls | Återfynd, Väder and Flyttningskalendern (on Fenologi) have their own controls and are not affected by the species picker. |

## 2. Open questions and answers

Answered by the owner in phase 1 (2026-11-18) unless marked **open**.

| # | Question | Answer |
| --- | --- | --- |
| 1 | What counts as "ringmärkta"? | **New ringings only (`ringon`).** Recaptures (`kontr`) stay in the aggregates as `n_recap` and are shown as their own data (a KPI and an optional series on Sammanfattning), never added to "ringmärkta". Beslutat 2026-11-18. |
| 2 | Minimum sample per median | **10 birds** per species × year × season (`median_min_birds` in `config/analysis.json`). Beslutat 2026-11-18. |
| 3 | Hudsonspov | **Moot.** None of the 41 species in the original single-season list has ever been ringed at Nidingen. The list is dropped and replaced by a data rule: more than 90 % of new ringings in one season gives one season (93 of 210 ringed codes today, mostly rare; among species with ≥ 100 birds: ÄNPIP, KÄSNÄ, BLMES, VIHÄM, KUSNÄ, BLHAK, SPSNÄ, STMES, SNSPA, SMSNÄ, BRHAN, MFTHU, SVMES, MYSPO, GRBEN, PCCOL). Beslutat 2026-11-18. |
| 4 | Period presets, free year range? | **Presets only** (`period_options` in `config/analysis.json`). No free range; the free date pickers of the Dash app are dropped. Beslutat 2026-11-18. |
| 5 | Hosting limits on SciLifeLab Serve | **Deferred** by the owner (2026-11-18). Working assumption until revisited: ≤ 1 GB RAM per container, 2 workers, in-process cache per worker; the aggregate tables are small. |
| 6 | Seasons on Väder | **The two site seasons** (Vår januari–juni, Höst juli–december) from config. The design's Vår / Sommar / Höst is dropped. Beslutat 2026-11-18. |
| 7 | Age classes on Morfometri (new) | **Show the raw age codes, ungrouped.** The design's 1K / 2K / +1K / +2K do not exist in the data. Codes in `ringon`: `10, 2+, 20, 3+, 00, 1+, 4+, 5+, 30, 6+, 40, 50`; `kontr` also has `--` and `0+`. NULL and `''` show as "Okänd". Beslutat 2026-11-18. |

## 2b. Decisions from phase 1

- **Single-season species** are derived from the data in `build_aggregates.py` (threshold in config), for every species, not only waders.
- **Feature parity kept** beyond the design: the weather variable choice ("Visa" chips) with values gap-filled from Vinga A marked as such (precipitation, pressure, visibility); recaptures as data; species codes without a Swedish name (78 codes at the time, for example `KASS`, `KETÄR`; 2 after the manual code list) stay selectable by code, with the code as fallback name.
- **Dropped from the Dash app:** free date ranges, heatmap top-N 30 / 50 / 100 (the design's Topp 5 / 10 / 15 / Alla only), violin plots (replaced by 50-bin histograms), fat score, and later (phase 7) the `TOTAL` rows in `ring_records`.
- **Återfynd** filters its period on the **find date**, as the Dash app does. Recoveries are not subject to the gap-year rule (no trend is computed).
- **Daily weather covers only 03:00–12:00 Swedish time**, when most birds are caught (72 % of new ringings with a recorded hour), for every weather metric. Precipitation sums the hours ending 04:00–12:00. The window and its Swedish note are in `config/analysis.json` (`weather_window`); Väder and Om show the note. Decided 2026-11-18.
- **Weeks** are 7-day periods counted from 1 January (week 1 = 1–7 January, week 52 = 24–31 December), never calendar (ISO) week numbers, on every page. The Om page explains this; the copy is in `design/README.md`. Decided 2026-11-18.
- **Morfometri** aggregates use **all years** (descriptive statistics, no trend).
- **Current year** = the latest year in `ringon`; **complete years** end at the upper bound of `period_options`. The "idag" marker uses the real date.
- **Species count on Hem:** the real number of ringed species codes (210 today; 209 have a Swedish name since the manual code list was added) is computed from the data. The design's "133" is the number of mapped names; settle the wording in the Hem PR.
- **Countries for Återfynd:** `fynd.province_code` is a EURING place code (country prefix + region, for example `SV`, `DK`, `ES`, `+A`). Phase 2 adds a EURING country → Swedish name lookup.
- **Line endings:** 24 tracked files under `app/` are stored with CRLF and there is no `.gitattributes`. Preserve CRLF when editing them, or normalise them in a separate whitespace-only commit.
- **Design corrections** to apply when building pages are listed in `design/README.md`, "Design deviations decided in phase 1".

## 3. Target architecture

```
DuckDB file (persistent volume, read-only at runtime)
        │
FastAPI (Python 3.13, uv) ── /api/* JSON endpoints, cached
        │
React + Vite + TypeScript ── built to static files, served by FastAPI at /
```

- **One Docker container**, as today. Gunicorn with Uvicorn workers on port 8000, running as UID 1000. The database stays outside the image, at the path in `DUCKDB_PATH`.
- **Keep the existing Python data layer.** `convert_mdb_to_duckdb.py`, `fetch_smhi_weather.py`, `db_manager.py` and the useful parts of `query_utils.py` carry over. Only Dash, Dash Bootstrap Components, Plotly and flask-caching go.
- **Charts are written in React with SVG and d3 helpers** (`d3-scale`, `d3-shape`, `d3-geo`), or visx. Do not use Plotly. The designs need custom charts (ridgelines with per-species axes, a forest plot, a bubble chart on log axes, a globe) that generic chart libraries handle poorly.
- **Globe:** `d3-geo` orthographic projection with Natural Earth 1:110m land, bundled with the app. Dragging rotates it, buttons zoom it, and "Centrera på Nidingen" flies back to the island.

## 4. Data layer: precompute or calculate live?

The data changes **once per season**, so anything that scans individual birds is computed **once, when the database is built**. The API then reads small aggregate tables, and the browser does the light maths.

### 4.1 New build step

Add `app/src/build_aggregates.py`. It runs after `convert_mdb_to_duckdb.py` (and after the weather fetch), reads `config/analysis.json`, and (re)creates the `agg_*` tables below. Re-running it is safe.

Drop the duplicated `TOTAL` rows from the analysis path. Totals across species come from `GROUP BY ROLLUP` / `GROUPING SETS` in these tables, using the species code `ALLA`. Keep `ring_records` as it is until Dash is removed, then simplify it.

### 4.2 Aggregate tables

All tables carry `is_gap_year BOOLEAN` where relevant, so every consumer can filter consistently.

| Table | Grain | Columns (suggested) | Used by |
| --- | --- | --- | --- |
| `agg_counts_day` | species × date | `species, date, year, doy, month, iso_week, season, n_new, n_recap, is_gap_year` | Sammanfattning (day/week/month/year series), the season-to-date chart, Väder |
| `agg_counts_year_season` | species × year × season (+ `ALL`) | `species, year, season, n_new, n_recap, is_gap_year` | Trender, Sammanfattning year view, the trend overview, tables |
| `agg_counts_week` | species × year × week | `species, year, week, season, n, is_gap_year` | Flyttningskalendern (heatmap), ridgelines |
| `agg_phenology_median` | species × year × season | `species, year, season, n, median_doy, q25_doy, q75_doy, is_gap_year` | Ridgelines, the species table |
| `agg_phenology_species` | species × season × period | `species, season, period_id, mean_median_doy, years_used, single_season BOOLEAN` | Species table and ridgeline medians (**one source for both**) |
| `agg_trend` | species × season × period | `species, season, period_id, slope_per_year, ci_low, ci_high, n_years, mean` | "Alla arter" trend overview on Trender |
| `agg_morph_species` | species × season | `species, season, n, weight_mean, weight_q25, weight_med, weight_q75, wing_mean, wing_q25, wing_med, wing_q75, n_total_since_1980` | Morfometri bubble chart and key figures |
| `agg_morph_hist` | species × season × variable × bin | `species, season, variable, bin, bin_low, bin_high, n` (**50 bins**, p0.5–p99.5) | Morfometri distributions |
| `agg_age_sex` | species × season × category | `species, season, kind ('age'/'sex'), category, n` | Morfometri "Ålder och kön" |
| `agg_recoveries` | one row per recovery | `species, direction ('out'/'in'), ring_date, find_date, days, lat, lon, place, country, distance_km, bearing_deg` | Återfynd (about 6 000 rows, sent to the client after filtering) |

Weather stays as it is (`weather_daily`, about 16 000 rows) and is queried live.

**As built in phase 2** (full column reference: `.github/copilot-instructions.md` §5.3b). Differences from the suggestions above:
- `week` replaces `iso_week`: `(doy − 1) // 7 + 1` on a non-leap calendar, capped at 52, so weeks never cross a year boundary. A week's season is the season of its 4th day.
- Extra tables: `agg_species` (names, totals, single-season flag), `euring_countries` and `agg_build_info` (build time for ETags and "Senast uppdaterad").
- `agg_counts_year_season` also has `season = 'helar'` (Trender's Helår) and is dense, so trends see years with zero birds.
- `agg_trend` also stores `intercept`, `slope_se`, `t_crit`, `residual_se`, `x_mean` and `sxx`, enough to draw the CI band without the raw points.
- `agg_morph_hist` uses one bin per measurement step (1 mm, 0.1 g) when the range has fewer than 50 steps, so integer wing lengths don't produce a comb of empty bins.
- `agg_recoveries`: inbound is one row per foreign-ringed bird (its first encounter), and outbound joins on ring number **and** centre.

### 4.3 Calculated live (API or browser)

- Filtering by species, season and period.
- **Linear regression with a 95 % band** for the selected species (at most 6 species × 46 points). Use the exact t quantile with `df = n − 2` (`scipy.stats.t.ppf` in Python). The design sketch uses an approximation.
- The season-to-date comparison for the current year: cumulative sums from `agg_counts_day`.
- Day-level series for a single year, and weather windows.

### 4.4 Tests for the data layer

- Totals reconcile with the source: about 396 000 rows in `ringon`, 54 000 in `kontr` and 6 000 in `fynd` (exact numbers from the database).
- `sum(agg_counts_year_season.n_new) == count(ringon)` for every species.
- Every median in `agg_phenology_species` equals the mean of the per-year medians it is built from, with gap years excluded.
- Gap years never enter `agg_trend` or `agg_phenology_species`.
- The single-season flag follows the threshold rule: a fixture species just above and one just below 90 % in one season get one and two medians respectively.

## 5. Statistical definitions

### 5.1 Gap years (1991–2004)

Excluded from:
- linear trends and their confidence bands, and the trend overview;
- "normal year" averages, the ±1 SD band, top year and worst year in the season-to-date chart;
- multi-year averages of counts (ridgelines, migration calendar "Snitt alla år");
- mean median passage days, and "Snitt" columns in tables.

Still shown in raw yearly or daily series, marked as "delvis bemannad" (for example hatched or lighter bars, plus a note). Gap years are not offered as single years in year pickers that feed statistics. The design's Flyttningskalendern year list still offers 1990, which is staffed again under the 1991–2004 range, but older design text calls it a gap year: follow `config/analysis.json`.

### 5.2 Trends

Ordinary least squares on yearly counts within the chosen period, with gap years removed. Report the slope in **birds per year** and its 95 % CI (`slope ± t(0.975, n−2) · SE`). The verdict is "Ökar" or "Minskar" if the CI excludes 0, otherwise "Ingen säker trend". The band shows the 95 % CI of the fitted mean line. Numbers under 10 are shown with one decimal, larger numbers as whole numbers. Swedish formatting: decimal comma, the minus sign `−`, and a space as thousands separator.

### 5.3 Median passage day

For each species, year and season: the day of year on which the cumulative count first reaches half of that season's total. Species summary = **mean of the yearly medians** over the selected period, excluding gap years and years below the minimum sample size (open question 2). Shown as a date ("19 apr"). Single-season species get one median, for their majority season.

### 5.4 Season-to-date chart (Fenologi)

One chart for the **whole year**, not split by season. It shows cumulative new ringings by day of year for the current year, against:
- the mean of earlier complete years;
- a ±1 SD band (across years, scaled along the mean curve);
- the top year and the worst year.

Gap years are excluded from the mean, the band and the top/worst years. The summary shows the current total, the difference against a normal year at the same date in birds, and the top year, worst year and standard deviation.

### 5.5 Migration calendar normalisation

The "Per säsong" scale normalises each species **within each season separately** (January–June and July–December), so both peaks show. Single-season species are normalised on their single season. The "Antal" scale uses one global square-root scale.

## 6. API sketch

All endpoints return JSON shaped for the charts. Use `Cache-Control` and ETags; data changes only when the database is rebuilt.

| Endpoint | Parameters | Returns |
| --- | --- | --- |
| `GET /api/meta` | | species list (code, Swedish name, scientific name, total, single_season), years, gap years, seasons, period options, last updated |
| `GET /api/summary/kpis` | | totals (new, recaptures, recoveries, species) |
| `GET /api/summary/timeseries` | `species`, `from`, `to`, `grain=day\|week\|month\|year` | series per species |
| `GET /api/summary/top-species` | `from`, `to`, `limit=100` | ranked list with counts and shares |
| `GET /api/trends` | `species`, `season`, `period` | yearly counts and fit per species (slope, CI, band points) |
| `GET /api/trends/overview` | `season`, `period` | slope and CI for all species |
| `GET /api/morphometrics/means` | `season` | all species: mean weight, wing, total (bubble chart) |
| `GET /api/morphometrics/detail` | `species`, `season` | quartiles, 50-bin histograms, age and sex shares |
| `GET /api/phenology/calendar` | `year=avg\|YYYY`, `top` | weekly counts per species |
| `GET /api/phenology/ridges` | `species`, `period` | mean daily counts per season (365 days, latest year alongside), and medians |
| `GET /api/phenology/season-to-date` | `species`, `year` | mean curve, SD band, top and worst years, current year |
| `GET /api/phenology/table` | `period` | per-species rows (2025 count, average, trend in birds per year, medians) |
| `GET /api/recoveries` | `species`, `period`, `direction` | recovery points, summary, countries, longest finds and the species options |
| `GET /api/weather` | `season`, `year`, `species` | daily weather, daily catch, and catch per wind sector |

**As built in phase 3** (live docs at `/api/docs`; module notes in `.github/copilot-instructions.md` §6):
- `from`/`to` are replaced by `period=<id>` (presets only). Sammanfattning also accepts `year=YYYY` instead, for the design's single-year option; `grain=day` needs it.
- Lists that put selected species first take the selection as `species=` (`top-species`, `trends/overview`, `phenology/calendar`, `phenology/table`).
- `trends/overview` takes `min_total` (default 0) to leave out species with few birds; the page decides the cut-off.
- `phenology/calendar` ranks species by mean annual total over the first period's non-gap years, so the list doesn't change with the year; non-selected rows are ordered by spring median, then autumn-only species by autumn median. `year=` rejects gap years.
- `phenology/season-to-date` takes one species. Reference years are the earlier complete years without gap years; the ±1 SD band is the per-day SD of cumulative counts. The "idag" marker is drawn by the frontend; `data_through_doy` says where the data ends.
- `phenology/table`: "Snitt" is the mean of the 10 years before the period's last year, gap years removed (`avg_years` lists them).
- `weather` reads `agg_weather_daily` (Swedish local dates, vector-mean wind, Vinga flags). The wind rose averages the catch over days when the station was active (any ringing or recapture that day).
- `/api/health` for container health checks.

## 7. Frontend

- Vite, React, TypeScript, React Router (one route per page) and TanStack Query for fetching and caching.
- **Shared state:** the selected species (max 6, colours by order), the period and the season, all kept in the URL so links can be shared.
- **Shared components:** `Nav`, `Footer`, `SpeciesPicker` (search by name, code or scientific name), `SegmentedControl`, `ChartCard` (with loading, empty and error states, as in `design/components/Tillstand.dc.html`), `TrendBadge`, `Legend`.
- **Tokens:** generate CSS variables from `design/tokens.json`.
- **Accessibility:** real buttons and links, labels on inputs, 44 px touch targets, `aria-label` on icon-only buttons, text contrast of at least 4.5:1 (the palette already passes). Charts need a text alternative, such as the readout panels and tables in the design.
- **Responsive:** every page must work at phone width. Wide tables and charts scroll inside their card.

**As built in phase 4** (code in `web/`, details in `.github/copilot-instructions.md` §7b):
- Pinned to the newest release of the majors in common use: React 19.2, React Router 7, Vite 7, TypeScript 5.9, Vitest 4 (3 had critical advisories), Playwright 1.65.
- Routes: `/`, `/sammanfattning`, `/trender`, `/morfometri`, `/fenologi`, `/aterfynd`, `/vader`, `/om`, plus a Swedish not-found page. FastAPI serves `web/dist` and sends `index.html` for every other non-API path.
- URL state: `arter`, `period`, `sasong`. The menu carries them between pages. Each page has its own default and its own fallback when the selection is emptied, as in the design (Sammanfattning: ALLA; Trender and Fenologi: the configured default, fallback RÖHAK; Morfometri: RÖHAK, ÄNPIP, SKPIP). Defaults are not written to the URL.
- Fonts are bundled (`@fontsource`) instead of loaded from Google Fonts, so visitors' browsers don't contact Google.
- Additions to the design for accessibility: skip link, focus moves to the page heading after navigation, the species picker closes on Escape and outside click and returns focus, 44 px hit areas around the design's 40 px controls.
- On phones the menu scrolls sideways and the selection bar is not sticky (it wraps to about a quarter of the screen).
- The species picker lists ALLA and every species with new ringings (codes seen only as recaptures are left out); codes without a Swedish name are listed by code.

**As built in phase 5** (one PR per page):
- *Sammanfattning*: periods are the config presets plus the last complete year (`?period=2025`, sent to the API as `year=`). Dag needs a single year (falls back to Vecka), and År is disabled for a single year (it would be one bar; falls back to Månad). Grain and chart type are page state, not URL state. Gap years are hatched in every grain, drawn lighter as bars, and named in the footnote. Recaptures are an optional dashed series ("Visa kontroller"), never added to ringings. The readout can be driven with the arrow keys (the chart is an ARIA slider). The top list shows 100 rows with "Visa alla"; once 6 species are selected the other rows are disabled. The species tile shows every ringed species code (210 today); the wording is still to settle with Hem. Unselected bars use the design's #B4C4B8 as a `color-mix` of tokens, because `design/` is read-only.
- *Trender*: one `/api/trends` call feeds the comparison chart and the species cards. Gap years are hatched, drawn as lighter bars in the cards, and named in a caption generated from the period, gap years and current year ("Trenderna bygger på hela säsonger 1980–1989 samt 2005–2025 …"). The species cards also show the CI and first/last year labels (the design has neither). "Alla arter" leaves out species with fewer than 100 new ringings in the period and season (`min_total=100`), except selected ones, and has a "Visa alla arter" button. Rows are disabled once 6 species are selected. On phones the unit moves to the column header and only the outer ticks are shown. The comparison chart can be read with the keyboard.
- *Fenologi*: week charts (calendar, ridgelines, season-to-date) show the station's **active weeks** instead of the design's fixed weeks 11–46. That is `/api/meta` `active_weeks`: the first and last week in which the all-species mean catch is at least 0.1 % of the mean annual total (`ACTIVE_WEEK_SHARE` in `store.py`; weeks 10–47 today). It is a display window, not a season boundary: the station also catches in summer, so vår (weeks 10–26) and höst (27–47) panels meet. The calendar offers "Snitt alla år" plus every non-gap year and marks where the per-season colour scale switches. The calendar names stay pinned while it scrolls sideways on phones. Ridgeline panels for the minor season of a single-season species read "Ringmärks nästan bara på hösten/våren", and medians show "för få fåglar" when no year reaches `median_min_birds`. Season-to-date covers the current year only. It says how far the data goes ("till och med 9 maj"), puts the "idag" marker at today's date, and can be read with the pointer or keyboard. The table colours trends by verdict (certain up/down), not by sign, and shows 10 rows with "Visa alla".
- *Morfometri*: the page reads `/api/morphometrics/means` (bubble chart) and one `/detail` call for the selected species (key figures, distributions, age and sex). Season is Vår / Höst / Båda (default Höst) and there is no period choice (all years). Bubble axes are log scales fitted to the data (1-2-5 ticks, at most 8), area by the log of the count since 1980; selected species are coloured and drawn last, the rest grey; a click adds or removes a species (disabled at 6; only chosen bubbles are in the tab order, keyboard users use the picker). Species with fewer than `min_n` (10) weighed and measured birds are not in the chart, and a selected one says so under it. The distribution cards show **all selected species (up to 6)**, not the design's first three. Histograms read the API's bins (50, or one per measurement step) with a median line and a per-bar hover. Age is the raw codes in a stack ordered youngest first, coloured along the design's four greens (`ageRamp` in `tokens.ts`, from `age_colors`); missing = "Okänd". Rare sex codes (`N`, `G`, `W`) are shown by their raw code in grey tints, because their meaning is not documented. "Alla arter" has no measures: the detail cards say so. On phones the species name sits above its two charts. **Open:** the meaning of the age and sex codes should be confirmed with the station and explained on Om.
- *Återfynd*: own controls (species select with counts, config period presets, direction Båda / Utgående / Inkommande, "Visa linjer"), kept in the URL as `art`, `period` and `riktning`; the species picker of the other pages does not apply. The globe is a canvas (about 6 000 finds stay smooth) with `d3-geo` and Natural Earth 1:110m land from the `world-atlas` package, loaded on demand (55 kB). Drag turns it, hover reads a find, a click or tap picks it for the dark panel, and the arrow keys and +/− work from the keyboard. Great-circle lines are left out while the globe moves when there are more than 1 200 finds. Green = ringed here, found there (the point is the find site); red = ringed there, caught here (the point is the ringing site). **Decision to review:** the period filters on the find year, and a preset that ends before the newest find is extended to it (`/api/recoveries` returns the effective `period.year_to`, today 2026), so finds from the current year are included although the preset label says 1980–2025; the caption under the globe gives the real range and "till och med 8 apr 2026". The utgående/inkommande tiles ignore the direction choice and say so. `species_options` in the response lists the species with finds in the period and direction (not narrowed by the chosen species). Place names are shown as stored (often upper case).
- *Väder*: own controls (Vår / Höst from config, year, "Fångst av" species, and the "Visa" chips), in the URL as `sasong`, `ar`, `art` and `visa` (nothing written for the defaults: temperature, wind, direction, pressure). The year list runs over the years with weather (`/api/meta` `weather_years`, 1982 to now); partly staffed years can be picked and are named "(delvis bemannad)". The catch is bars with the days the station did not work shaded; the other tracks share one crosshair and one reader (pointer, or the arrow keys as an ARIA slider). The dark readout shows the hovered day, by default the day with the most birds. Weather covers only the catch window (03–12) and the note from `/api/meta` is printed under the tracks; precipitation is the sum for that window, not per day. Values filled in from Vinga A (precipitation, pressure, visibility, from 2007) are marked on the track ("delvis Vinga A", "Vinga A (vald dag)") and in the readout. **Design deviation:** the wind arrows point where the wind blows *to* (the design rotated them the other way while labelling them "vart det blåser"). The wind rose uses a square-root scale, shows the best direction in the trend colour, and a direction needs at least 3 days (`MIN_DAYS` in `RoseCard.tsx`) to count as the best one; thinner sectors are drawn paler. Wedges read on hover.
- *Hem*: the aerial photo (`src/assets/nidingen-hero.jpg`) with the glass card, the intro, four key figures and one entry card per page. Everything that is a number or a year comes from the API: the start year (`first_year`), "46 år" (complete years: `complete_through − first_year + 1`, the running year is not counted), and the figures from `/api/summary/kpis`, rounded to thousands with a tilde (`formatApprox`: ~396 000, ~54 000, ~6 000). **Wording settled:** the species tile reads "210 unika arter och raser" (the design's "133" is only the mapped names): 210 ringed species codes include hybrids, groups and subspecies, 209 with a Swedish name. Sammanfattning uses the same label. **Design deviation:** the glass card is more opaque (white 0.72 → 0.6 instead of 0.42 → 0.2); on the design's tint the worst pixel behind the subtitle was 2.7:1, now at least 5.5:1 (WCAG AA 4.5:1, measured in the e2e test). Without `backdrop-filter` the card falls back to a nearly solid white.
- *Om*: intro, "Källor", contact card with the Nidingen and GOF logos, a glossary and a one-at-a-time FAQ (first answer open). Every number, year and season boundary is generated from the API (`pages/om/content.ts`, tested): the material sentence (rounded to thousands), the gap-year question ("Varför saknas 1991–2004 i trenderna?", reason from config), the running-year question (only while `current_year > complete_through`), the compare limit, the species count, the season months and the build date. The weeks text is the decided wording from `design/README.md`; the weather-hours answer is `weather_window.note` itself, one source. Added beyond the design: Källor, "Ringmärkta", "Vecka" and "Ålderskoder" in the glossary, and FAQ entries on the partly staffed years, weeks, weather hours, "unika arter och raser", update frequency and personal data (none shown). The design's 1K / 2K glossary entry is replaced by the raw age codes. **Open, to confirm with the station:** the age-code wording ("10 är född i år (1K)", "20 föddes i fjol", "1+ fullvuxen, okänd ålder") follows common Swedish practice but is not verified; the meaning of the rare sex codes `N`, `G`, `W` is not explained. The contact links are real (`mailto:`, nidingen.gof.nu); the "report a found ring" text links only to nrm.se because no current reporting page was found, so add the direct address when known.

## 8. Feature parity checklist

| Current Dash tab | New page | Notes |
| --- | --- | --- |
| Hem | Hem | Hero photo with a glass card, key figures, entry cards with black-and-white icons |
| Sammanfattning | Sammanfattning | Day/week/month/year; bar or line, with line the default for day and week; top 100 species with selected species first |
| (new) | Trender | Counts per year for the selected species with fit and band, per-species cards, and the all-species overview in birds per year |
| Morfometri | Morfometri | Bubble chart of species means (log axes, size by log of the count, other species in grey); 50-bin histograms; age and sex. **Fat score is removed.** |
| Fenologi + Värmekarta | Fenologi | Order: Flyttningskalendern (former Värmekarta) at the top with its own controls, the species bar, ridgelines (two seasons, raw counts, an axis per species), the whole-year season-to-date chart, then the species table with "Visa alla". **No season toggle on this page.** |
| Väderanalys | Väder | Season (Vår / Höst, the site seasons) and year pickers, chosen variables as stacked tracks with a shared crosshair, a wind rose, and the best days |
| Återfynd | Återfynd | Draggable globe, direction filter, lines, summary, longest recoveries |
| Om websidan | Om | Text, glossary and FAQ |

Keep everything the old app supports that the design does not explicitly drop. Ask before removing anything else.

## 9. Phases (one PR each)

1. **Plan.** Read the repo, `.github/copilot-instructions.md`, this file, `config/` and `design/`. Propose the concrete plan and answer or raise the open questions. No code yet.
2. **Data layer.** `build_aggregates.py`, the tables in §4.2 and the tests in §4.4. The Dash app still runs.
3. **API.** FastAPI app with the endpoints in §6, pytest coverage, and parity checks against the current Dash queries where they overlap.
4. **Frontend shell.** Vite app, tokens, routing, Nav, Footer, species picker, URL state, chart card states. FastAPI serves the built files.
5. **Pages,** in this order: Sammanfattning, Trender, Fenologi, Morfometri, Återfynd, Väder, Hem, Om. One PR per page, checked against its design file.
6. **Ship.** Dockerfile (multi-stage: node build, then a Python runtime), CI workflow, deploy to Serve, and a smoke test against the real database.
7. **Remove Dash.** Delete the Dash app, Plotly and Dash Bootstrap Components, unused query code and the `TOTAL` duplication. Update the README and `CLAUDE.md`.

The Dash app stays runnable until phase 7.

## 10. Definition of done

- Every page matches its design file in layout, colours, copy and behaviour, using real data.
- Every rule in §1 and §5 is implemented and covered by a test, either data or unit.
- `docker run` against the real database serves the whole site from one container. Pages respond in under 300 ms with a warm cache.
- The README describes the new data pipeline, including the `build_aggregates.py` step after each new season.

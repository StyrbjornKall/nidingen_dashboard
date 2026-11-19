# Design reference

These files are the approved visual and interaction design for the new dashboard. They come from a design canvas, not from a React codebase. **Use them as a reference for layout, styling, copy and chart behaviour. Do not copy them into the app.**

## What is in here

| Folder | Content |
| --- | --- |
| `pages/` | One file per page: Hem, Sammanfattning, Trender, Morfometri, Fenologi, Återfynd, Väder, Om |
| `components/` | Shared pieces: `Nav`, `Footer`, `Artvaljare` (species picker), `Tillstand` (loading, empty and error states). `Palett-Ejder` and `Palettprov` show the colour palette with its contrast checks. |
| `assets/` | Hero photo and station logo. `coastlines-simplified-PLACEHOLDER.json` is a hand-drawn low-resolution coastline used only in the sketch. **Replace it with Natural Earth 1:110m land (TopoJSON)** in the real app. |
| `tokens.json` | Colours, species palette, fonts, radii and layout values. |

## How to read a `.dc.html` file

Each file has two parts:

1. **Markup inside `<x-dc> … </x-dc>`.** HTML with inline styles. `{{name}}` is a placeholder filled from the script, `<sc-for list="{{items}}" as="x">` is a loop, `<sc-if value="{{flag}}">` is a conditional, and `<dc-import name="Nav">` mounts another file from `components/`.
2. **A script at the bottom (`class Component extends DCLogic`).** `renderVals()` computes everything the markup shows. This is where the chart maths lives: regression with 95 % confidence band, cumulative season curves with a ±1 SD band, top and worst year, median passage days, per-season normalisation of the migration calendar, histograms, and the selected-first ordering.

## The data in these files is illustrative

Every chart is driven by a seeded random generator (`series()`, `rate()`, `birds()` and so on), not real data. **Port the formulas and behaviour, never the generated numbers.** Only the key figures on Hem and Sammanfattning (~396 000 ringed, ~54 000 recaptures, ~6 000 recoveries, 133 species) are real.

## Where the design files are out of date

`../HANDOFF.md` and `../config/analysis.json` win over these files. Known differences:

- **Seasons.** The sketch uses week windows (vår weeks 11–24, höst weeks 28–46). The decision is calendar months: **vår = januari–juni, höst = juli–december**.
- **Years left out of trends.** The sketch has no gap handling, and two captions hard-code "1980–1995 samt 2005–2025". The decision is that **1991–2004 are excluded from all trends and multi-year statistics**, because the station was only partly staffed. See HANDOFF.md, "Gap years".
- **Single-season species.** The sketch marks Kungsfågel, Rödvingetrast and Blåmes as "passes only in autumn" purely as examples. The real rule: a species with more than 90 % of its new ringings in one season gets that season only (`single_season_threshold` in `../config/analysis.json`).
- **Flyttningskalendern year list.** Offer exactly the years outside the configured gap range.
- **Väder seasons.** The Väder page uses Vår / Sommar / Höst. Decided: it uses the two site-wide seasons, Vår (januari–juni) and Höst (juli–december).
- **Today marker.** The "idag" date in Fenologi is fixed in the sketch (18 nov 2026). In the app it is the real current date.
- **Globe.** Återfynd loads d3 from a canvas asset URL. In the app, install `d3-geo` from npm.

## Design deviations decided in phase 1

Apply these when building each page. HANDOFF.md §2 and §2b hold the reasoning.

- **Seasons** come from `../config/analysis.json` (calendar months), never from the sketch's day or week windows. The migration calendar normalises per season on that boundary; a week belongs to the month of its Thursday.
- **Gap years 1991–2004** are drawn hatched or lighter in raw series, with a "delvis bemannad" note. They are left out of fits, means, the SD band, top and worst year, ridgeline means, "Snitt" columns and the calendar's "Snitt alla år", and from year pickers that feed statistics. Trend charts must cope with the missing years on the x axis.
- **Captions are generated** from config and data, not hard-coded: "1980–1989 samt 2005–2025", "Snitt …", table column headers, "Säsongen {år}", "46 år", "sedan 1980", the Hem key figures, and FAQ years.
- **Confidence intervals** use the exact t quantile (`df = n − 2`), computed in the API. The Fenologi table trend uses the selected period, not always 1980–2025.
- **Season-to-date ±1 SD band** is the per-day standard deviation of the cumulative counts across years, not the sketch's scaled approximation.
- **Period presets only** (`period_options`); there is no free year range. Sammanfattning's and Återfynd's presets also come from config.
- **Morfometri bubble chart** axes are derived from the data, not fixed at 44–150 mm and 4–130 g. The "Ålder" bar shows the **raw age codes** (`10`, `2+`, `20`, `3+` …), not 1K / 2K / +1K / +2K; `age_colors` in `tokens.json` needs a ramp over the codes instead.
- **Återfynd**: the period filters on find date; "medianavstånd" is the true median; tiles that ignore the direction filter say so; countries come from a EURING code lookup.
- **Väder**: keeps the "Visa" variable chips and marks values gap-filled from Vinga A (precipitation, pressure, visibility).
- **Daily weather covers 03:00–12:00 Swedish time only**, the hours when birds are caught, not the whole day. This applies to every weather value on the site (tracks, readout, wind rose, best days). Show the note from `/api/meta` (`weather_window.note`, from `config/analysis.json`) as a caption on Väder, and add it to Om:

  > **Vilka timmar gäller väderuppgifterna?**
  > Väderuppgifterna för en dag gäller bara kl. 03–12 svensk tid, då de flesta fåglarna fångas, inte hela dygnet. Temperatur, vind, lufttryck, sikt och molnighet är medelvärden (eller högsta och lägsta värde) av SMHI:s timvärden under de timmarna, och nederbörden är summan för samma tid. Dagens vindriktning är ett medelvärde där timmar med starkare vind väger tyngre.
- **Species picker**: species codes without a Swedish name are selectable, shown by code.
- **Om**: add an FAQ entry on the partly staffed years 1991–2004, and replace the `claudeusercontent.com` contact links with real ones.
- **Weeks are 7-day periods from 1 January, never calendar (ISO) week numbers**, everywhere on the site (Sammanfattning "Vecka", Flyttningskalendern, ridgelines). Labels such as "v.12" stay, but hover readouts should also show the dates ("v.12 · 19–25 mar"). Om must explain this with the text below.

  FAQ entry for Om:

  > **Varför stämmer inte veckorna med almanackan?**
  > När data visas per vecka använder vi inte kalenderveckor. En vecka här är en period på sju dagar räknad från den 1 januari: vecka 1 är 1–7 januari, vecka 2 är 8–14 januari och så vidare. Flyttfåglarna bryr sig inte om veckonummer, och på det här sättet hamnar samma datum i samma vecka varje år, så att åren går att jämföra. Vecka 12 på webbplatsen är alltså alltid 19–25 mars, inte kalendervecka 12. Vecka 52 omfattar årets sista åtta dagar (24–31 december), och skottdagen 29 februari räknas till samma vecka som 28 februari.

  Glossary entry for Om: **Vecka** — sju dagar räknat från 1 januari (vecka 1 = 1–7 januari), inte kalendervecka.
- **Footer** "Senast uppdaterad" and the "Data från …" labels read a build timestamp written by `build_aggregates.py`.

## Asset URLs in the files

The files reference uploaded assets by canvas URL. Map them like this:

| URL in the files | File |
| --- | --- |
| `/_blob/1edd62a10926311f281cf855ad125cc9` | `assets/nidingen-hero.jpg` |
| `/_blob/fb00db7bb89f48f4e80a38c753b00970` | `assets/nidingen-logo.png` |
| `/_blob/424ea45a3969fb9203bbeeebc8397b74` | d3 v7 (use `d3-geo` from npm instead) |

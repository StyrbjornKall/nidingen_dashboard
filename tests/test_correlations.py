"""
Correlation exploration – Bird Ringing × Weather

Generates a variety of interactive Plotly figures that explore correlations
between ringing activity / morphometric data and SMHI weather parameters.
Saved as HTML files in the ``figures/`` directory for review before
integrating into the dashboard.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── project imports ──────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from db_manager import BirdRingingDB
from query_utils import BirdRingingQueries

DB_PATH = ROOT / "data" / "bird_ringing.db"
FIG_DIR = ROOT / "figures"
FIG_DIR.mkdir(exist_ok=True)

# ── shared palette & layout helper ───────────────────────────────────────
PASTEL_COLORS = [
    "#A8C7D3",  # Pastel blue
    "#FAC39F",  # Pastel orange
    "#ADCF99",  # Pastel green
    "#F8A5B2",  # Pastel pink
    "#D5AAE2",  # Pastel purple
    "#F3D699",  # Pastel yellow
    "#A0DDDD",  # Pastel cyan
    "#F7BAD2",  # Pastel rose
    "#BCD8BC",  # Pastel mint
    "#D3BBA9",  # Pastel tan
]

SEASON_COLORS = {
    "Spring": "#C5E1B5",
    "Summer": "#FFE8B8",
    "Autumn": "#FFD4B8",
    "Winter": "#B4D4E1",
}


def _base_layout(fig, **kwargs):
    """Apply the dashboard's standard layout to a Plotly figure."""
    fig.update_layout(
        template="plotly_white",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50"),
        **kwargs,
    )
    return fig


def _season(month: int) -> str:
    if month in (3, 4, 5):
        return "Spring"
    if month in (6, 7):
        return "Summer"
    if month in (8, 9, 10, 11):
        return "Autumn"
    return "Winter"


# ══════════════════════════════════════════════════════════════════════════
# DATA LOADING – all queries executed once, then reused across figures
# ══════════════════════════════════════════════════════════════════════════

print("Loading data …")
with BirdRingingDB(DB_PATH, read_only=True) as db:

    # 1) Daily ringing counts (all species) joined with daily weather
    q_daily = BirdRingingQueries.get_weather_joined_with_ringing(
        start_date="2020-01-01",
        end_date="2024-12-31",
        weather_aggregation="daily",
    )
    daily_raw = db.execute_query(q_daily).pl().to_pandas()

    # 2) Record-level weather-at-capture (for body metric scatter plots)
    q_capture = BirdRingingQueries.get_weather_at_capture_time(
        start_date="2020-01-01",
        end_date="2024-12-31",
        max_gap_hours=2,
    )
    capture_raw = db.execute_query(q_capture).pl().to_pandas()

    # 3) Daily weather summary (standalone, for the correlation matrix)
    q_weather = BirdRingingQueries.get_daily_weather_summary(
        start_date="2020-01-01",
        end_date="2024-12-31",
    )
    weather_daily = db.execute_query(q_weather).pl().to_pandas()

    # 4) Species diversity per day
    q_diversity = """
        SELECT
            date,
            COUNT(*) AS total_obs,
            COUNT(DISTINCT species_code) AS n_species,
            COUNT(DISTINCT ring_number)  AS n_individuals
        FROM ring_records
        WHERE date >= '2020-01-01' AND date <= '2024-12-31'
        GROUP BY date
        ORDER BY date
    """
    diversity = db.execute_query(q_diversity).pl().to_pandas()

print(f"  daily_raw:    {len(daily_raw):,} rows")
print(f"  capture_raw:  {len(capture_raw):,} rows")
print(f"  weather_daily:{len(weather_daily):,} rows")
print(f"  diversity:    {len(diversity):,} rows")


# ── derived tables ───────────────────────────────────────────────────────

# Aggregate daily_raw to one row per date (sum captures across species)
daily_all = (
    daily_raw.groupby("date")
    .agg(
        captures=("captures", "sum"),
        mean_temperature=("mean_temperature", "first"),
        min_temperature=("min_temperature", "first"),
        max_temperature=("max_temperature", "first"),
        mean_wind_speed=("mean_wind_speed", "first"),
        mean_visibility=("mean_visibility", "first"),
        max_gust=("max_gust", "first"),
        mean_humidity=("mean_humidity", "first"),
        total_precipitation=("total_precipitation", "first"),
        mean_pressure=("mean_pressure", "first"),
        mean_cloud_cover=("mean_cloud_cover", "first"),
    )
    .reset_index()
)
daily_all["date"] = pd.to_datetime(daily_all["date"])
daily_all["month"] = daily_all["date"].dt.month
daily_all["season"] = daily_all["month"].map(_season)
daily_all["year"] = daily_all["date"].dt.year

# Weekly aggregation
daily_all["week_start"] = daily_all["date"].dt.to_period("W").apply(lambda x: x.start_time)
weekly = (
    daily_all.groupby("week_start")
    .agg(
        captures=("captures", "sum"),
        mean_temperature=("mean_temperature", "mean"),
        mean_wind_speed=("mean_wind_speed", "mean"),
        mean_visibility=("mean_visibility", "mean"),
        max_gust=("max_gust", "max"),
        mean_humidity=("mean_humidity", "mean"),
        total_precipitation=("total_precipitation", "sum"),
        mean_pressure=("mean_pressure", "mean"),
        mean_cloud_cover=("mean_cloud_cover", "mean"),
        month=("month", "first"),
    )
    .reset_index()
)
weekly["season"] = weekly["month"].map(_season)

# Diversity merged with weather
diversity["date"] = pd.to_datetime(diversity["date"])
weather_daily["date"] = pd.to_datetime(weather_daily["date"])
div_weather = diversity.merge(weather_daily, on="date", how="inner")
div_weather["month"] = div_weather["date"].dt.month
div_weather["season"] = div_weather["month"].map(_season)


# ══════════════════════════════════════════════════════════════════════════
# FIGURE 1 – Weekly captures vs. weather scatterplots (2 × 3 grid)
# ══════════════════════════════════════════════════════════════════════════

print("Figure 1 – weekly captures vs weather scatter grid …")

weather_vars = [
    ("mean_temperature", "Mean Temperature (°C)"),
    ("mean_wind_speed", "Mean Wind Speed (m/s)"),
    ("total_precipitation", "Total Precipitation (mm)"),
    ("mean_visibility", "Mean Visibility (m)"),
    ("mean_humidity", "Mean Humidity (%)"),
    ("mean_cloud_cover", "Mean Cloud Cover (%)"),
    ("mean_pressure", "Mean Pressure (hPa)"),
]

fig1 = make_subplots(
    rows=2, cols=4,
    subplot_titles=[v[1] for v in weather_vars],
    horizontal_spacing=0.06,
    vertical_spacing=0.12,
)

for idx, (col, label) in enumerate(weather_vars):
    row = idx // 4 + 1
    col_pos = idx % 4 + 1
    color = PASTEL_COLORS[idx % len(PASTEL_COLORS)]

    sub = weekly.dropna(subset=[col, "captures"])
    if sub.empty:
        continue

    # colour per season
    for season, sc in SEASON_COLORS.items():
        ss = sub[sub["season"] == season]
        if ss.empty:
            continue
        fig1.add_trace(
            go.Scatter(
                x=ss[col],
                y=ss["captures"],
                mode="markers",
                marker=dict(size=6, color=sc, opacity=0.7,
                            line=dict(width=0.5, color="white")),
                name=season,
                legendgroup=season,
                showlegend=(idx == 0),
                hovertemplate=(
                    f"<b>{label}</b>: %{{x:.1f}}<br>"
                    "Captures: %{y:,}<br>"
                    f"Season: {season}<extra></extra>"
                ),
            ),
            row=row, col=col_pos,
        )

    # OLS trendline
    mask = sub[[col, "captures"]].dropna()
    if len(mask) > 5:
        z = np.polyfit(mask[col], mask["captures"], 1)
        x_range = np.linspace(mask[col].min(), mask[col].max(), 50)
        fig1.add_trace(
            go.Scatter(
                x=x_range,
                y=np.polyval(z, x_range),
                mode="lines",
                line=dict(color="rgba(100,100,100,0.6)", width=2, dash="dash"),
                showlegend=False,
                hoverinfo="skip",
            ),
            row=row, col=col_pos,
        )

    fig1.update_xaxes(title_text=label, title_font_size=10, row=row, col=col_pos)
    fig1.update_yaxes(title_text="Weekly Captures" if col_pos == 1 else "", row=row, col=col_pos)

_base_layout(
    fig1,
    height=700,
    width=1200,
    title_text="Weekly Captures vs Weather Parameters (2020–2024)",
    legend=dict(orientation="h", yanchor="bottom", y=1.04, xanchor="center", x=0.5),
)
fig1.write_html(str(FIG_DIR / "correlation_01_weekly_scatter_grid.html"))
print("  ✓ saved")


# ══════════════════════════════════════════════════════════════════════════
# FIGURE 2 – Bubble chart: daily captures × temperature × wind speed
# ══════════════════════════════════════════════════════════════════════════

print("Figure 2 – bubble chart (temperature × wind × captures) …")

bubble = daily_all.dropna(subset=["mean_temperature", "mean_wind_speed", "captures"])
bubble = bubble[bubble["captures"] > 0]

fig2 = px.scatter(
    bubble,
    x="mean_temperature",
    y="mean_wind_speed",
    size="captures",
    color="season",
    color_discrete_map=SEASON_COLORS,
    size_max=35,
    opacity=0.65,
    hover_data={
        "date": True,
        "captures": True,
        "mean_temperature": ":.1f",
        "mean_wind_speed": ":.1f",
    },
    labels={
        "mean_temperature": "Mean Temperature (°C)",
        "mean_wind_speed": "Mean Wind Speed (m/s)",
        "captures": "Daily Captures",
        "season": "Season",
    },
    title=(
        "Daily Captures by Temperature & Wind Speed (2020–2024)"
        "<br><sub>Bubble size = number of captures</sub>"
    ),
)

_base_layout(
    fig2,
    height=600,
    width=900,
    legend=dict(
        orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5,
        bgcolor="rgba(255,255,255,0.8)", bordercolor="#dee2e6", borderwidth=1,
    ),
)
fig2.write_html(str(FIG_DIR / "correlation_02_bubble_temp_wind.html"))
print("  ✓ saved")


# ══════════════════════════════════════════════════════════════════════════
# FIGURE 3 – Box plots: daily captures binned by weather categories
# ══════════════════════════════════════════════════════════════════════════

print("Figure 3 – boxplots captures by weather bins …")

df_box = daily_all[daily_all["captures"] > 0].copy()

# Create binned categories
df_box["wind_cat"] = pd.cut(
    df_box["mean_wind_speed"],
    bins=[0, 3, 6, 10, 50],
    labels=["Calm (0–3)", "Light (3–6)", "Moderate (6–10)", "Strong (>10)"],
)
df_box["temp_cat"] = pd.cut(
    df_box["mean_temperature"],
    bins=[-20, 0, 5, 10, 15, 40],
    labels=["< 0°C", "0–5°C", "5–10°C", "10–15°C", "> 15°C"],
)
df_box["cloud_cat"] = pd.cut(
    df_box["mean_cloud_cover"],
    bins=[-1, 25, 50, 75, 101],
    labels=["Clear (0–25%)", "Partly (25–50%)", "Mostly (50–75%)", "Overcast (>75%)"],
)
df_box["precip_cat"] = pd.cut(
    df_box["total_precipitation"].fillna(0),
    bins=[-0.1, 0, 1, 5, 200],
    labels=["Dry (0 mm)", "Light (0–1 mm)", "Moderate (1–5 mm)", "Heavy (>5 mm)"],
)

fig3 = make_subplots(
    rows=2, cols=2,
    subplot_titles=[
        "Captures by Wind Category",
        "Captures by Temperature Category",
        "Captures by Cloud Cover",
        "Captures by Precipitation",
    ],
    horizontal_spacing=0.10,
    vertical_spacing=0.14,
)

box_configs = [
    ("wind_cat", 1, 1, PASTEL_COLORS[1]),
    ("temp_cat", 1, 2, PASTEL_COLORS[0]),
    ("cloud_cat", 2, 1, PASTEL_COLORS[6]),
    ("precip_cat", 2, 2, PASTEL_COLORS[2]),
]

for cat_col, row, col_pos, color in box_configs:
    sub = df_box.dropna(subset=[cat_col])
    categories = sub[cat_col].cat.categories.tolist()

    for i, cat in enumerate(categories):
        cat_data = sub[sub[cat_col] == cat]["captures"]
        r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
        # Vary brightness per bin
        factor = 0.7 + 0.3 * (i / max(len(categories) - 1, 1))
        adj_color = f"rgba({int(r*factor)},{int(g*factor)},{int(b*factor)},0.7)"

        fig3.add_trace(
            go.Box(
                y=cat_data,
                name=str(cat),
                marker_color=adj_color,
                showlegend=False,
                hoverinfo="y+name",
            ),
            row=row, col=col_pos,
        )

    fig3.update_yaxes(title_text="Daily Captures", row=row, col=col_pos)

_base_layout(
    fig3,
    height=750,
    width=1100,
    title_text=(
        "How Weather Conditions Affect Daily Capture Counts (2020–2024)"
        "<br><sub>Boxes show median, IQR; diamonds show mean</sub>"
    ),
)
fig3.write_html(str(FIG_DIR / "correlation_03_boxplots_weather_bins.html"))
print("  ✓ saved")


# ══════════════════════════════════════════════════════════════════════════
# FIGURE 4 – Correlation matrix heatmap: weather + ringing metrics
# ══════════════════════════════════════════════════════════════════════════

print("Figure 4 – correlation matrix heatmap …")

# Build a single daily table with all variables
corr_df = daily_all[
    [
        "captures",
        "mean_temperature",
        "mean_wind_speed",
        "max_gust",
        "mean_visibility",
        "mean_humidity",
        "total_precipitation",
        "mean_pressure",
        "mean_cloud_cover",
    ]
].rename(columns={
    "captures": "Daily Captures",
    "mean_temperature": "Temperature",
    "mean_wind_speed": "Wind Speed",
    "max_gust": "Max Gust",
    "mean_visibility": "Visibility",
    "mean_humidity": "Humidity",
    "total_precipitation": "Precipitation",
    "mean_pressure": "Pressure",
    "mean_cloud_cover": "Cloud Cover",
})

corr_matrix = corr_df.corr(numeric_only=True)

fig4 = go.Figure(
    data=go.Heatmap(
        z=corr_matrix.values,
        x=corr_matrix.columns,
        y=corr_matrix.index,
        zmin=-1, zmax=1,
        colorscale=[
            [0.0, "#B4D4E1"],   # negative = blue
            [0.5, "#FFFFFF"],   # zero = white
            [1.0, "#FFB8C3"],   # positive = pink
        ],
        text=np.round(corr_matrix.values, 2),
        texttemplate="%{text}",
        textfont=dict(size=12),
        hoverongaps=False,
        hovertemplate="%{x} × %{y}<br>r = %{z:.3f}<extra></extra>",
        colorbar=dict(title="Pearson r", thickness=15),
    )
)

_base_layout(
    fig4,
    height=600,
    width=700,
    title_text=(
        "Correlation Matrix: Ringing Activity & Weather (2020–2024)"
        "<br><sub>Pearson correlation on daily values</sub>"
    ),
    xaxis=dict(tickangle=-40),
)
fig4.write_html(str(FIG_DIR / "correlation_04_heatmap_matrix.html"))
print("  ✓ saved")


# ══════════════════════════════════════════════════════════════════════════
# FIGURE 5 – Body metrics vs weather at capture time (scatter)
# ══════════════════════════════════════════════════════════════════════════

print("Figure 5 – individual body metrics vs weather …")

cap = capture_raw.copy()
cap = cap[cap["weather_match_hours"].notna() & (cap["weather_match_hours"] <= 2)]
cap["month"] = pd.to_datetime(cap["date"]).dt.month
cap["season"] = cap["month"].map(_season)

# Top 6 species by count for clarity
top_species = cap["swedish_name"].value_counts().head(6).index.tolist()
cap_top = cap[cap["swedish_name"].isin(top_species)]

fig5 = make_subplots(
    rows=2, cols=2,
    subplot_titles=[
        "Fat Score vs Temperature",
        "Weight vs Temperature",
        "Fat Score vs Wind Speed",
        "Weight vs Wind Speed",
    ],
    horizontal_spacing=0.10,
    vertical_spacing=0.14,
)

combos = [
    ("temperature", "fat_score", 1, 1),
    ("temperature", "weight", 1, 2),
    ("wind_speed", "fat_score", 2, 1),
    ("wind_speed", "weight", 2, 2),
]

for wx, bio, row, col_pos in combos:
    sub = cap_top.dropna(subset=[wx, bio])
    if bio == "fat_score":
        sub = sub[(sub[bio] >= 0) & (sub[bio] <= 10)]
    if bio == "weight":
        sub = sub[sub[bio] > 0]
    if sub.empty:
        continue

    for i, species in enumerate(top_species):
        ss = sub[sub["swedish_name"] == species]
        if ss.empty:
            continue
        color = PASTEL_COLORS[i % len(PASTEL_COLORS)]
        fig5.add_trace(
            go.Scatter(
                x=ss[wx],
                y=ss[bio],
                mode="markers",
                marker=dict(size=4, color=color, opacity=0.5,
                            line=dict(width=0)),
                name=species,
                legendgroup=species,
                showlegend=(row == 1 and col_pos == 1),
                hovertemplate=(
                    f"<b>{species}</b><br>"
                    f"{wx}: %{{x:.1f}}<br>"
                    f"{bio}: %{{y:.1f}}<extra></extra>"
                ),
            ),
            row=row, col=col_pos,
        )

    # Trendline across all species
    mask = sub[[wx, bio]].dropna()
    if len(mask) > 10:
        z = np.polyfit(mask[wx], mask[bio], 1)
        xr = np.linspace(mask[wx].min(), mask[wx].max(), 50)
        fig5.add_trace(
            go.Scatter(
                x=xr, y=np.polyval(z, xr),
                mode="lines",
                line=dict(color="rgba(60,60,60,0.5)", width=2, dash="dash"),
                showlegend=False, hoverinfo="skip",
            ),
            row=row, col=col_pos,
        )

    x_label = "Temperature (°C)" if wx == "temperature" else "Wind Speed (m/s)"
    y_label = "Fat Score" if bio == "fat_score" else "Weight (g)"
    fig5.update_xaxes(title_text=x_label, title_font_size=10, row=row, col=col_pos)
    fig5.update_yaxes(title_text=y_label, title_font_size=10, row=row, col=col_pos)

_base_layout(
    fig5,
    height=800,
    width=1100,
    title_text=(
        "Body Condition vs Weather at Capture Time (2020–2024, top 6 species)"
        "<br><sub>Each point = one ringed bird, weather matched within 2 h</sub>"
    ),
    legend=dict(
        orientation="h", yanchor="bottom", y=1.04, xanchor="center", x=0.5,
        bgcolor="rgba(255,255,255,0.8)", bordercolor="#dee2e6", borderwidth=1,
    ),
)
fig5.write_html(str(FIG_DIR / "correlation_05_body_metrics_vs_weather.html"))
print("  ✓ saved")


# ══════════════════════════════════════════════════════════════════════════
# FIGURE 6 – Species diversity vs weather (scatter + marginal hists)
# ══════════════════════════════════════════════════════════════════════════

print("Figure 6 – species diversity vs weather …")

fig6 = make_subplots(
    rows=1, cols=4,
    subplot_titles=[
        "Diversity vs Temperature",
        "Diversity vs Wind Speed",
        "Diversity vs Visibility",
        "Diversity vs Cloud Cover",
    ],
    horizontal_spacing=0.08,
)

div_vars = [
    ("mean_temperature", "Temperature (°C)", PASTEL_COLORS[0]),
    ("mean_wind_speed", "Wind Speed (m/s)", PASTEL_COLORS[1]),
    ("mean_visibility", "Visibility (km)", PASTEL_COLORS[2]),
    ("mean_cloud_cover", "Cloud Cover (%)", PASTEL_COLORS[6]),
]

for col_idx, (wx_col, wx_label, color) in enumerate(div_vars, start=1):
    sub = div_weather.dropna(subset=[wx_col, "n_species"])
    if sub.empty:
        continue

    for season, sc in SEASON_COLORS.items():
        ss = sub[sub["season"] == season]
        if ss.empty:
            continue
        fig6.add_trace(
            go.Scatter(
                x=ss[wx_col],
                y=ss["n_species"],
                mode="markers",
                marker=dict(size=5, color=sc, opacity=0.6),
                name=season,
                legendgroup=season,
                showlegend=(col_idx == 1),
                hovertemplate=(
                    f"{wx_label}: %{{x:.1f}}<br>"
                    "Species: %{y}<br>"
                    f"Season: {season}<extra></extra>"
                ),
            ),
            row=1, col=col_idx,
        )

    # Trendline
    mask = sub[[wx_col, "n_species"]].dropna()
    if len(mask) > 10:
        z = np.polyfit(mask[wx_col], mask["n_species"], 1)
        xr = np.linspace(mask[wx_col].min(), mask[wx_col].max(), 50)
        fig6.add_trace(
            go.Scatter(
                x=xr, y=np.polyval(z, xr),
                mode="lines",
                line=dict(color="rgba(80,80,80,0.5)", width=2, dash="dash"),
                showlegend=False, hoverinfo="skip",
            ),
            row=1, col=col_idx,
        )

    fig6.update_xaxes(title_text=wx_label, row=1, col=col_idx)
    fig6.update_yaxes(
        title_text="Unique Species per Day" if col_idx == 1 else "",
        row=1, col=col_idx,
    )

_base_layout(
    fig6,
    height=450,
    width=1200,
    title_text=(
        "Species Diversity vs Weather Conditions (2020–2024)"
        "<br><sub>Each point = one active ringing day</sub>"
    ),
    legend=dict(
        orientation="h", yanchor="bottom", y=1.06, xanchor="center", x=0.5,
        bgcolor="rgba(255,255,255,0.8)", bordercolor="#dee2e6", borderwidth=1,
    ),
)
fig6.write_html(str(FIG_DIR / "correlation_06_diversity_vs_weather.html"))
print("  ✓ saved")


# ══════════════════════════════════════════════════════════════════════════
# FIGURE 7 – Dual-axis time series: weekly captures + mean temperature
# ══════════════════════════════════════════════════════════════════════════

print("Figure 7 – dual-axis weekly captures & temperature …")

fig7 = make_subplots(specs=[[{"secondary_y": True}]])

# Bar = captures
fig7.add_trace(
    go.Bar(
        x=weekly["week_start"],
        y=weekly["captures"],
        name="Weekly Captures",
        marker_color=PASTEL_COLORS[0],
        opacity=0.6,
        hovertemplate="Week: %{x|%Y-%m-%d}<br>Captures: %{y:,}<extra></extra>",
    ),
    secondary_y=False,
)

# Line = temperature
fig7.add_trace(
    go.Scatter(
        x=weekly["week_start"],
        y=weekly["mean_temperature"],
        name="Mean Temperature",
        mode="lines",
        line=dict(color=PASTEL_COLORS[3], width=3),
        hovertemplate="Week: %{x|%Y-%m-%d}<br>Temp: %{y:.1f} °C<extra></extra>",
    ),
    secondary_y=True,
)

# Line = wind speed
fig7.add_trace(
    go.Scatter(
        x=weekly["week_start"],
        y=weekly["mean_wind_speed"],
        name="Mean Wind Speed",
        mode="lines",
        line=dict(color=PASTEL_COLORS[1], width=2, dash="dot"),
        hovertemplate="Week: %{x|%Y-%m-%d}<br>Wind: %{y:.1f} m/s<extra></extra>",
    ),
    secondary_y=True,
)

# Line = mean visibility
fig7.add_trace(
    go.Scatter(
        x=weekly["week_start"],
        y=weekly["mean_visibility"],
        name="Mean Visibility",
        mode="lines",
        line=dict(color=PASTEL_COLORS[2], width=2, dash="dot"),
        hovertemplate="Week: %{x|%Y-%m-%d}<br>Visibility: %{y:.1f} km<extra></extra>",
    ),
    secondary_y=True,
)

fig7.update_yaxes(title_text="Weekly Captures", secondary_y=False)
fig7.update_yaxes(title_text="Temperature (°C) / Wind (m/s)", secondary_y=True)

_base_layout(
    fig7,
    height=500,
    width=1100,
    title_text=(
        "Weekly Captures Overlaid with Temperature & Wind (2020–2024)"
        "<br><sub>Are capture peaks aligned with favourable weather windows?</sub>"
    ),
    hovermode="x unified",
    legend=dict(
        orientation="h", yanchor="bottom", y=1.04, xanchor="center", x=0.5,
        bgcolor="rgba(255,255,255,0.8)", bordercolor="#dee2e6", borderwidth=1,
    ),
)
fig7.write_html(str(FIG_DIR / "correlation_07_dualaxis_weekly.html"))
print("  ✓ saved")


# ══════════════════════════════════════════════════════════════════════════
# FIGURE 8 – Violin: weight distribution by wind speed category
#             (top 4 species, side-by-side violins)
# ══════════════════════════════════════════════════════════════════════════

print("Figure 8 – violin plots: weight by wind category …")

cap_v = cap_top.copy()
cap_v = cap_v[cap_v["weight"].notna() & (cap_v["weight"] > 0)]
cap_v["wind_cat"] = pd.cut(
    cap_v["wind_speed"],
    bins=[0, 3, 6, 10, 50],
    labels=["Calm\n(0–3 m/s)", "Light\n(3–6 m/s)", "Moderate\n(6–10 m/s)", "Strong\n(>10 m/s)"],
)
cap_v = cap_v.dropna(subset=["wind_cat"])

top4 = cap_v["swedish_name"].value_counts().head(4).index.tolist()
cap_v4 = cap_v[cap_v["swedish_name"].isin(top4)]

fig8 = make_subplots(
    rows=1, cols=4,
    subplot_titles=top4,
    horizontal_spacing=0.06,
)

for i, species in enumerate(top4, start=1):
    ss = cap_v4[cap_v4["swedish_name"] == species]
    cats = ss["wind_cat"].cat.categories.tolist()

    for j, cat in enumerate(cats):
        cat_data = ss[ss["wind_cat"] == cat]["weight"]
        if cat_data.empty:
            continue
        color = PASTEL_COLORS[j % len(PASTEL_COLORS)]
        fig8.add_trace(
            go.Violin(
                y=cat_data,
                name=str(cat),
                legendgroup=str(cat),
                showlegend=(i == 1),
                box_visible=True,
                meanline_visible=True,
                marker_color=color,
                line_color=color,
                opacity=0.7,
                scalemode="width",
                hoverinfo="y+name",
            ),
            row=1, col=i,
        )
    fig8.update_yaxes(title_text="Weight (g)" if i == 1 else "", row=1, col=i)

_base_layout(
    fig8,
    height=500,
    width=1200,
    title_text=(
        "Weight Distribution by Wind Speed Category (top 4 species, 2020–2024)"
        "<br><sub>Does wind affect body mass of captured birds?</sub>"
    ),
    legend=dict(
        orientation="h", yanchor="bottom", y=1.06, xanchor="center", x=0.5,
        bgcolor="rgba(255,255,255,0.8)", bordercolor="#dee2e6", borderwidth=1,
    ),
)
fig8.write_html(str(FIG_DIR / "correlation_08_violin_weight_wind.html"))
print("  ✓ saved")


# ══════════════════════════════════════════════════════════════════════════
# FIGURE 9 – Hexbin-style 2D histogram: captures vs temp × wind
# ══════════════════════════════════════════════════════════════════════════

print("Figure 9 – 2D histogram captures by temp × wind …")

# Each cell = sum of captures for that (temp, wind) bin
heatbin = daily_all.dropna(subset=["mean_temperature", "mean_wind_speed", "captures"])
heatbin = heatbin[heatbin["captures"] > 0]

fig9 = go.Figure(
    go.Histogram2d(
        x=heatbin["mean_temperature"],
        y=heatbin["mean_wind_speed"],
        z=heatbin["captures"],
        histfunc="sum",
        nbinsx=25,
        nbinsy=20,
        colorscale=[
            [0.0, "#FFFFFF"],
            [0.3, "#B4D4E1"],
            [0.6, "#E0C5E8"],
            [1.0, "#FFB8C3"],
        ],
        colorbar=dict(title="Total Captures"),
        hovertemplate=(
            "Temp: %{x:.0f}°C<br>"
            "Wind: %{y:.1f} m/s<br>"
            "Total captures: %{z:,}<extra></extra>"
        ),
    )
)

_base_layout(
    fig9,
    height=550,
    width=700,
    title_text=(
        "Capture Intensity by Temperature × Wind Speed (2020–2024)"
        "<br><sub>2D histogram – colour = total captures in each bin</sub>"
    ),
    xaxis_title="Mean Daily Temperature (°C)",
    yaxis_title="Mean Daily Wind Speed (m/s)",
)
fig9.write_html(str(FIG_DIR / "correlation_09_2dhist_temp_wind.html"))
print("  ✓ saved")


# ══════════════════════════════════════════════════════════════════════════
# FIGURE 10 – Scatter: Observation intensity vs visibility
# ══════════════════════════════════════════════════════════════════════════

print("Figure 10 – captures vs visibility scatter …")

vis = div_weather.dropna(subset=["mean_visibility", "total_obs"])
vis = vis[vis["total_obs"] > 0]

fig10 = px.scatter(
    vis,
    x="mean_visibility",
    y="total_obs",
    color="season",
    color_discrete_map=SEASON_COLORS,
    opacity=0.6,
    trendline="ols",
    labels={
        "mean_visibility": "Mean Visibility (m)",
        "total_obs": "Daily Total Observations",
        "season": "Season",
    },
    title=(
        "Daily Observations vs Visibility (2020–2024)"
        "<br><sub>Does visibility drive capture effort or bird activity?</sub>"
    ),
)

# Style the OLS lines
for trace in fig10.data:
    if trace.mode == "lines":
        trace.line.dash = "dash"
        trace.line.width = 2

_base_layout(
    fig10,
    height=500,
    width=800,
    legend=dict(
        orientation="h", yanchor="bottom", y=1.04, xanchor="center", x=0.5,
        bgcolor="rgba(255,255,255,0.8)", bordercolor="#dee2e6", borderwidth=1,
    ),
)
fig10.write_html(str(FIG_DIR / "correlation_10_captures_vs_visibility.html"))
print("  ✓ saved")


print(f"\n{'='*60}")
print(f"All figures saved to {FIG_DIR.resolve()}")
print(f"{'='*60}")

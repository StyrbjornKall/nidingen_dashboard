"""
Bird Ringing Dashboard - Main Application

This is the main Dash application file for the bird ringing data dashboard.
It provides interactive visualizations for exploring bird observation data.
"""

import os
import json

import pandas  # noqa: F401 – must be imported first to avoid partial-init errors with pandas 3.x
import dash
from dash import dcc, html, Input, Output, callback, State
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
import polars as pl
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv()

# Load translation locales
locales_path = Path(__file__).parent / "assets" / "locales.json"
with open(locales_path, "r", encoding="utf-8") as f:
    LOCALES = json.load(f)

# Add src to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from db_manager import BirdRingingDB
from query_utils import BirdRingingQueries
from data_processor import BirdDataProcessor

# Pastel Color Palette
PASTEL_COLORS = [
    '#B4D4E1',  # Pastel blue
    '#FFD4B8',  # Pastel orange
    '#C5E1B5',  # Pastel green
    '#FFB8C3',  # Pastel pink
    '#E0C5E8',  # Pastel purple
    '#FFE8B8',  # Pastel yellow
    '#B8E6E6',  # Pastel cyan
    '#FFD4E5',  # Pastel rose
    '#D4E8D4',  # Pastel mint
    '#E8D4C5',  # Pastel tan
]

# Initialize Dash app with Bootstrap theme
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
    title="Nidingen Bird Ringing Station",
    update_title="Loading...",
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"}
    ]
)

# Database path — override with DUCKDB_PATH env var for deployment.
# Default points to the new MDB-sourced database created by
# app/src/convert_mdb_to_duckdb.py.  The old bird_ringing.db is deprecated.
DB_PATH = os.getenv("DUCKDB_PATH", "/data/bird_ringing_0016.duckdb")

# Fail fast with a clear message if the database file is missing.
# This surfaces immediately in the container logs instead of a cryptic
# DuckDB traceback that is hard to interpret on SciLifeLab Serve.
if not Path(DB_PATH).exists():
    raise FileNotFoundError(
        f"\n\n"
        f"  DATABASE NOT FOUND: {DB_PATH!r}\n\n"
        f"  Checklist:\n"
        f"    1. Run the conversion script to create the database:\n"
        f"         uv run python app/src/convert_mdb_to_duckdb.py\n"
        f"    2. Did you upload bird_ringing_0016.duckdb into the 'project-vol'\n"
        f"       folder via the Serve File Manager?\n"
        f"    3. Is the Storage mount path set to '/project-vol' in\n"
        f"       Project Settings → Storage?\n"
        f"    4. Is DUCKDB_PATH correct? Currently: {DB_PATH!r}\n"
        f"       Expected layout inside the container: /project-vol/bird_ringing_0016.duckdb\n"
    )

# Load initial data for filters
with BirdRingingDB(DB_PATH, read_only=True) as db:
    # Get available species
    species_list = db.execute_query("""
        SELECT DISTINCT species_code, swedish_name 
        FROM ring_records 
        ORDER BY species_code
    """).fetchall()
    
    # Get date range
    date_range = db.execute_query("""
        SELECT MIN(date), MAX(date) 
        FROM ring_records
    """).fetchone()
    
    # Get available years for heatmap
    years_list = db.execute_query("""
        SELECT DISTINCT EXTRACT(YEAR FROM date) as year
        FROM ring_records
        ORDER BY year
    """).fetchall()
    available_years = [int(row[0]) for row in years_list]

    # Build taxonomy sort order lookup: species_code -> (order, family, scientific_name)
    # Sorts by the biological hierarchy: Order → Family → Scientific name.
    # Unmatched species (no metadata join) are placed at the end via '~' sentinel.
    _taxon_rows = db.execute_query("""
        SELECT r.species_code, r.swedish_name,
               COALESCE(m.order_scientific_name, '~') AS order_name,
               COALESCE(m.family_scientific_name, '~') AS family_name,
               COALESCE(m.scientific_name, r.swedish_name, r.species_code) AS sci_name
        FROM (SELECT DISTINCT species_code, swedish_name FROM ring_records) r
        LEFT JOIN species_metadata m ON r.swedish_name = m.swedish_name
        ORDER BY order_name, family_name, sci_name
    """).fetchall()
    # TOTAL always first (empty-string tuple sorts before anything); others by hierarchy
    TAXON_SORT_ORDER = {}
    _SPECIES_SWEDISH = {}
    for code, swe_name, order_name, family_name, sci_name in _taxon_rows:
        if code == "TOTAL":
            TAXON_SORT_ORDER[code] = ("", "", "")
        else:
            TAXON_SORT_ORDER[code] = (order_name, family_name, sci_name)
        _SPECIES_SWEDISH[code] = swe_name


def sort_species_by_taxonomy(species_codes):
    """Return *species_codes* sorted by taxonomic hierarchy (TOTAL always first).

    Sort key is a tuple (order_scientific_name, family_scientific_name, scientific_name)
    so species are grouped by Order, then Family, then alphabetically by scientific name.
    Species with no metadata match fall to the end via the '~' sentinel.
    """
    return sorted(species_codes, key=lambda c: TAXON_SORT_ORDER.get(c, ("~", "~", c)))


# Prepare options for dropdowns – sorted taxonomically
_sorted_codes = sort_species_by_taxonomy([code for code, _ in species_list])
species_options = [
    {"label": f"{code} - {_SPECIES_SWEDISH.get(code, '')}", "value": code}
    for code in _sorted_codes
]

# Year options for heatmap (including "All Years" option)
year_options = [{"label": "Average (All Years)", "value": "all"}] + [
    {"label": str(year), "value": year} for year in available_years
]

# App Layout
app.layout = dbc.Container([
    # Language store (hidden, stores current language state)
    dcc.Store(id="language-store", data="sv"),
    
    # Header with Language Toggle
    dbc.Row([
        dbc.Col([
            html.Div([
                # Language toggle button in top right
                dbc.Button(
                    "SV/EN",
                    id="language-toggle-btn",
                    className="btn-sm",
                    color="#ebe8c6",
                    style={
                        "position": "fixed",
                        "top": "20px",
                        "right": "20px",
                        "zIndex": "1000",
                        "fontSize": "0.9rem",
                        "fontWeight": "bold",
                        "padding": "0.5rem 1rem"
                    }
                ),
                
                html.H1(id="page-title", className="text-center mb-3", style={
                    "color": "#2c3e50",
                    "fontWeight": "600",
                    "fontSize": "2.5rem"
                }),
                html.P(
                    id="page-subtitle",
                    className="text-center text-muted mb-0",
                    style={"fontSize": "1.1rem"}
                ),
            ], className="py-4")
        ])
    ], className="mb-4", style={
        "backgroundColor": "#f8f9fa",
        "borderRadius": "10px",
        "boxShadow": "0 2px 4px rgba(0,0,0,0.05)"
    }),
    
    # Filters Card
    dbc.Card([
        dbc.CardBody([
            html.H5(id="filters-title", className="mb-4", style={"color": "#495057"}, children=[
                html.I(className="fas fa-filter me-2"),
                "Data Filters"
            ]),
            dbc.Row([
                dbc.Col([
                    html.Label(id="label-select-species", className="fw-bold mb-2", style={"color": "#6c757d"}, children="Select Species"),
                    dcc.Dropdown(
                        id="species-dropdown",
                        options=species_options,
                        value=[opt["value"] for opt in species_options if opt["value"] in ("TOTAL", "RÖHAK", "LÖSÅN")][:3] or [],
                        multi=True,
                        placeholder="Select one or more species...",
                        className="mb-3"
                    )
                ], md=6),
                
                dbc.Col([
                    html.Label(id="label-time-agg", className="fw-bold mb-2", style={"color": "#6c757d"}, children="Time Aggregation"),
                    dcc.Dropdown(
                        id="aggregation-dropdown",
                        options=[
                            {"label": "📅 Daily", "value": "daily"},
                            {"label": "📊 Weekly", "value": "weekly"},
                            {"label": "📈 Monthly", "value": "monthly"},
                            {"label": "📆 Yearly", "value": "yearly"}
                        ],
                        value="yearly",
                        clearable=False,
                        className="mb-3"
                    )
                ], md=6),
            ]),
            
            dbc.Row([
                dbc.Col([
                    html.Label(id="label-date-range", className="fw-bold mb-2", style={"color": "#6c757d"}, children="Date Range"),
                    dcc.DatePickerRange(
                        id="date-range-picker",
                        start_date=date_range[0],
                        end_date=date_range[1],
                        display_format="YYYY-MM-DD",
                        style={"width": "50%"}
                    )
                ], md=12),
            ])
        ])
    ], className="mb-4 shadow-sm", style={"borderRadius": "10px", "border": "none"}),
    
    # Main Content - Tabs
    dbc.Card([
        dbc.CardBody([
            dbc.Tabs([
                # Summary Tab (formerly Time Series)
                dbc.Tab(label="Summary", tab_id="tab-summary-timeseries", children=[
                    html.Div([
                        dbc.Row([
                            dbc.Col([
                                html.Label(id="label-plot-type", className="fw-bold me-3", style={"color": "#6c757d"}),
                                dbc.RadioItems(
                                    id="plot-type-toggle",
                                    options=[
                                        {"label": html.Span(id="opt-bar-chart"), "value": "bar"},
                                        {"label": html.Span(id="opt-line-chart"), "value": "line"}
                                    ],
                                    value="bar",
                                    inline=True,
                                    className="mb-3"
                                )
                            ])
                        ], className="mt-3"),
                        
                        dbc.Spinner(
                            dcc.Graph(id="time-series-plot", style={"height": "500px"}),
                            color="primary",
                            type="border",
                            spinner_style={"width": "3rem", "height": "3rem"}
                        ),
                        
                        # Summary statistics div moved here
                        html.Div(id="summary-stats", className="mt-4 p-2")
                    ], className="p-3")
                ]),
                
                # Morphometrics Tab
                dbc.Tab(label="📊 Morphometrics", tab_id="tab-morpho", children=[
                    html.Div([
                        # First row: Weight and Wing Length distributions
                        dbc.Row([
                            dbc.Col([
                                dbc.Spinner(
                                    dcc.Graph(id="weight-distribution", style={"height": "450px"}),
                                    color="primary",
                                    type="border"
                                )
                            ], md=6),
                            
                            dbc.Col([
                                dbc.Spinner(
                                    dcc.Graph(id="wing-length-distribution", style={"height": "450px"}),
                                    color="primary",
                                    type="border"
                                )
                            ], md=6),
                        ], className="mb-4"),
                        
                        # Second row: Age distribution and Fat score
                        dbc.Row([
                            dbc.Col([
                                dbc.Spinner(
                                    dcc.Graph(id="age-distribution", style={"height": "450px"}),
                                    color="primary",
                                    type="border"
                                )
                            ], md=6),
                            
                            dbc.Col([
                                dbc.Spinner(
                                    dcc.Graph(id="fat-score-distribution", style={"height": "450px"}),
                                    color="primary",
                                    type="border"
                                )
                            ], md=6),
                        ]),

                        html.Hr(className="my-4"),

                        # Third row: Weekly weight over the year
                        html.Div([
                            html.H5(id="header-weekly-weight", children=[
                                html.I(className="fas fa-weight-hanging me-2"),
                                "Weekly Weight Over the Year"
                            ], className="mb-3", style={"color": "#495057"}),
                            dbc.Row([
                                dbc.Col([
                                    html.Label(
                                        id="label-select-year",
                                        className="fw-bold mb-2",
                                        style={"color": "#6c757d"}
                                    ),
                                    dcc.Dropdown(
                                        id="weight-weekly-year-dropdown",
                                        options=year_options,
                                        value="all",
                                        clearable=False,
                                        style={"width": "260px"}
                                    ),
                                ], width="auto"),
                            ], className="mb-3"),
                            dbc.Spinner(
                                dcc.Graph(id="weight-weekly-plot", style={"height": "450px"}),
                                color="primary",
                                type="border"
                            ),
                        ], className="mt-4 mb-4"),

                        # Fourth row: Yearly mean weight trend
                        html.Div([
                            html.H5(id="header-yearly-weight", children=[
                                html.I(className="fas fa-chart-line me-2"),
                                "Yearly Mean Weight Trend"
                            ], className="mb-3", style={"color": "#495057"}),
                            dbc.Spinner(
                                dcc.Graph(id="weight-yearly-plot", style={"height": "450px"}),
                                color="primary",
                                type="border"
                            ),
                        ], className="mb-4"),

                    ], className="p-3")
                ]),
                
                # Phenology Tab
                dbc.Tab(label="⏱️ Phenology", tab_id="tab-phenology", children=[
                    html.Div([
                        html.Div([
                            html.H4(id="header-pheno-main"),
                            html.P(id="desc-pheno", className="text-muted mb-4"),
                        ], className="mt-3"),
                        
                        # Weekly Distribution
                        html.Div([
                            dbc.Spinner(
                                dcc.Graph(id="phenology-weekly-plot", style={"height": "450px"}),
                                color="primary",
                                type="border"
                            )
                        ], className="mb-4"),
                        
                        # Ridgeline Plot
                        html.Div([
                            dbc.Spinner(
                                dcc.Graph(id="phenology-ridgeline-plot"),
                                color="primary",
                                type="border"
                            )
                        ], className="mb-4"),
                        
                        # Seasonal Comparison
                        html.Div([
                            dbc.Spinner(
                                dcc.Graph(id="phenology-seasonal-plot", style={"height": "450px"}),
                                color="primary",
                                type="border"
                            )
                        ], className="mb-4"),
                    ], className="p-3")
                ]),
                
                # Weekly Heatmap Tab
                dbc.Tab(label="📈 Weekly Heatmap", tab_id="tab-heatmap", children=[
                    html.Div([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Select Year", className="fw-bold mb-2", style={"color": "#6c757d"}),
                                dcc.Dropdown(
                                    id="heatmap-year-dropdown",
                                    options=year_options,
                                    value="all",
                                    clearable=False,
                                    style={"width": "300px"}
                                )
                            ], width="auto"),
                            dbc.Col([
                                html.Label("Number of species", className="fw-bold mb-2", style={"color": "#6c757d"}),
                                dcc.Dropdown(
                                    id="heatmap-top-n-dropdown",
                                    options=[
                                        {"label": "10",  "value": 10},
                                        {"label": "30",  "value": 30},
                                        {"label": "50",  "value": 50},
                                        {"label": "100", "value": 100},
                                        {"label": "All", "value": 0},
                                    ],
                                    value=50,
                                    clearable=False,
                                    style={"width": "200px"}
                                )
                            ], width="auto"),
                        ], className="mt-3 mb-3", align="end"),
                        dbc.Spinner(
                            dcc.Graph(id="weekly-heatmap"),
                            color="primary",
                            type="border"
                        )
                    ], className="p-3")
                ]),
                
                # Weather Analysis Tab
                dbc.Tab(label="🌤️ Weather Analysis", tab_id="tab-weather", children=[
                    html.Div([
                        html.Div([
                            html.H4(id="header-weather-main"),
                            html.P(id="desc-weather", className="text-muted mb-3"),
                        ], className="mt-3"),

                        # Time series plot
                        html.Div([
                            dbc.Spinner(
                                dcc.Graph(id="weather-timeseries-plot"),
                                color="primary",
                                type="border",
                                spinner_style={"width": "3rem", "height": "3rem"}
                            ),
                        ], className="mb-4"),

                        # Variable selector
                        dbc.Row([
                            dbc.Col([
                                html.Label(
                                    "Select variables to display",
                                    className="fw-bold mb-2",
                                    style={"color": "#6c757d"}
                                ),
                                dbc.Checklist(
                                    id="weather-variable-checklist",
                                    options=[
                                        {"label": "Temperature (mean / min / max) — Nidingen A",    "value": "temperature"},
                                        {"label": "Wind speed & gusts — Nidingen A",                 "value": "wind"},
                                        {"label": "Precipitation — Nidingen A (≤2007) + Vinga A (2007→)", "value": "precipitation"},
                                        {"label": "Cloud cover — Nidingen A",                       "value": "cloud"},
                                        {"label": "Humidity — Nidingen A",                           "value": "humidity"},
                                        {"label": "Pressure — Nidingen A (≤1995) + Vinga A (1996→)", "value": "pressure"},
                                        {"label": "Visibility (m) — Nidingen A (≤2007) + Vinga A (2007→)", "value": "visibility"},
                                    ],
                                    value=["temperature", "wind", "precipitation", "visibility", "humidity", "cloud", "pressure"],
                                    inline=False,
                                    className="mb-3",
                                ),
                            ], md=12),
                        ]),

                    ], className="p-3")
                ]),

                # Summary Tab (Original removed)
            ], id="tabs", active_tab="tab-summary-timeseries")
        ])
    ], className="shadow-sm", style={"borderRadius": "10px", "border": "none"}),
    
    # Footer
    dbc.Row([
        dbc.Col([
            html.Hr(className="my-4"),
            html.P(id="footer-text", className="text-muted small")
        ])
    ])
], fluid=True, className="py-4", style={"backgroundColor": "#f5f7fa"})


# Callbacks
@callback(
    Output("time-series-plot", "figure"),
    [Input("species-dropdown", "value"),
     Input("aggregation-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date"),
     Input("plot-type-toggle", "value"),
     Input("language-store", "data")]
)
def update_time_series(species_codes, aggregation, start_date, end_date, plot_type, language):
    """Update time series plot based on filters."""
    t = LOCALES[language]
    
    if not species_codes:
        return go.Figure().add_annotation(
            text=t["please_select_species"],
            showarrow=False,
            font={"size": 20, "color": "#95a5a6"}
        )
    
    with BirdRingingDB(DB_PATH, read_only=True) as db:
        query = BirdRingingQueries.get_species_time_series(
            start_date=start_date,
            end_date=end_date,
            species_codes=species_codes,
            aggregation=aggregation
        )
        df = db.execute_query(query).pl().to_pandas()
    
    # Create figure based on plot type
    agg_text = t.get(f"agg_{aggregation}", aggregation.capitalize()).split(" ", 1)[1] if f"agg_{aggregation}" in t else aggregation.capitalize()
    if plot_type == "bar":
        # Bar chart with pastel colors
        fig = px.bar(
            df,
            x="period",
            y="count",
            color="swedish_name",
            title=f"{t['species_obs_over_time']} ({agg_text})",
            labels={"period": t["date_label"], "count": t["num_obs"], "swedish_name": t["species_label"]},
            barmode="group",
            color_discrete_sequence=PASTEL_COLORS
        )
        
        # Set bar width based on aggregation level and number of data points
        num_periods = df['period'].nunique()
        
        if aggregation == "daily":
            if num_periods < 50:
                bargap = 0.1
            elif num_periods < 200:
                bargap = 0.05
            else:
                bargap = 0.01
        elif aggregation == "weekly":
            bargap = 0.15
        elif aggregation == "monthly":
            bargap = 0.2
        else:  # yearly
            bargap = 0.3
        
        fig.update_traces(
            marker=dict(line=dict(width=0.5, color='white'))
        )
        
        fig.update_layout(
            bargap=bargap,
            bargroupgap=0.05
        )
    else:
        # Line chart with pastel colors
        total_points = len(df)
        if total_points < 20:
            mode = "markers"
            marker_size = 8
        elif total_points < 100:
            mode = "lines+markers"
            marker_size = 6
        else:
            mode = "lines"
            marker_size = 4
        
        fig = px.line(
            df,
            x="period",
            y="count",
            color="swedish_name",
            markers=True if mode in ["markers", "lines+markers"] else False,
            title=f"{t['species_obs_over_time']} ({agg_text})",
            labels={"period": t["date_label"], "count": t["num_obs"], "swedish_name": t["species_label"]},
            color_discrete_sequence=PASTEL_COLORS
        )
        
        # Update traces to use appropriate mode
        fig.update_traces(
            mode=mode,
            marker=dict(size=marker_size),
            line=dict(width=3),
            connectgaps=False
        )
    
    fig.update_layout(
        hovermode="x unified",
        template="plotly_white",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#dee2e6",
            borderwidth=1
        )
    )
    
    return fig


@callback(
    Output("summary-stats", "children"),
    [Input("species-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date"),
     Input("language-store", "data")]
)
def update_summary(species_codes, start_date, end_date, language):
    """Update summary statistics."""
    t = LOCALES[language]
    # Convert date strings to proper date format for comparison
    if isinstance(start_date, str):
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
    else:
        start_date_obj = start_date
        
    if isinstance(end_date, str):
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
    else:
        end_date_obj = end_date
    
    with BirdRingingDB(DB_PATH, read_only=True) as db:
        if species_codes:
            df = db.get_data_as_polars(
                filters={"species_code": species_codes}
            )
            # Filter by date using proper date objects
            df = df.filter(
                (pl.col("date") >= start_date_obj) & (pl.col("date") <= end_date_obj)
            )
            
            # Special handling for TOTAL species to show correct unique species count
            if len(species_codes) == 1 and species_codes[0] == "TOTAL":
                # When only TOTAL is selected, we want to show the count of actual species
                # that contribute to this total, not just "1" (which is the TOTAL species itself)
                start_str = start_date if isinstance(start_date, str) else start_date.strftime("%Y-%m-%d")
                end_str = end_date if isinstance(end_date, str) else end_date.strftime("%Y-%m-%d")
                
                unique_species = db.conn.execute(f"""
                    SELECT COUNT(DISTINCT species_code) 
                    FROM ring_records 
                    WHERE species_code != 'TOTAL' 
                    AND date >= '{start_str}' AND date <= '{end_str}'
                """).fetchone()[0]
            else:
                # When more than one species is selected we first try to remove TOTAL if it's in the selection to avoid double counting, then count unique species
                if "TOTAL" in species_codes:
                    df = df.filter(pl.col("species_code") != "TOTAL")
                unique_species = df["species_code"].n_unique()
        else:
            # If no species selected, fetch all but exclude TOTAL to avoid double counting
            df = db.get_data_as_polars()
            df = df.filter(pl.col("species_code") != "TOTAL")
            
            df = df.filter(
                (pl.col("date") >= start_date_obj) & (pl.col("date") <= end_date_obj)
            )
            unique_species = df["species_code"].n_unique()
        
        total_records = len(df)
        unique_individuals = df["ring_number"].n_unique()
        
        # Handle empty df possibility for min/max
        if len(df) > 0:
            date_range_str = f"{df['date'].min()} to {df['date'].max()}"
        else:
            date_range_str = "No data"
    
    return dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-clipboard-list fa-2x mb-3", 
                              style={"color": "#B4D4E1"}),
                        html.H2(f"{total_records:,}", 
                               className="mb-2",
                               style={"color": "#2c3e50", "fontWeight": "600"}),
                        html.P(t["summary_total_obs"], 
                              className="text-muted mb-0",
                              style={"fontSize": "0.95rem"})
                    ], className="text-center")
                ])
            ], className="shadow-sm h-100", style={
                "borderRadius": "10px",
                "border": "none",
                "borderLeft": "4px solid #B4D4E1"
            })
        ], md=3, className="mb-3"),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-dove fa-2x mb-3", 
                              style={"color": "#C5E1B5"}),
                        html.H2(f"{unique_species}", 
                               className="mb-2",
                               style={"color": "#2c3e50", "fontWeight": "600"}),
                        html.P(t["summary_unique_species"], 
                              className="text-muted mb-0",
                              style={"fontSize": "0.95rem"})
                    ], className="text-center")
                ])
            ], className="shadow-sm h-100", style={
                "borderRadius": "10px",
                "border": "none",
                "borderLeft": "4px solid #C5E1B5"
            })
        ], md=3, className="mb-3"),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-hashtag fa-2x mb-3", 
                              style={"color": "#FFD4B8"}),
                        html.H2(f"{unique_individuals:,}", 
                               className="mb-2",
                               style={"color": "#2c3e50", "fontWeight": "600"}),
                        html.P(t["summary_unique_inds"], 
                              className="text-muted mb-0",
                              style={"fontSize": "0.95rem"})
                    ], className="text-center")
                ])
            ], className="shadow-sm h-100", style={
                "borderRadius": "10px",
                "border": "none",
                "borderLeft": "4px solid #FFD4B8"
            })
        ], md=3, className="mb-3"),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-calendar-alt fa-2x mb-3", 
                              style={"color": "#E0C5E8"}),
                        html.H4(date_range_str, 
                               className="mb-2",
                               style={"color": "#2c3e50", "fontWeight": "600", "fontSize": "1.1rem"}),
                        html.P(t["summary_date_range"], 
                              className="text-muted mb-0",
                              style={"fontSize": "0.95rem"})
                    ], className="text-center")
                ])
            ], className="shadow-sm h-100", style={
                "borderRadius": "10px",
                "border": "none",
                "borderLeft": "4px solid #E0C5E8"
            })
        ], md=3, className="mb-3"),
    ])


@callback(
    Output("weight-distribution", "figure"),
    [Input("species-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date"),
     Input("language-store", "data")]
)
def update_weight_distribution(species_codes, start_date, end_date, language):
    """Update weight distribution plot."""
    t = LOCALES[language]
    
    if not species_codes:
        return go.Figure()
    
    with BirdRingingDB(DB_PATH, read_only=True) as db:
        df = db.get_data_as_polars(
            filters={"species_code": species_codes}
        ).to_pandas()
    
    # Filter by date
    df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
    # Remove NA and zero values (incorrect measurements)
    df = df[(df["weight"].notna()) & (df["weight"] > 0)]
    
    # Create mapping from species_code to swedish_name
    species_name_map = df[['species_code', 'swedish_name']].drop_duplicates().set_index('species_code')['swedish_name'].to_dict()
    
    # Sort species by taxonomy (TOTAL first, then taxonomic order)
    sorted_codes = sort_species_by_taxonomy([c for c in species_codes if c in species_name_map])
    species_order = [species_name_map[code] for code in sorted_codes]
    
    # Calculate sample sizes for each species
    sample_sizes = df.groupby("swedish_name").size()
    
    # Create labels with sample sizes
    df["species_label"] = df["swedish_name"].map(
        lambda x: f"{x}<br>(n={sample_sizes[x]})"
    )
    
    fig = px.box(
        df,
        x="species_label",
        y="weight",
        color="species_label",
        title=t["weight_dist_title"],
        labels={"species_label": t["species_label"], "weight": t["weight_g"]},
        color_discrete_sequence=PASTEL_COLORS,
        category_orders={"species_label": [f"{s}<br>(n={sample_sizes[s]})" for s in species_order]}
    )
    
    fig.update_layout(
        template="plotly_white",
        showlegend=False,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50")
    )
    
    return fig


@callback(
    Output("wing-length-distribution", "figure"),
    [Input("species-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date"),
     Input("language-store", "data")]
)
def update_wing_distribution(species_codes, start_date, end_date, language):
    """Update wing length distribution plot."""
    t = LOCALES[language]
    
    if not species_codes:
        return go.Figure()
    
    with BirdRingingDB(DB_PATH, read_only=True) as db:
        df = db.get_data_as_polars(
            filters={"species_code": species_codes}
        ).to_pandas()
    
    # Filter by date
    df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
    # Remove NA and zero values (incorrect measurements)
    df = df[(df["wing_length"].notna()) & (df["wing_length"] > 0)]
    
    # Create mapping from species_code to swedish_name
    species_name_map = df[['species_code', 'swedish_name']].drop_duplicates().set_index('species_code')['swedish_name'].to_dict()
    
    # Sort species by taxonomy (TOTAL first, then taxonomic order)
    sorted_codes = sort_species_by_taxonomy([c for c in species_codes if c in species_name_map])
    species_order = [species_name_map[code] for code in sorted_codes]
    
    # Calculate sample sizes for each species
    sample_sizes = df.groupby("swedish_name").size()
    
    # Create labels with sample sizes
    df["species_label"] = df["swedish_name"].map(
        lambda x: f"{x}<br>(n={sample_sizes[x]})"
    )
    
    fig = px.box(
        df,
        x="species_label",
        y="wing_length",
        color="species_label",
        title=t["wing_dist_title"],
        labels={"species_label": t["species_label"], "wing_length": t["wing_mm"]},
        color_discrete_sequence=PASTEL_COLORS,
        category_orders={"species_label": [f"{s}<br>(n={sample_sizes[s]})" for s in species_order]}
    )
    
    fig.update_layout(
        template="plotly_white",
        showlegend=False,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50")
    )
    
    return fig


@callback(
    Output("age-distribution", "figure"),
    [Input("species-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date"),
     Input("language-store", "data")]
)
def update_age_distribution(species_codes, start_date, end_date, language):
    """Update age distribution plot showing percentage of age classes per species."""
    t = LOCALES[language]
    
    if not species_codes:
        return go.Figure()
    
    with BirdRingingDB(DB_PATH, read_only=True) as db:
        df = db.get_data_as_polars(
            filters={"species_code": species_codes}
        ).to_pandas()
    
    # Filter by date
    df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
    # Remove NA values in age
    df = df[df["age"].notna() & (df["age"] != "")]
    
    if len(df) == 0:
        return go.Figure().add_annotation(
            text=t.get("no_data", "No data available"),
            showarrow=False,
            font={"size": 16, "color": "#95a5a6"}
        )
    
    # Create mapping from species_code to swedish_name
    species_name_map = df[['species_code', 'swedish_name']].drop_duplicates().set_index('species_code')['swedish_name'].to_dict()
    
    # Sort species by taxonomy (TOTAL first, then taxonomic order)
    sorted_codes = sort_species_by_taxonomy([c for c in species_codes if c in species_name_map])
    species_order = [species_name_map[code] for code in sorted_codes]
    
    # Calculate percentages for each species and age combination
    age_counts = df.groupby(['swedish_name', 'age']).size().reset_index(name='count')
    totals = df.groupby('swedish_name').size().reset_index(name='total')
    age_counts = age_counts.merge(totals, on='swedish_name')
    age_counts['percentage'] = (age_counts['count'] / age_counts['total']) * 100
    
    # Create labels with sample sizes
    species_labels = {row['swedish_name']: f"{row['swedish_name']}<br>(n={int(row['total'])})" 
                     for _, row in totals.iterrows()}
    
    # Get unique ages for consistent coloring
    unique_ages = sorted(df['age'].unique())
    age_color_map = {age: PASTEL_COLORS[i % len(PASTEL_COLORS)] for i, age in enumerate(unique_ages)}
    
    fig = go.Figure()
    
    # Add traces for each age class
    for age in unique_ages:
        age_data = age_counts[age_counts['age'] == age].copy()
        age_data['species_label'] = age_data['swedish_name'].map(species_labels)
        
        fig.add_trace(go.Bar(
            x=age_data['species_label'],
            y=age_data['percentage'],
            name=age,
            marker_color=age_color_map[age],
            text=age_data['percentage'].round(1),
            texttemplate='%{text}%',
            textposition='inside',
            hovertemplate='<b>%{x}</b><br>' +
                         f'Age: {age}<br>' +
                         f'{t["percentage"]}: %{{y:.1f}}%<br>' +
                         '<extra></extra>'
        ))
    
    # Create ordered category list for x-axis
    ordered_labels = [species_labels[s] for s in species_order if s in species_labels]
    
    fig.update_layout(
        barmode='stack',
        title=t["age_dist_title"],
        xaxis_title=t["species_label"],
        yaxis_title=t["percentage"],
        template="plotly_white",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50"),
        legend=dict(
            title=t["age_class"],
            orientation="v",
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99,
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#dee2e6",
            borderwidth=1
        ),
        yaxis=dict(range=[0, 100]),
        xaxis=dict(categoryorder='array', categoryarray=ordered_labels)
    )
    
    return fig


@callback(
    Output("fat-score-distribution", "figure"),
    [Input("species-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date"),
     Input("language-store", "data")]
)
def update_fat_score_distribution(species_codes, start_date, end_date, language):
    """Update fat score distribution plot."""
    t = LOCALES[language]
    
    if not species_codes:
        return go.Figure()
    
    with BirdRingingDB(DB_PATH, read_only=True) as db:
        df = db.get_data_as_polars(
            filters={"species_code": species_codes}
        ).to_pandas()
    
    # Filter by date
    df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
    # Remove NA values in fat score and filter valid range (0-10)
    df = df[df["fat_score"].notna() & (df["fat_score"] >= 0) & (df["fat_score"] <= 10)]
    
    if len(df) == 0:
        return go.Figure().add_annotation(
            text=t.get("no_data", "No data available"),
            showarrow=False,
            font={"size": 16, "color": "#95a5a6"}
        )
    
    # Create mapping from species_code to swedish_name
    species_name_map = df[['species_code', 'swedish_name']].drop_duplicates().set_index('species_code')['swedish_name'].to_dict()
    
    # Sort species by taxonomy (TOTAL first, then taxonomic order)
    sorted_codes = sort_species_by_taxonomy([c for c in species_codes if c in species_name_map])
    species_order = [species_name_map[code] for code in sorted_codes]
    
    # Calculate average fat score per species
    fat_summary = df.groupby('swedish_name').agg({
        'fat_score': ['mean', 'std', 'count']
    }).reset_index()
    fat_summary.columns = ['swedish_name', 'mean_fat_score', 'std_fat_score', 'count']
    
    # Create labels with sample sizes
    fat_summary['species_label'] = fat_summary.apply(
        lambda row: f"{row['swedish_name']}<br>(n={int(row['count'])})", axis=1
    )
    
    fig = go.Figure()
    
    # Add bars in the order of species_order
    for idx, species in enumerate(species_order):
        if species in fat_summary['swedish_name'].values:
            row = fat_summary[fat_summary['swedish_name'] == species].iloc[0]
            color = PASTEL_COLORS[idx % len(PASTEL_COLORS)]
            
            fig.add_trace(go.Bar(
                x=[row['species_label']],
                y=[row['mean_fat_score']],
                name=row['swedish_name'],
                marker_color=color,
                error_y=dict(
                    type='data',
                    array=[row['std_fat_score']],
                    visible=True
                ),
                text=[f"{row['mean_fat_score']:.2f}"],
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>' +
                             f'{t["fat_score"]}: %{{y:.2f}}<br>' +
                             f"Std: {row['std_fat_score']:.2f}<br>" +
                             f"n={int(row['count'])}<br>" +
                             '<extra></extra>',
                showlegend=False
            ))
    
    # Create ordered category list for x-axis
    ordered_labels = [f"{s}<br>(n={int(fat_summary[fat_summary['swedish_name']==s]['count'].iloc[0])})" 
                     for s in species_order if s in fat_summary['swedish_name'].values]
    
    fig.update_layout(
        title=t["fat_dist_title"],
        xaxis_title=t["species_label"],
        yaxis_title=t["fat_score"],
        template="plotly_white",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50"),
        yaxis=dict(range=[0, 12]),
        bargap=0.2,
        xaxis=dict(categoryorder='array', categoryarray=ordered_labels)
    )
    
    return fig


@callback(
    Output("weight-weekly-plot", "figure"),
    [Input("species-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date"),
     Input("weight-weekly-year-dropdown", "value"),
     Input("language-store", "data")]
)
def update_weight_weekly(species_codes, start_date, end_date, selected_year, language):
    """Weekly mean weight over the year, one line per species."""
    t = LOCALES[language]
    
    if not species_codes:
        return go.Figure()

    year_param = None if selected_year == "all" else int(selected_year)

    with BirdRingingDB(DB_PATH, read_only=True) as db:
        query = BirdRingingQueries.get_weekly_weight_by_species(
            species_codes=species_codes,
            year=year_param,
            start_date=start_date if year_param is None else None,
            end_date=end_date if year_param is None else None,
        )
        df = db.execute_query(query).pl().to_pandas()

    if df.empty:
        return go.Figure().add_annotation(
            text=t.get("no_data", "No data available"),
            showarrow=False,
            font={"size": 16, "color": "#95a5a6"},
        )

    year_label = str(year_param) if year_param else f"{t['avg_all_years']} {start_date[:4]}–{end_date[:4]}"
    title = f"{t['weekly_weight_title']} ({year_label})"

    fig = go.Figure()

    sorted_codes = sort_species_by_taxonomy(species_codes)
    for idx, species_code in enumerate(sorted_codes):
        sp_df = df[df["species_code"] == species_code]
        if sp_df.empty:
            continue

        name = sp_df["swedish_name"].iloc[0]
        color = PASTEL_COLORS[idx % len(PASTEL_COLORS)]
        r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
        fill_color = f"rgba({r},{g},{b},0.15)"

        # Shaded min–max band
        x_band = sp_df["week_of_year"].tolist() + sp_df["week_of_year"].tolist()[::-1]
        y_band = sp_df["max_weight"].tolist() + sp_df["min_weight"].tolist()[::-1]
        fig.add_trace(go.Scatter(
            x=x_band,
            y=y_band,
            fill="toself",
            fillcolor=fill_color,
            line=dict(color="rgba(0,0,0,0)"),
            showlegend=False,
            hoverinfo="skip",
            name=f"{name} range",
        ))

        # Mean line
        fig.add_trace(go.Scatter(
            x=sp_df["week_of_year"],
            y=sp_df["mean_weight"],
            mode="lines+markers",
            name=name,
            line=dict(color=color, width=2.5),
            marker=dict(size=5, color=color),
            hovertemplate=(
                f"<b>{name}</b><br>"
                f"{t['week_of_year']}: %{{x}}<br>"
                f"{t['weight_g']}: %{{y:.2f}}<br>"
                "<extra></extra>"
            ),
        ))

    fig.update_layout(
        title=title,
        xaxis_title=f"{t['week_of_year']}",
        yaxis_title=t["weight_g"],
        template="plotly_white",
        hovermode="x unified",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#dee2e6",
            borderwidth=1,
        ),
    )
    return fig


@callback(
    Output("weight-yearly-plot", "figure"),
    [Input("species-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date"),
     Input("language-store", "data")]
)
def update_weight_yearly(species_codes, start_date, end_date, language):
    """Yearly mean (± min/max) weight trend per species."""
    t = LOCALES[language]
    
    if not species_codes:
        return go.Figure()

    with BirdRingingDB(DB_PATH, read_only=True) as db:
        query = BirdRingingQueries.get_yearly_weight_by_species(
            species_codes=species_codes,
            start_date=start_date,
            end_date=end_date,
        )
        df = db.execute_query(query).pl().to_pandas()

    if df.empty:
        return go.Figure().add_annotation(
            text=t.get("no_data", "No data available"),
            showarrow=False,
            font={"size": 16, "color": "#95a5a6"},
        )

    fig = go.Figure()

    sorted_codes = sort_species_by_taxonomy(species_codes)
    for idx, species_code in enumerate(sorted_codes):
        sp_df = df[df["species_code"] == species_code].sort_values("year")
        if sp_df.empty:
            continue

        name = sp_df["swedish_name"].iloc[0]
        color = PASTEL_COLORS[idx % len(PASTEL_COLORS)]
        r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
        fill_color = f"rgba({r},{g},{b},0.15)"

        # Shaded min–max band
        x_band = sp_df["year"].tolist() + sp_df["year"].tolist()[::-1]
        y_band = sp_df["max_weight"].tolist() + sp_df["min_weight"].tolist()[::-1]
        fig.add_trace(go.Scatter(
            x=x_band,
            y=y_band,
            fill="toself",
            fillcolor=fill_color,
            line=dict(color="rgba(0,0,0,0)"),
            showlegend=False,
            hoverinfo="skip",
            name=f"{name} range",
        ))

        # Mean line with markers
        fig.add_trace(go.Scatter(
            x=sp_df["year"],
            y=sp_df["mean_weight"],
            mode="lines+markers",
            name=name,
            line=dict(color=color, width=2.5),
            marker=dict(size=8, color=color, line=dict(width=1.5, color="white")),
            text=sp_df["n"].map(lambda v: f"n={v:,}"),
            hovertemplate=(
                f"<b>{name}</b><br>"
                f"{t['year_label']}: %{{x}}<br>"
                f"{t['weight_g']}: %{{y:.2f}}<br>"
                "%{text}<extra></extra>"
            ),
        ))

    fig.update_layout(
        title=t["yearly_weight_title"].format(start=start_date[:4], end=end_date[:4]),
        xaxis_title=t["year_label"],
        yaxis_title=t["weight_g"],
        template="plotly_white",
        hovermode="x unified",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50"),
        xaxis=dict(dtick=1, tickformat="d"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#dee2e6",
            borderwidth=1,
        ),
    )
    return fig


@callback(
    Output("phenology-weekly-plot", "figure"),
    [Input("species-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date"),
     Input("language-store", "data")]
)
def update_phenology_weekly(species_codes, start_date, end_date, language):
    """Update weekly phenology plot showing migration patterns."""
    t = LOCALES[language]
    
    if not species_codes:
        return go.Figure()
    
    with BirdRingingDB(DB_PATH, read_only=True) as db:
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        
        query = BirdRingingQueries.get_phenology_weekly_distribution(
            species_codes=species_codes,
            start_year=start_year,
            end_year=end_year,
            aggregate_years=True
        )
        df = db.execute_query(query).pl().to_pandas()
    
    fig = go.Figure()
    
    # Sort species by taxonomy
    code_to_name = df[['species_code', 'swedish_name']].drop_duplicates().set_index('species_code')['swedish_name'].to_dict()
    sorted_codes = sort_species_by_taxonomy([c for c in code_to_name if c in TAXON_SORT_ORDER])
    sorted_names = [code_to_name[c] for c in sorted_codes if c in code_to_name]
    
    for idx, species in enumerate(sorted_names):
        species_df = df[df['swedish_name'] == species]
        if species_df.empty:
            continue
        color = PASTEL_COLORS[idx % len(PASTEL_COLORS)]
        
        # Convert hex color to rgba with opacity
        r = int(color[1:3], 16)
        g = int(color[3:5], 16)
        b = int(color[5:7], 16)
        fill_color = f'rgba({r}, {g}, {b}, 0.3)'  # 30% opacity for fill
        line_color = f'rgba({r}, {g}, {b}, 0.9)'  # 90% opacity for line
        
        fig.add_trace(go.Scatter(
            x=species_df['week_of_year'],
            y=species_df['avg_count'],
            mode='lines',
            name=species,
            fill='tozeroy',
            line=dict(width=3, color=line_color),
            fillcolor=fill_color,
            hovertemplate='<b>%{fullData.name}</b><br>' +
                         f'{t["week_of_year"]}: %{{x}}<br>' +
                         f'{t["avg_weekly_obs"]}: %{{y:.1f}}<br>' +
                         '<extra></extra>'
        ))
    
    # Add season markers with pastel colors
    fig.add_vrect(
        x0=9, x1=22, 
        fillcolor="rgba(144, 238, 144, 0.15)", 
        line_width=0,
        annotation_text=t["spring_mig"],
        annotation_position="top left",
        annotation=dict(font_size=11, font_color="#6c757d")
    )
    fig.add_vrect(
        x0=31, x1=43,
        fillcolor="rgba(255, 218, 185, 0.15)",
        line_width=0,
        annotation_text=t["autumn_mig"],
        annotation_position="top left",
        annotation=dict(font_size=11, font_color="#6c757d")
    )
    
    fig.update_layout(
        title=t["weekly_obs_pattern"].format(start=start_year, end=end_year),
        xaxis_title=f"{t['week_of_year']}",
        yaxis_title=t["avg_weekly_obs"],
        template="plotly_white",
        hovermode="x unified",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50"),
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99,
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#dee2e6",
            borderwidth=1
        )
    )
    
    return fig


@callback(
    Output("phenology-ridgeline-plot", "figure"),
    [Input("species-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date"),
     Input("language-store", "data")]
)
def update_phenology_ridgeline(species_codes, start_date, end_date, language):
    """Update ridgeline plot showing daily distribution by species."""
    t = LOCALES[language]
    
    if not species_codes:
        return go.Figure()
    
    with BirdRingingDB(DB_PATH, read_only=True) as db:
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        
        query = BirdRingingQueries.get_phenology_daily_distribution(
            species_codes=species_codes,
            start_year=start_year,
            end_year=end_year,
            aggregate_years=True
        )
        df = db.execute_query(query).pl().to_pandas()
    
    from plotly.subplots import make_subplots
    
    # Sort species by taxonomy
    code_to_name = df[['species_code', 'swedish_name']].drop_duplicates().set_index('species_code')['swedish_name'].to_dict()
    sorted_codes = sort_species_by_taxonomy([c for c in code_to_name if c in TAXON_SORT_ORDER])
    species_list = [code_to_name[c] for c in sorted_codes if c in code_to_name]
    n_species = len(species_list)
    
    # Increase vertical spacing to prevent overlap
    spacing = 0.08 if n_species > 3 else 0.05
    
    fig = make_subplots(
        rows=n_species, 
        cols=1,
        subplot_titles=[f"<b>{sp}</b>" for sp in species_list],
        vertical_spacing=spacing,
        shared_xaxes=True
    )
    
    for idx, species in enumerate(species_list, start=1):
        species_df = df[df['swedish_name'] == species]
        color = PASTEL_COLORS[(idx - 1) % len(PASTEL_COLORS)]
        
        # Convert hex to rgba for transparency
        r = int(color[1:3], 16)
        g = int(color[3:5], 16)
        b = int(color[5:7], 16)
        fill_color = f'rgba({r}, {g}, {b}, 0.4)'
        line_color = f'rgba({r}, {g}, {b}, 0.9)'
        
        fig.add_trace(
            go.Scatter(
                x=species_df['day_of_year'],
                y=species_df['avg_count'],
                mode='lines',
                fill='tozeroy',
                line=dict(width=2, color=line_color),
                fillcolor=fill_color,
                showlegend=False,
                hovertemplate=f'{t["day_of_year"]}: %{{x}}<br>{t["count_label"]}: %{{y:.2f}}<extra></extra>'
            ),
            row=idx,
            col=1
        )
        
        # Update y-axis for each subplot
        fig.update_yaxes(
            title_text=t["count_label"],
            title_standoff=5,
            title_font_size=10,
            row=idx,
            col=1
        )
    
    fig.update_xaxes(title_text=t["day_of_year"], row=n_species, col=1)
    
    fig.update_layout(
        height=220 * n_species,  # Increased height per species
        title_text=t["daily_obs_dist"].format(start=start_year, end=end_year),
        template="plotly_white",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50")
    )
    
    return fig


@callback(
    Output("phenology-seasonal-plot", "figure"),
    [Input("species-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date"),
     Input("language-store", "data")]
)
def update_phenology_seasonal(species_codes, start_date, end_date, language):
    """Update spring vs autumn migration comparison plot."""
    t = LOCALES[language]
    
    if not species_codes:
        return go.Figure()
    
    with BirdRingingDB(DB_PATH, read_only=True) as db:
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        
        query = BirdRingingQueries.get_phenology_migration_windows(
            species_codes=species_codes,
            start_year=start_year,
            end_year=end_year
        )
        df = db.execute_query(query).pl().to_pandas()
    
    # Sort species by taxonomy for y-axis ordering
    code_to_name = df[['species_code', 'swedish_name']].drop_duplicates().set_index('species_code')['swedish_name'].to_dict()
    sorted_codes = sort_species_by_taxonomy(list(code_to_name.keys()))
    sorted_names = [code_to_name[c] for c in sorted_codes if c in code_to_name]

    # Calculate average across years for each species/season
    seasonal_avg = df.groupby(['swedish_name', 'season']).agg({
        'median': 'mean',
        'q25': 'mean',
        'q75': 'mean',
        'n_observations': 'sum'
    }).reset_index()
    
    fig = go.Figure()
    
    for season, color, symbol in [
        ('spring', '#C5E1B5', 'circle'),  # Pastel green
        ('autumn', '#FFD4B8', 'square')   # Pastel orange
    ]:
        season_df = seasonal_avg[seasonal_avg['season'] == season]
        
        if len(season_df) > 0:
            fig.add_trace(go.Scatter(
                y=season_df['swedish_name'],
                x=season_df['median'],
                error_x=dict(
                    type='data',
                    symmetric=False,
                    array=season_df['q75'] - season_df['median'],
                    arrayminus=season_df['median'] - season_df['q25']
                ),
                mode='markers',
                name=season.capitalize(),
                marker=dict(
                    size=14,
                    symbol=symbol,
                    color=color,
                    line=dict(width=2, color='white')
                ),
                text=[f"n={int(n)}" for n in season_df['n_observations']],
                hovertemplate='<b>%{y}</b><br>' +
                             f'{season_df["season"].iloc[0].capitalize()} migration<br>' +
                             f'{t["day_of_year"]}: %{{x:.0f}}<br>' +
                             f'IQR: %{{error_x.array:.0f}} days<br>' +
                             '%{text}<br>' +
                             '<extra></extra>'
            ))
    
    # Add month labels on y-axis
    months = [(1, t["month_jan"]), (32, t["month_feb"]), (60, t["month_mar"]), (91, t["month_apr"]), (121, t["month_may"]), 
              (152, t["month_jun"]), (182, t["month_jul"]), (213, t["month_aug"]), (244, t["month_sep"]), (274, t["month_oct"]), 
              (305, t["month_nov"]), (335, t["month_dec"])]
    
    fig.update_layout(
        title=t["spring_vs_autumn"].format(start=start_year, end=end_year),
        yaxis_title=t["species_label"],
        xaxis_title=t["day_of_year"],
        yaxis=dict(categoryorder='array', categoryarray=list(reversed(sorted_names))),
        template="plotly_white",
        hovermode="closest",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#dee2e6",
            borderwidth=1
        )
    )
    
    fig.update_xaxes(
        tickmode='array',
        tickvals=[m[0] for m in months],
        ticktext=[m[1] for m in months]
    )
    
    return fig


@callback(
    Output("weekly-heatmap", "figure"),
    [Input("heatmap-year-dropdown", "value"),
     Input("heatmap-top-n-dropdown", "value"),
     Input("language-store", "data")]
)
def update_weekly_heatmap(selected_year, top_n, language):
    """Update weekly heatmap showing normalized observations per species."""
    t = LOCALES[language]
    
    with BirdRingingDB(DB_PATH, read_only=True) as db:
        # Determine if we're showing all years or a specific year
        if selected_year == "all":
            year_param = None
            title_suffix = t["avg_all_years"]
        else:
            year_param = int(selected_year)
            title_suffix = f"({selected_year})"
        
        # Get heatmap data
        top_n_species = None if top_n == 0 else top_n
        query = BirdRingingQueries.get_weekly_heatmap_data(
            year=year_param,
            top_n_species=top_n_species
        )
        df = db.execute_query(query).pl()
    
    if len(df) == 0:
        return go.Figure().add_annotation(
            text=t.get("no_data", "No data available"),
            showarrow=False,
            font={"size": 20, "color": "#95a5a6"}
        )
    
    # Convert to pandas for pivoting
    df_pd = df.to_pandas()

    # Determine which count column the query returned
    # (specific year → 'count'; all-years average → 'avg_count')
    count_col = 'count' if 'count' in df_pd.columns else 'avg_count'
    count_label = 'n' if count_col == 'count' else 'avg n'

    # ── Step 1: single count pivot (source of truth) ─────────────────────
    # aggfunc='sum' is safe because the SQL CROSS JOIN guarantees exactly one
    # row per (species, week). Using 'sum' also prevents silent averaging if
    # a swedish_name ever maps to more than one species_code.
    pivot_counts = df_pd.pivot_table(
        index='swedish_name',
        columns='week_of_year',
        values=count_col,
        aggfunc='sum',
        fill_value=0,
    )

    # ── Step 2: taxonomy-sorted species order ─────────────────────────────
    heatmap_code_to_name = (
        df_pd[['species_code', 'swedish_name']]
        .drop_duplicates()
        .set_index('species_code')['swedish_name']
        .to_dict()
    )
    sorted_heatmap_codes = sort_species_by_taxonomy(list(heatmap_code_to_name.keys()))
    species_order = [
        heatmap_code_to_name[c]
        for c in sorted_heatmap_codes
        if c in heatmap_code_to_name
    ]

    # ── Step 3: reindex rows and ensure all 52 weeks are columns ──────────
    pivot_counts = pivot_counts.reindex(species_order).fillna(0)
    for week in range(1, 53):
        if week not in pivot_counts.columns:
            pivot_counts[week] = 0.0
    pivot_counts = pivot_counts[sorted(pivot_counts.columns)]

    # ── Step 4: derive percent-of-row from counts ─────────────────────────
    # Divide each cell by its row total; species with 0 observations stay 0.
    row_sums = pivot_counts.sum(axis=1)
    pivot_pct = (
        pivot_counts
        .div(row_sums.replace(0, float('nan')), axis=0)
        .fillna(0)
        * 100
    )

    # Create heatmap — z comes from pct, customdata from the same count pivot
    fig = go.Figure(data=go.Heatmap(
        z=pivot_pct.values,
        x=pivot_pct.columns.tolist(),
        y=pivot_pct.index.tolist(),
        customdata=pivot_counts.values,
        colorscale='viridis',
        colorbar=dict(
            title=dict(
                text=t["heatmap_legend"],
                side="right"
            ),
            thickness=15,
            len=0.7
        ),
        hoverongaps=False,
        hovertemplate=(
            f'<b>%{{y}}</b><br>'
            f'{t["week_of_year"]} %{{x}}<br>'
            f'%{{z:.1f}}%<br>'
            f'{count_label}=%{{customdata:.1f}}'
            f'<extra></extra>'
        ),
    ))
    
    fig.update_layout(
        title=t["weekly_heatmap_title"].format(suffix=title_suffix),
        xaxis=dict(
            title=f"{t['week_of_year']}",
            tickmode='linear',
            tick0=1,
            dtick=2,
            side='bottom'
        ),
        yaxis=dict(
            title=t["species_label"],
            tickfont=dict(size=13)
        ),
        height=200 + 20 * len(pivot_counts),  # Dynamic height based on number of species
        template="plotly_white",
        font=dict(size=14, family="Arial, sans-serif", color="#495057"),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        title_font=dict(size=18, color="#2c3e50")
    )
    
    return fig


@callback(
    Output("weather-timeseries-plot", "figure"),
    [Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date"),
     Input("weather-variable-checklist", "value"),
     Input("language-store", "data")]
)
def update_weather_timeseries(start_date, end_date, selected_vars, language):
    """Update the weather time series plot for the selected date range and variables."""
    t = LOCALES[language]
    
    if not selected_vars:
        fig = go.Figure()
        fig.add_annotation(
            text=t.get("please_select_species", "Please select at least one variable"),
            showarrow=False,
            font={"size": 16, "color": "#95a5a6"},
            xref="paper", yref="paper", x=0.5, y=0.5
        )
        fig.update_layout(template="plotly_white", paper_bgcolor="rgba(0,0,0,0)")
        return fig

    with BirdRingingDB(DB_PATH, read_only=True) as db:
        query = BirdRingingQueries.get_daily_weather_summary(
            start_date=start_date,
            end_date=end_date
        )
        df = db.execute_query(query).pl().to_pandas()

    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text=t.get("no_data", "No data available"),
            showarrow=False,
            font={"size": 16, "color": "#95a5a6"},
            xref="paper", yref="paper", x=0.5, y=0.5
        )
        fig.update_layout(template="plotly_white", paper_bgcolor="rgba(0,0,0,0)")
        return fig

    # -----------------------------------------------------------------------
    # Build subplot grid: one row per selected variable
    # -----------------------------------------------------------------------
    VAR_META = {
        "temperature":   {"title": "Temperature (°C)  ·  Nidingen A",  "color_idx": 0},
        "wind":          {"title": "Wind (m/s)  ·  Nidingen A",         "color_idx": 1},
        "precipitation": {"title": "Precipitation (mm)  ·  Nidingen A + Vinga A", "color_idx": 2},
        "cloud":         {"title": "Cloud cover (%)  ·  Nidingen A",    "color_idx": 6},
        "humidity":      {"title": "Humidity (%)  ·  Nidingen A",       "color_idx": 3},
        "pressure":      {"title": "Pressure (hPa)  ·  Nidingen A + Vinga A", "color_idx": 4},
        "visibility":    {"title": "Visibility (m)  ·  Nidingen A + Vinga A",  "color_idx": 7},
    }

    # Vinga accent colour (pastel tan, distinct from the primary colours used above)
    VINGA_COLOR = "#E8D4C5"  # pastel tan
    rv, gv, bv = int(VINGA_COLOR[1:3], 16), int(VINGA_COLOR[3:5], 16), int(VINGA_COLOR[5:7], 16)

    # Keep order stable regardless of checklist order
    var_order = ["temperature", "wind", "precipitation", "cloud", "humidity", "pressure", "visibility"]
    active_vars = [v for v in var_order if v in selected_vars]
    n_rows = len(active_vars)

    # Row heights: temperature gets a bit more vertical space
    row_heights = []
    for v in active_vars:
        row_heights.append(1.5 if v == "temperature" else 1.0)
    total_height = sum(row_heights)
    row_heights_norm = [h / total_height for h in row_heights]

    from plotly.subplots import make_subplots

    subplot_titles = [VAR_META[v]["title"] for v in active_vars]
    fig = make_subplots(
        rows=n_rows,
        cols=1,
        shared_xaxes=True,
        subplot_titles=subplot_titles,
        row_heights=row_heights_norm,
        vertical_spacing=0.06,
    )

    for row_idx, var in enumerate(active_vars, start=1):
        color = PASTEL_COLORS[VAR_META[var]["color_idx"]]
        r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
        fill_color = f"rgba({r},{g},{b},0.25)"

        if var == "temperature":
            # Shaded band: min–max + mean line
            fig.add_trace(
                go.Scatter(
                    x=df["date"].tolist() + df["date"].tolist()[::-1],
                    y=df["max_temperature"].tolist() + df["min_temperature"].tolist()[::-1],
                    fill="toself",
                    fillcolor=fill_color,
                    line=dict(color="rgba(0,0,0,0)"),
                    showlegend=True,
                    name="Temp min–max · Nidingen A",
                    hoverinfo="skip",
                ),
                row=row_idx, col=1,
            )
            fig.add_trace(
                go.Scatter(
                    x=df["date"],
                    y=df["mean_temperature"],
                    mode="lines",
                    line=dict(color=color, width=2),
                    name="Mean temp · Nidingen A",
                    hovertemplate="%{x|%Y-%m-%d}<br>Mean: %{y:.1f} °C  [Nidingen A]<extra></extra>",
                ),
                row=row_idx, col=1,
            )

        elif var == "wind":
            gust_color = PASTEL_COLORS[VAR_META["temperature"]["color_idx"]]
            r2, g2, b2 = int(gust_color[1:3], 16), int(gust_color[3:5], 16), int(gust_color[5:7], 16)
            fig.add_trace(
                go.Scatter(
                    x=df["date"],
                    y=df["max_gust"],
                    mode="lines",
                    line=dict(color=f"rgba({r2},{g2},{b2},0.5)", width=1, dash="dot"),
                    name="Max gust · Nidingen A",
                    hovertemplate="%{x|%Y-%m-%d}<br>Max gust: %{y:.1f} m/s  [Nidingen A]<extra></extra>",
                ),
                row=row_idx, col=1,
            )
            fig.add_trace(
                go.Scatter(
                    x=df["date"],
                    y=df["mean_wind_speed"],
                    mode="lines",
                    line=dict(color=color, width=2),
                    fill="tozeroy",
                    fillcolor=fill_color,
                    name="Mean wind · Nidingen A",
                    hovertemplate="%{x|%Y-%m-%d}<br>Mean wind: %{y:.1f} m/s  [Nidingen A]<extra></extra>",
                ),
                row=row_idx, col=1,
            )

        elif var == "precipitation":
            # Split into Nidingen rows and Vinga gap-fill rows
            df_nid  = df[~df["vinga_gap_fill_used"]]
            df_ving = df[df["vinga_gap_fill_used"]]
            if not df_nid.empty:
                fig.add_trace(
                    go.Bar(
                        x=df_nid["date"],
                        y=df_nid["total_precipitation"],
                        marker_color=color,
                        marker_line_width=0,
                        name="Precipitation · Nidingen A",
                        hovertemplate="%{x|%Y-%m-%d}<br>Precip: %{y:.1f} mm  [Nidingen A]<extra></extra>",
                    ),
                    row=row_idx, col=1,
                )
            if not df_ving.empty:
                fig.add_trace(
                    go.Bar(
                        x=df_ving["date"],
                        y=df_ving["total_precipitation"],
                        marker_color=VINGA_COLOR,
                        marker_line_width=0,
                        name="Precipitation · Vinga A (gap-fill)",
                        hovertemplate="%{x|%Y-%m-%d}<br>Precip: %{y:.1f} mm  [Vinga A]<extra></extra>",
                    ),
                    row=row_idx, col=1,
                )

        elif var == "cloud":
            fig.add_trace(
                go.Scatter(
                    x=df["date"],
                    y=df["mean_cloud_cover"],
                    mode="lines",
                    line=dict(color=color, width=2),
                    fill="tozeroy",
                    fillcolor=fill_color,
                    name="Cloud cover · Nidingen A",
                    hovertemplate="%{x|%Y-%m-%d}<br>Cloud: %{y:.0f}%  [Nidingen A]<extra></extra>",
                ),
                row=row_idx, col=1,
            )

        elif var == "humidity":
            fig.add_trace(
                go.Scatter(
                    x=df["date"],
                    y=df["mean_humidity"],
                    mode="lines",
                    line=dict(color=color, width=2),
                    fill="tozeroy",
                    fillcolor=fill_color,
                    name="Humidity · Nidingen A",
                    hovertemplate="%{x|%Y-%m-%d}<br>Humidity: %{y:.0f}%  [Nidingen A]<extra></extra>",
                ),
                row=row_idx, col=1,
            )

        elif var == "pressure":
            # Split into Nidingen rows and Vinga gap-fill rows
            df_nid  = df[~df["vinga_gap_fill_used"]]
            df_ving = df[df["vinga_gap_fill_used"]]
            if not df_nid.empty:
                fig.add_trace(
                    go.Scatter(
                        x=df_nid["date"],
                        y=df_nid["mean_pressure"],
                        mode="lines",
                        line=dict(color=color, width=2),
                        name="Pressure · Nidingen A",
                        hovertemplate="%{x|%Y-%m-%d}<br>Pressure: %{y:.1f} hPa  [Nidingen A]<extra></extra>",
                    ),
                    row=row_idx, col=1,
                )
            if not df_ving.empty:
                fig.add_trace(
                    go.Scatter(
                        x=df_ving["date"],
                        y=df_ving["mean_pressure"],
                        mode="lines",
                        line=dict(color=VINGA_COLOR, width=2),
                        name="Pressure · Vinga A (gap-fill)",
                        hovertemplate="%{x|%Y-%m-%d}<br>Pressure: %{y:.1f} hPa  [Vinga A]<extra></extra>",
                    ),
                    row=row_idx, col=1,
                )

        elif var == "visibility":
            df_nid  = df[~df["vinga_gap_fill_used"]]
            df_ving = df[df["vinga_gap_fill_used"]]
            if not df_nid.empty:
                fig.add_trace(
                    go.Scatter(
                        x=df_nid["date"],
                        y=df_nid["mean_visibility"],
                        mode="lines",
                        line=dict(color=color, width=2),
                        name="Visibility · Nidingen A",
                        hovertemplate="%{x|%Y-%m-%d}<br>Visibility: %{y:.0f} m  [Nidingen A]<extra></extra>",
                    ),
                    row=row_idx, col=1,
                )
            if not df_ving.empty:
                fig.add_trace(
                    go.Scatter(
                        x=df_ving["date"],
                        y=df_ving["mean_visibility"],
                        mode="lines",
                        line=dict(color=VINGA_COLOR, width=2),
                        name="Visibility · Vinga A (gap-fill)",
                        hovertemplate="%{x|%Y-%m-%d}<br>Visibility: %{y:.0f} m  [Vinga A]<extra></extra>",
                    ),
                    row=row_idx, col=1,
                )

    # Shared layout
    plot_height = max(350, 220 * n_rows)
    fig.update_layout(
        height=plot_height,
        template="plotly_white",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50"),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#dee2e6",
            borderwidth=1,
        ),
        margin=dict(l=60, r=30, t=60, b=40),
    )

    # Style subplot title annotations
    for annotation in fig.layout.annotations:
        annotation.update(font=dict(size=13, color="#495057"), xanchor="left", x=0)

    return fig


# Language Toggle Callback
@callback(
    Output("language-store", "data"),
    Input("language-toggle-btn", "n_clicks"),
    State("language-store", "data"),
    prevent_initial_call=True
)
def toggle_language(n_clicks, current_language):
    """Toggle between Swedish and English."""
    return "en" if current_language == "sv" else "sv"


# Update Page Title and Subtitle
@callback(
    [Output("page-title", "children"),
     Output("page-subtitle", "children"),
     Output("filters-title", "children"),
     Output("label-select-species", "children"),
     Output("label-time-agg", "children"),
     Output("label-date-range", "children"),
     Output("species-dropdown", "placeholder"),
     Output("label-plot-type", "children"),
     Output("opt-bar-chart", "children"),
     Output("opt-line-chart", "children"),
     Output("header-weekly-weight", "children"),
     Output("label-select-year", "children"),
     Output("header-yearly-weight", "children"),
     Output("header-pheno-main", "children"),
     Output("desc-pheno", "children"),
     Output("header-weather-main", "children"),
     Output("desc-weather", "children"),
     Output("footer-text", "children")],
    Input("language-store", "data")
)
def update_header_and_filters(language):
    """Update all static text elements based on language."""
    t = LOCALES[language]
    subtitle = t["subtitle"].format(start=date_range[0], end=date_range[1])
    
    return (
        [html.I(className="fas fa-dove me-3"), t["title"]],
        subtitle,
        [html.I(className="fas fa-filter me-2"), t["filters_title"]],
        t["select_species"],
        t["time_aggregation"],
        t["date_range"],
        t["species_placeholder"],
        t.get("plot_type", "Plot Type"),
        t.get("plot_bar", "Bar Chart"),
        t.get("plot_line", "Line Chart"),
        [html.I(className="fas fa-weight-hanging me-2"), t.get("weekly_weight_header", "Weekly Weight Over the Year")],
        t.get("select_year", "Select Year"),
        [html.I(className="fas fa-chart-line me-2"), t.get("yearly_weight_header", "Yearly Mean Weight Trend")],
        t.get("pheno_header", "Migration Phenology Analysis"),
        t.get("pheno_desc", "Explore migration patterns throughout the year. Birds are captured during spring (northward) and autumn (southward) migration periods."),
        t.get("weather_header", "Weather Analysis"),
        t.get("weather_desc", "Meteorological observations from SMHI Nidingen A (station 71190) supplemented by Vinga A (station 71380) where Nidingen lacks data."),
        f"{t.get('dashboard', 'Nidingen Bird Ringing Station Dashboard')} · {html.A(t.get('view_github', 'View on GitHub'), href='#', className='text-decoration-none')} · {t.get('built_with', 'Built with Dash & Plotly')}"
    )


# Expose Flask server for gunicorn: `gunicorn app:server`
server = app.server

if __name__ == "__main__":
    app.run(debug=True, port=8050)

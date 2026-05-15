"""
Bird Ringing Dashboard - Main Application

This is the main Dash application file for the bird ringing data dashboard.
It provides interactive visualizations for exploring bird observation data.
"""

import os

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
    title="Nidingens Fågelstation",
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
year_options = [{"label": "Genomsnitt (Alla år)", "value": "all"}] + [
    {"label": str(year), "value": year} for year in available_years
]

# App Layout
app.layout = html.Div([

    # ── TOP NAVIGATION (dbc.Tabs used as sticky nav bar only) ─────────────
    dbc.Tabs([
        dbc.Tab(label="Hem",            tab_id="tab-home"),
        dbc.Tab(label="Sammanfattning", tab_id="tab-summary-timeseries"),
        dbc.Tab(label="Morfometri",     tab_id="tab-morpho"),
        dbc.Tab(label="Fenologi",       tab_id="tab-phenology"),
        dbc.Tab(label="Värmekarta",     tab_id="tab-heatmap"),
        dbc.Tab(label="Väderanalys",    tab_id="tab-weather"),
    ], id="tabs", active_tab="tab-home", className="top-nav-tabs"),

    # ── FILTER DROPDOWN PANEL (hidden on Hem + Väderanalys) ──────────────
    html.Div([
        dbc.Container([
            dbc.Button(
                [html.I(className="fas fa-sliders-h me-2"), "Datafilter ▾"],
                id="filter-toggle-btn",
                color="light",
                size="sm",
                className="my-2",
            ),
            dbc.Collapse(
                dbc.Card([
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Välj art", className="fw-bold mb-1 small",
                                           style={"color": "#6c757d"}),
                                dcc.Dropdown(
                                    id="species-dropdown",
                                    options=species_options,
                                    value=[opt["value"] for opt in species_options
                                           if opt["value"] in ("TOTAL", "RÖHAK", "LÖSÅN")][:3] or [],
                                    multi=True,
                                    placeholder="Välj en eller flera arter...",
                                )
                            ], md=5),
                            dbc.Col([
                                html.Label("Tidsaggregering", className="fw-bold mb-1 small",
                                           style={"color": "#6c757d"}),
                                dcc.Dropdown(
                                    id="aggregation-dropdown",
                                    options=[
                                        {"label": "Dagligen",   "value": "daily"},
                                        {"label": "Veckovis",  "value": "weekly"},
                                        {"label": "Månadsvis", "value": "monthly"},
                                        {"label": "Årligen",   "value": "yearly"},
                                    ],
                                    value="yearly",
                                    clearable=False,
                                )
                            ], md=3),
                            dbc.Col([
                                html.Label("Datumintervall", className="fw-bold mb-1 small",
                                           style={"color": "#6c757d"}),
                                dcc.DatePickerRange(
                                    id="date-range-picker",
                                    start_date=date_range[0],
                                    end_date=date_range[1],
                                    display_format="YYYY-MM-DD",
                                )
                            ], md=4),
                        ], align="end", className="g-3")
                    ])
                ], className="border-0 shadow-sm"),
                id="filter-collapse",
                is_open=False,
            ),
        ], fluid=True, className="px-3")
    ], id="filter-panel-outer", style={"display": "none"}),

    # ── MAIN CONTENT AREA ─────────────────────────────────────────────────
    html.Div([

        # ── Hem tab – hero image ──────────────────────────────────────────
        html.Div([
            html.Div([
                html.H1(
                    "NIDINGENS FÅGELSTATION DASHBOARD",
                    style={
                        "color": "#E7E7E7",
                        "fontWeight": "900",
                        "fontSize": "clamp(2rem, 4.5vw, 5rem)",
                        "textAlign": "left",
                        "lineHeight": "1.05",
                        "letterSpacing": "0.02em",
                        "margin": "0",
                    }
                ),
                html.H2(
                    "Historisk ringmärkningsdata och återfynd från Nidingen Fågelstation. Daglig upplösning från 1980-talet till idag-",
                    style={
                        "color": "#E7E7E7",
                        "fontWeight": "400",
                        "fontSize": "clamp(0.85rem, 1.5vw, 1.6rem)",
                        "textAlign": "left",
                        "lineHeight": "1.4",
                        "letterSpacing": "0.01em",
                        "margin": "0",
                        "maxWidth": "none",
                    }
                ),
            ], id="hero-text-block"),
        ], id="content-tab-home"),

        # ── Sammanfattning tab ────────────────────────────────────────────
        html.Div([
            dbc.Container([
                html.Div(id="summary-stats", className="mb-4"),
                dbc.Row([
                    dbc.Col([
                        html.Label("Diagramtyp", className="fw-bold me-3",
                                   style={"color": "#6c757d"}),
                        dbc.RadioItems(
                            id="plot-type-toggle",
                            options=[
                                {"label": html.Span("Stapeldiagram"), "value": "bar"},
                                {"label": html.Span("Linjediagram"),  "value": "line"},
                            ],
                            value="bar",
                            inline=True,
                            className="mb-3",
                        )
                    ])
                ], className="mt-3"),
                dbc.Spinner(
                    dcc.Graph(id="time-series-plot", style={"height": "500px"}),
                    color="primary", type="border",
                    spinner_style={"width": "3rem", "height": "3rem"},
                ),
            ], fluid=True, className="py-4"),
        ], id="content-tab-summary-timeseries", style={"display": "none"}),

        # ── Morfometri tab ────────────────────────────────────────────────
        html.Div([
            dbc.Container([
                html.Div([
                    html.H5([html.I(className="fas fa-circle me-2"),
                             "Vikt vs. vinglängd per art"],
                            className="mb-3", style={"color": "#495057"}),
                    html.P(
                        "Varje bubbla är en art. Storlek = genomsnittliga fångster per år (log-skalat). "
                        "Färg = taxonomisk familj (grå = okänd).",
                        className="text-muted small mb-3"
                    ),
                    dbc.Spinner(dcc.Graph(id="morpho-bubble-chart",
                                          style={"height": "600px"}),
                                color="primary", type="border"),
                ], className="mb-4"),
                dbc.Row([
                    dbc.Col([
                        dbc.Spinner(dcc.Graph(id="weight-distribution",
                                              style={"height": "450px"}),
                                    color="primary", type="border")
                    ], md=6),
                    dbc.Col([
                        dbc.Spinner(dcc.Graph(id="wing-length-distribution",
                                              style={"height": "450px"}),
                                    color="primary", type="border")
                    ], md=6),
                ], className="mb-4"),
                dbc.Row([
                    dbc.Col([
                        dbc.Spinner(dcc.Graph(id="age-distribution",
                                              style={"height": "450px"}),
                                    color="primary", type="border")
                    ], md=6),
                    dbc.Col([
                        dbc.Spinner(dcc.Graph(id="fat-score-distribution",
                                              style={"height": "450px"}),
                                    color="primary", type="border")
                    ], md=6),
                ]),
            ], fluid=True, className="py-4"),
        ], id="content-tab-morpho", style={"display": "none"}),

        # ── Fenologi tab ──────────────────────────────────────────────────
        html.Div([
            dbc.Container([
                html.H4("Analys av flyttningsfenologi", className="mt-3"),
                html.P("Utforska flyttningsmönster under året. Fåglar fångas under "
                       "vår- (norrut) och höstflyttningen (söderut).",
                       className="text-muted mb-4"),
                dbc.Spinner(dcc.Graph(id="phenology-weekly-plot",
                                      style={"height": "450px"}),
                            color="primary", type="border"),
                html.Div(className="mb-4"),
                dbc.Spinner(dcc.Graph(id="phenology-ridgeline-plot"),
                            color="primary", type="border"),
                html.Div(className="mb-4"),
                dbc.Spinner(dcc.Graph(id="phenology-seasonal-plot",
                                      style={"height": "450px"}),
                            color="primary", type="border"),
            ], fluid=True, className="py-4"),
        ], id="content-tab-phenology", style={"display": "none"}),

        # ── Värmekarta tab ────────────────────────────────────────────────
        html.Div([
            dbc.Container([
                dbc.Row([
                    dbc.Col([
                        html.Label("Välj år", className="fw-bold mb-2",
                                   style={"color": "#6c757d"}),
                        dcc.Dropdown(
                            id="heatmap-year-dropdown",
                            options=year_options,
                            value="all",
                            clearable=False,
                            style={"width": "300px"},
                        )
                    ], width="auto"),
                    dbc.Col([
                        html.Label("Antal arter", className="fw-bold mb-2",
                                   style={"color": "#6c757d"}),
                        dcc.Dropdown(
                            id="heatmap-top-n-dropdown",
                            options=[
                                {"label": "10",   "value": 10},
                                {"label": "30",   "value": 30},
                                {"label": "50",   "value": 50},
                                {"label": "100",  "value": 100},
                                {"label": "Alla", "value": 0},
                            ],
                            value=50,
                            clearable=False,
                            style={"width": "200px"},
                        )
                    ], width="auto"),
                ], className="mt-3 mb-3", align="end"),
                dbc.Spinner(dcc.Graph(id="weekly-heatmap"),
                            color="primary", type="border"),
            ], fluid=True, className="py-4"),
        ], id="content-tab-heatmap", style={"display": "none"}),

        # ── Väderanalys tab ───────────────────────────────────────────────
        html.Div([
            dbc.Container([
                html.H4("Väderanalys", className="mt-3"),
                html.P("Meteorologiska observationer från SMHI Nidingen A (station 71190) "
                       "kompletterade med Vinga A (station 71380) där Nidingen saknar data.",
                       className="text-muted mb-3"),
                dbc.Row([
                    dbc.Col([
                        html.Label("Datumintervall", className="fw-bold mb-2",
                                   style={"color": "#6c757d"}),
                        dcc.DatePickerRange(
                            id="weather-date-picker",
                            start_date=date_range[0],
                            end_date=date_range[1],
                            display_format="YYYY-MM-DD",
                        ),
                    ], md=6),
                ], className="mb-3"),
                dbc.Spinner(
                    dcc.Graph(id="weather-timeseries-plot"),
                    color="primary", type="border",
                    spinner_style={"width": "3rem", "height": "3rem"},
                ),
                dbc.Row([
                    dbc.Col([
                        html.Label("Välj variabler att visa", className="fw-bold mb-2",
                                   style={"color": "#6c757d"}),
                        dbc.Checklist(
                            id="weather-variable-checklist",
                            options=[
                                {"label": "🌡️ Temperatur (medel / min / max) — Nidingen A",         "value": "temperature"},
                                {"label": "💨 Vindhastighet & byvind — Nidingen A",                  "value": "wind"},
                                {"label": "🌧️ Nederbörd — Nidingen A (≤2007) + Vinga A (2007→)",  "value": "precipitation"},
                                {"label": "☁️ Molnighet — Nidingen A",                               "value": "cloud"},
                                {"label": "💧 Luftfuktighet — Nidingen A",                           "value": "humidity"},
                                {"label": "🔵 Lufttryck — Nidingen A (≤1995) + Vinga A (1996→)",    "value": "pressure"},
                                {"label": "👁️ Sikt (m) — Nidingen A (≤2007) + Vinga A (2007→)",    "value": "visibility"},
                            ],
                            value=["temperature", "wind", "precipitation",
                                   "visibility", "humidity", "cloud", "pressure"],
                            inline=False,
                            className="mb-3",
                        ),
                    ], md=12),
                ]),
            ], fluid=True, className="py-4"),
        ], id="content-tab-weather", style={"display": "none"}),

    ], id="main-content-area"),

    # ── FOOTER ────────────────────────────────────────────────────────────
    html.Div([
        html.Hr(className="my-3"),
        html.P("Nidingens Fågelstations Dashboard",
               className="text-muted small text-center pb-2"),
    ], style={"backgroundColor": "#f5f7fa"}),

], style={"minHeight": "100vh", "backgroundColor": "#f5f7fa"})


# Callbacks

# ── Tab visibility + filter panel show/hide ───────────────────────────────
@callback(
    [Output("content-tab-home", "style"),
     Output("content-tab-summary-timeseries", "style"),
     Output("content-tab-morpho", "style"),
     Output("content-tab-phenology", "style"),
     Output("content-tab-heatmap", "style"),
     Output("content-tab-weather", "style"),
     Output("filter-panel-outer", "style")],
    Input("tabs", "active_tab"),
)
def update_tab_visibility(active_tab):
    """Show only the active tab content; show filter panel unless on Hem/Väderanalys."""
    _TAB_IDS = [
        "tab-home",
        "tab-summary-timeseries",
        "tab-morpho",
        "tab-phenology",
        "tab-heatmap",
        "tab-weather",
    ]
    content_styles = [
        {"display": "block"} if tid == active_tab else {"display": "none"}
        for tid in _TAB_IDS
    ]
    # Home tab needs its background-image style preserved
    if active_tab == "tab-home":
        content_styles[0] = {"display": "flex"}

    filter_style = (
        {"display": "none"}
        if active_tab in ("tab-home", "tab-weather")
        else {"display": "block"}
    )
    return content_styles + [filter_style]


@callback(
    Output("filter-collapse", "is_open"),
    Input("filter-toggle-btn", "n_clicks"),
    State("filter-collapse", "is_open"),
    prevent_initial_call=True,
)
def toggle_filter_collapse(n_clicks, is_open):
    """Open / close the filter panel."""
    return not is_open


@callback(
    Output("time-series-plot", "figure"),
    [Input("species-dropdown", "value"),
     Input("aggregation-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date"),
     Input("plot-type-toggle", "value")]
)
def update_time_series(species_codes, aggregation, start_date, end_date, plot_type):
    """Update time series plot based on filters."""
    if not species_codes:
        return go.Figure().add_annotation(
            text="Vänligen välj minst en art",
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
    agg_text = aggregation.capitalize()
    if plot_type == "bar":
        # Bar chart with pastel colors
        fig = px.bar(
            df,
            x="period",
            y="count",
            color="swedish_name",
            title=f"Observationer över tid ({agg_text})",
            labels={"period": "Datum", "count": "Antal observationer", "swedish_name": "Art"},
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
            title=f"Observationer över tid ({agg_text})",
            labels={"period": "Datum", "count": "Antal observationer", "swedish_name": "Art"},
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
     Input("date-range-picker", "end_date")]
)
def update_summary(species_codes, start_date, end_date):
    """Update summary statistics."""
    # Convert date strings to proper date format for comparison
    if isinstance(start_date, str):
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
    else:
        start_date_obj = start_date
        
    if isinstance(end_date, str):
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
    else:
        end_date_obj = end_date

    selected_species = [code for code in (species_codes or []) if code != "TOTAL"]

    def _table_metrics(db, table_name):
        where_clauses = ["date >= ?", "date <= ?"]
        params = [start_date_obj, end_date_obj]

        if selected_species:
            placeholders = ", ".join(["?"] * len(selected_species))
            where_clauses.append(f"species_code IN ({placeholders})")
            params.extend(selected_species)

        where_sql = " AND ".join(where_clauses)
        row_count = db.conn.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE {where_sql}",
            params,
        ).fetchone()[0]
        return row_count
    
    with BirdRingingDB(DB_PATH, read_only=True) as db:
        ringon_rows = _table_metrics(db, "ringon")
        kontr_rows = _table_metrics(db, "kontr")
        fynd_rows = _table_metrics(db, "fynd")

        where_clauses = ["date >= ?", "date <= ?"]
        params = [start_date_obj, end_date_obj]
        if selected_species:
            placeholders = ", ".join(["?"] * len(selected_species))
            where_clauses.append(f"species_code IN ({placeholders})")
            params.extend(selected_species)
        where_sql = " AND ".join(where_clauses)

        unique_species = db.conn.execute(
            f"SELECT COUNT(DISTINCT species_code) FROM ringon WHERE {where_sql}",
            params,
        ).fetchone()[0]

        year_bounds = db.conn.execute(
            f"""
            SELECT
                MIN(EXTRACT(YEAR FROM date)) AS min_year,
                MAX(EXTRACT(YEAR FROM date)) AS max_year
            FROM ringon
            WHERE {where_sql}
            """,
            params,
        ).fetchone()

        if year_bounds and year_bounds[0] is not None and year_bounds[1] is not None:
            year_range_str = f"{int(year_bounds[0])}–{int(year_bounds[1])}"
        else:
            year_range_str = "No data"
    
    return dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-clipboard-list fa-2x mb-3", 
                              style={"color": "#B4D4E1"}),
                        html.H2(f"{ringon_rows:,}", 
                               className="mb-2",
                               style={"color": "#2c3e50", "fontWeight": "600"}),
                        html.P("Ringmärkningar",
                              className="text-muted mb-0",
                              style={"fontSize": "0.95rem"})
                    ], className="text-center")
                ])
            ], className="shadow-sm h-100", style={
                "borderRadius": "10px",
                "border": "none",
                "borderLeft": "4px solid #B4D4E1"
            })
        ], md=2, className="mb-3"),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-dove fa-2x mb-3", 
                              style={"color": "#C5E1B5"}),
                        html.H2(f"{kontr_rows:,}", 
                               className="mb-2",
                               style={"color": "#2c3e50", "fontWeight": "600"}),
                        html.P("Kontroller",
                              className="text-muted mb-0",
                              style={"fontSize": "0.95rem"})
                    ], className="text-center")
                ])
            ], className="shadow-sm h-100", style={
                "borderRadius": "10px",
                "border": "none",
                "borderLeft": "4px solid #C5E1B5"
            })
        ], md=2, className="mb-3"),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-hashtag fa-2x mb-3", 
                              style={"color": "#FFD4B8"}),
                        html.H2(f"{unique_species:,}", 
                               className="mb-2",
                               style={"color": "#2c3e50", "fontWeight": "600"}),
                        html.P("Unika arter",
                              className="text-muted mb-0",
                              style={"fontSize": "0.95rem"})
                    ], className="text-center")
                ])
            ], className="shadow-sm h-100", style={
                "borderRadius": "10px",
                "border": "none",
                "borderLeft": "4px solid #FFD4B8"
            })
        ], md=2, className="mb-3"),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-calendar-alt fa-2x mb-3", 
                              style={"color": "#E0C5E8"}),
                        html.H2(f"{fynd_rows:,}", 
                               className="mb-2",
                               style={"color": "#2c3e50", "fontWeight": "600"}),
                        html.P("Återfynd",
                              className="text-muted mb-0",
                              style={"fontSize": "0.95rem"})
                    ], className="text-center")
                ])
            ], className="shadow-sm h-100", style={
                "borderRadius": "10px",
                "border": "none",
                "borderLeft": "4px solid #E0C5E8"
            })
        ], md=2, className="mb-3"),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-calendar-alt fa-2x mb-3", 
                              style={"color": "#9ec5fe"}),
                        html.H2(year_range_str,
                               className="mb-2",
                               style={"color": "#2c3e50", "fontWeight": "600", "fontSize": "1.15rem"}),
                        html.P("År",
                              className="text-muted mb-0",
                              style={"fontSize": "0.95rem"})
                    ], className="text-center")
                ])
            ], className="shadow-sm h-100", style={
                "borderRadius": "10px",
                "border": "none",
                "borderLeft": "4px solid #9ec5fe"
            })
        ], md=4, className="mb-3"),
    ])


@callback(
    Output("weight-distribution", "figure"),
    [Input("species-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date")]
)
def update_weight_distribution(species_codes, start_date, end_date):
    """Update weight distribution plot."""
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

    # Clip each species to the 1.5×IQR fence to remove outlier tails
    _q1 = df.groupby("swedish_name")["weight"].transform("quantile", 0.25)
    _q3 = df.groupby("swedish_name")["weight"].transform("quantile", 0.75)
    _iqr = _q3 - _q1
    df = df[(df["weight"] >= _q1 - 1.5 * _iqr) & (df["weight"] <= _q3 + 1.5 * _iqr)]

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
    
    fig = px.violin(
        df,
        x="species_label",
        y="weight",
        color="species_label",
        points=False,
        title="Viktfördelning per art",
        labels={"species_label": "Art", "weight": "Vikt (g)"},
        color_discrete_sequence=PASTEL_COLORS,
        category_orders={"species_label": [f"{s}<br>(n={sample_sizes[s]})" for s in species_order]}
    )
    # bandwidth >= 0.5 g prevents oscillation from discrete/coarse weight recordings
    fig.update_traces(spanmode="hard", bandwidth=0.5)
    
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
     Input("date-range-picker", "end_date")]
)
def update_wing_distribution(species_codes, start_date, end_date):
    """Update wing length distribution plot."""
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

    # Clip each species to the 1.5×IQR fence to remove outlier tails
    _q1 = df.groupby("swedish_name")["wing_length"].transform("quantile", 0.25)
    _q3 = df.groupby("swedish_name")["wing_length"].transform("quantile", 0.75)
    _iqr = _q3 - _q1
    df = df[(df["wing_length"] >= _q1 - 1.5 * _iqr) & (df["wing_length"] <= _q3 + 1.5 * _iqr)]

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
    
    fig = px.violin(
        df,
        x="species_label",
        y="wing_length",
        color="species_label",
        points=False,
        title="Vinglängdsfördelning per art",
        labels={"species_label": "Art", "wing_length": "Vinglängd (mm)"},
        color_discrete_sequence=PASTEL_COLORS,
        category_orders={"species_label": [f"{s}<br>(n={sample_sizes[s]})" for s in species_order]}
    )
    # bandwidth=1.0 mm matches measurement resolution, prevents KDE oscillation artefacts
    fig.update_traces(spanmode="hard", bandwidth=1.0)
    
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
     Input("date-range-picker", "end_date")]
)
def update_age_distribution(species_codes, start_date, end_date):
    """Update age distribution plot showing percentage of age classes per species."""
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
            text="Ingen data tillgänglig",
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
                         'Andel (%): %{y:.1f}%<br>' +
                         '<extra></extra>'
        ))
    
    # Create ordered category list for x-axis
    ordered_labels = [species_labels[s] for s in species_order if s in species_labels]
    
    fig.update_layout(
        barmode='stack',
        title="Åldersfördelning per art",
        xaxis_title="Art",
        yaxis_title="Andel (%)",
        template="plotly_white",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50"),
        legend=dict(
            title="Åldersklass",
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
     Input("date-range-picker", "end_date")]
)
def update_fat_score_distribution(species_codes, start_date, end_date):
    """Update fat score distribution plot."""
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
            text="Ingen data tillgänglig",
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
                             'Fettpoäng (0-10): %{y:.2f}<br>' +
                             f"Std: {row['std_fat_score']:.2f}<br>" +
                             f"n={int(row['count'])}<br>" +
                             '<extra></extra>',
                showlegend=False
            ))
    
    # Create ordered category list for x-axis
    ordered_labels = [f"{s}<br>(n={int(fat_summary[fat_summary['swedish_name']==s]['count'].iloc[0])})" 
                     for s in species_order if s in fat_summary['swedish_name'].values]
    
    fig.update_layout(
        title="Genomsnittlig fettpoäng per art<br><sub>Felstaplar visar standardavvikelse</sub>",
        xaxis_title="Art",
        yaxis_title="Fettpoäng (0-10)",
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
    Output("morpho-bubble-chart", "figure"),
    [Input("species-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date")]
)
def update_morpho_bubble(species_codes, start_date, end_date):
    """Bubble chart: mean weight vs mean wing length per species.
    Bubble size = avg captures per year (log-scaled). Colour = family."""
    selected_species = {c for c in (species_codes or []) if c != "TOTAL"}

    with BirdRingingDB(DB_PATH, read_only=True) as db:
        df = db.execute_query(f"""
            SELECT
                r.species_code,
                r.swedish_name,
                COALESCE(m.family_english_name, 'Unknown') AS family_name,
                AVG(r.weight)      AS mean_weight,
                AVG(r.wing_length) AS mean_wing,
                COUNT(*)           AS total_captures,
                COUNT(DISTINCT EXTRACT(YEAR FROM r.date)) AS n_years
            FROM ring_records r
            LEFT JOIN species_metadata m ON r.swedish_name = m.swedish_name
            WHERE r.date BETWEEN '{start_date}' AND '{end_date}'
              AND r.weight > 0 AND r.weight IS NOT NULL
              AND r.wing_length > 0 AND r.wing_length IS NOT NULL
              AND r.species_code != 'TOTAL'
            GROUP BY r.species_code, r.swedish_name, family_name
            HAVING total_captures >= 10
            ORDER BY family_name, r.swedish_name
        """).pl().to_pandas()

    if df.empty:
        return go.Figure().add_annotation(
            text="Ingen data tillgänglig",
            showarrow=False, font={"size": 16, "color": "#95a5a6"}
        )

    import math
    df["avg_per_year"] = df["total_captures"] / df["n_years"].clip(lower=1)
    # Log-scale the size so that very common species don't dominate visually;
    # clamp minimum to 1 before log to avoid zero/negative values.
    df["bubble_size"] = df["avg_per_year"].clip(lower=1).apply(math.log).clip(lower=1) * 6

    # Assign a consistent colour to each family
    families = sorted(df["family_name"].unique())
    unknown_label = "Unknown"
    known_families = [f for f in families if f != unknown_label]
    colour_map = {f: PASTEL_COLORS[i % len(PASTEL_COLORS)] for i, f in enumerate(known_families)}
    colour_map[unknown_label] = "#cccccc"

    fig = go.Figure()
    shown_families = set()

    for _, row in df.iterrows():
        family = row["family_name"]
        colour = colour_map[family]
        show_legend = family not in shown_families
        shown_families.add(family)
        is_selected = row["species_code"] in selected_species
        border_color = "#dc3545" if is_selected else "rgba(80,80,80,0.4)"
        border_width = 3 if is_selected else 1

        fig.add_trace(go.Scatter(
            x=[row["mean_weight"]],
            y=[row["mean_wing"]],
            mode="markers",
            name=family,
            legendgroup=family,
            showlegend=show_legend,
            marker=dict(
                size=row["bubble_size"],
                color=colour,
                line=dict(width=border_width, color=border_color),
                opacity=0.85,
            ),
            customdata=[[row["swedish_name"], row["avg_per_year"], row["total_captures"], family]],
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Art: %{customdata[0]}<br>"
                "Vikt: %{x:.1f} g<br>"
                "Vinglängd: %{y:.1f} mm<br>"
                "Avg fångster/år: %{customdata[1]:.0f}<br>"
                "Totalt: %{customdata[2]:,}<br>"
                "Familj: %{customdata[3]}<extra></extra>"
            ),
        ))

    fig.update_layout(
        xaxis_title="Vikt (g)",
        yaxis_title="Vinglängd (mm)",
        template="plotly_white",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50"),
        legend=dict(
            title="Familj",
            orientation="v",
            bgcolor="rgba(255,255,255,0.85)",
            bordercolor="#dee2e6",
            borderwidth=1,
        ),
        hovermode="closest",
    )
    return fig


@callback(
    Output("phenology-weekly-plot", "figure"),
    [Input("species-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date")]
)
def update_phenology_weekly(species_codes, start_date, end_date):
    """Update weekly phenology plot showing migration patterns."""
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
                         'Vecka: %{x}<br>' +
                         'Genomsnittliga veckovisa observationer: %{y:.1f}<br>' +
                         '<extra></extra>'
        ))
    
    # Add season markers with pastel colors
    fig.add_vrect(
        x0=9, x1=22, 
        fillcolor="rgba(144, 238, 144, 0.15)", 
        line_width=0,
        annotation_text="Vårflyttning",
        annotation_position="top left",
        annotation=dict(font_size=11, font_color="#6c757d")
    )
    fig.add_vrect(
        x0=31, x1=43,
        fillcolor="rgba(255, 218, 185, 0.15)",
        line_width=0,
        annotation_text="Höstflyttning",
        annotation_position="top left",
        annotation=dict(font_size=11, font_color="#6c757d")
    )
    
    fig.update_layout(
        title=f"Veckovis observationsmönster ({start_year}-{end_year} medel)<br><sub>Visar både vår- och höstflyttningstoppar</sub>",
        xaxis_title="Vecka",
        yaxis_title="Genomsnittliga veckovisa observationer",
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
     Input("date-range-picker", "end_date")]
)
def update_phenology_ridgeline(species_codes, start_date, end_date):
    """Update ridgeline plot showing daily distribution by species."""
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
                hovertemplate='Dag på året: %{x}<br>Antal: %{y:.2f}<extra></extra>'
            ),
            row=idx,
            col=1
        )
        
        # Update y-axis for each subplot
        fig.update_yaxes(
            title_text="Antal",
            title_standoff=5,
            title_font_size=10,
            row=idx,
            col=1
        )
    
    fig.update_xaxes(title_text="Dag på året", row=n_species, col=1)
    
    fig.update_layout(
        height=220 * n_species,  # Increased height per species
        title_text=f"Daglig observationsfördelning ({start_year}-{end_year})<br><sub>Varje rad visar tidsfördelningen för en art</sub>",
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
     Input("date-range-picker", "end_date")]
)
def update_phenology_seasonal(species_codes, start_date, end_date):
    """Update spring vs autumn migration comparison plot."""
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
                             'Dag på året: %{x:.0f}<br>' +
                             f'IQR: %{{error_x.array:.0f}} days<br>' +
                             '%{text}<br>' +
                             '<extra></extra>'
            ))
    
    # Add month labels on y-axis
    months = [(1, "Jan"), (32, "Feb"), (60, "Mar"), (91, "Apr"), (121, "Maj"),
              (152, "Jun"), (182, "Jul"), (213, "Aug"), (244, "Sep"), (274, "Okt"),
              (305, "Nov"), (335, "Dec")]
    
    fig.update_layout(
        title=f"Vår- vs Höstflyttningstiming ({start_year}-{end_year})<br><sub>Punkter visar mediandag, felstaplar visar kvartilavstånd</sub>",
        yaxis_title="Art",
        xaxis_title="Dag på året",
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
     Input("heatmap-top-n-dropdown", "value")]
)
def update_weekly_heatmap(selected_year, top_n):
    """Update weekly heatmap showing normalized observations per species."""
    with BirdRingingDB(DB_PATH, read_only=True) as db:
        # Determine if we're showing all years or a specific year
        if selected_year == "all":
            year_param = None
            title_suffix = "Genomsnitt (Alla år)"
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
            text="Ingen data tillgänglig",
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
                text="% av totala<br>observationer",
                side="right"
            ),
            thickness=15,
            len=0.7
        ),
        hoverongaps=False,
        hovertemplate=(
            f'<b>%{{y}}</b><br>'
            'Vecka %{x}<br>'
            f'%{{z:.1f}}%<br>'
            f'{count_label}=%{{customdata:.1f}}'
            f'<extra></extra>'
        ),
    ))
    
    fig.update_layout(
        title=f"Veckovisa observationsmönster - {title_suffix}",
        xaxis=dict(
            title="Vecka",
            tickmode='linear',
            tick0=1,
            dtick=2,
            side='bottom'
        ),
        yaxis=dict(
            title="Art",
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
    [Input("weather-date-picker", "start_date"),
     Input("weather-date-picker", "end_date"),
     Input("weather-variable-checklist", "value")]
)
def update_weather_timeseries(start_date, end_date, selected_vars):
    """Update the weather time series plot for the selected date range and variables."""
    if not selected_vars:
        fig = go.Figure()
        fig.add_annotation(
            text="Vänligen välj minst en variabel",
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
            text="Ingen data tillgänglig",
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


# Expose Flask server for gunicorn: `gunicorn app:server`
server = app.server

if __name__ == "__main__":
    app.run(debug=True, port=8050)

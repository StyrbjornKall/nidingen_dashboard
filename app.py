"""
Bird Ringing Dashboard - Main Application

This is the main Dash application file for the bird ringing data dashboard.
It provides interactive visualizations for exploring bird observation data.
"""

import dash
from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
import polars as pl
from datetime import datetime, date

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

# Database path
DB_PATH = Path(__file__).parent / "data" / "bird_ringing.db"

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

# Prepare options for dropdowns
species_options = [
    {"label": f"{code} - {name}", "value": code} 
    for code, name in species_list
]

# Year options for heatmap (including "All Years" option)
year_options = [{"label": "Average (All Years)", "value": "all"}] + [
    {"label": str(year), "value": year} for year in available_years
]

# App Layout
app.layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.Div([
                html.H1([
                    html.I(className="fas fa-dove me-3"),
                    "Nidingen Bird Ringing Station"
                ], className="text-center mb-3", style={
                    "color": "#2c3e50",
                    "fontWeight": "600",
                    "fontSize": "2.5rem"
                }),
                html.P(
                    f"Monitoring bird migration patterns · Data from {date_range[0]} to {date_range[1]}", 
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
            html.H5([
                html.I(className="fas fa-filter me-2"),
                "Data Filters"
            ], className="mb-4", style={"color": "#495057"}),
            dbc.Row([
                dbc.Col([
                    html.Label("Select Species", className="fw-bold mb-2", style={"color": "#6c757d"}),
                    dcc.Dropdown(
                        id="species-dropdown",
                        options=species_options,
                        value=[species_options[0]["value"]] if species_options else [],
                        multi=True,
                        placeholder="Select one or more species...",
                        className="mb-3"
                    )
                ], md=6),
                
                dbc.Col([
                    html.Label("Time Aggregation", className="fw-bold mb-2", style={"color": "#6c757d"}),
                    dcc.Dropdown(
                        id="aggregation-dropdown",
                        options=[
                            {"label": "📅 Daily", "value": "daily"},
                            {"label": "📊 Weekly", "value": "weekly"},
                            {"label": "📈 Monthly", "value": "monthly"},
                            {"label": "📆 Yearly", "value": "yearly"}
                        ],
                        value="monthly",
                        clearable=False,
                        className="mb-3"
                    )
                ], md=6),
            ]),
            
            dbc.Row([
                dbc.Col([
                    html.Label("Date Range", className="fw-bold mb-2", style={"color": "#6c757d"}),
                    dcc.DatePickerRange(
                        id="date-range-picker",
                        start_date=date_range[0],
                        end_date=date_range[1],
                        display_format="YYYY-MM-DD",
                        style={"width": "100%"}
                    )
                ], md=12),
            ])
        ])
    ], className="mb-4 shadow-sm", style={"borderRadius": "10px", "border": "none"}),
    
    # Main Content - Tabs
    dbc.Card([
        dbc.CardBody([
            dbc.Tabs([
                # Time Series Tab
                dbc.Tab(label="📈 Time Series", tab_id="tab-timeseries", children=[
                    html.Div([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Plot Type", className="fw-bold me-3", style={"color": "#6c757d"}),
                                dbc.RadioItems(
                                    id="plot-type-toggle",
                                    options=[
                                        {"label": "Bar Chart", "value": "bar"},
                                        {"label": "Line Chart", "value": "line"}
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
                        )
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
                        ])
                    ], className="p-3")
                ]),
                
                # Phenology Tab
                dbc.Tab(label="🌸 Phenology", tab_id="tab-phenology", children=[
                    html.Div([
                        html.Div([
                            html.H4("Migration Phenology Analysis", className="text-center mb-2"),
                            html.P(
                                "Explore migration patterns throughout the year. Birds are captured during spring (northward) and autumn (southward) migration periods.",
                                className="text-center text-muted mb-4"
                            ),
                        ], className="mt-3"),
                        
                        # Weekly Distribution
                        html.Div([
                            html.H5([
                                html.I(className="fas fa-chart-area me-2"),
                                "Weekly Observation Pattern"
                            ], className="mb-3", style={"color": "#495057"}),
                            dbc.Spinner(
                                dcc.Graph(id="phenology-weekly-plot", style={"height": "450px"}),
                                color="primary",
                                type="border"
                            )
                        ], className="mb-4"),
                        
                        # Ridgeline Plot
                        html.Div([
                            html.H5([
                                html.I(className="fas fa-layer-group me-2"),
                                "Daily Distribution by Species"
                            ], className="mb-3", style={"color": "#495057"}),
                            dbc.Spinner(
                                dcc.Graph(id="phenology-ridgeline-plot"),
                                color="primary",
                                type="border"
                            )
                        ], className="mb-4"),
                        
                        # Seasonal Comparison
                        html.Div([
                            html.H5([
                                html.I(className="fas fa-exchange-alt me-2"),
                                "Spring vs Autumn Migration Windows"
                            ], className="mb-3", style={"color": "#495057"}),
                            dbc.Spinner(
                                dcc.Graph(id="phenology-seasonal-plot", style={"height": "450px"}),
                                color="primary",
                                type="border"
                            )
                        ], className="mb-4"),
                        
                        # Year-over-Year
                        html.Div([
                            html.H5([
                                html.I(className="fas fa-calendar-alt me-2"),
                                "Year-over-Year Comparison"
                            ], className="mb-3", style={"color": "#495057"}),
                            dbc.Spinner(
                                dcc.Graph(id="phenology-yearly-plot"),
                                color="primary",
                                type="border"
                            )
                        ], className="mb-4")
                    ], className="p-3")
                ]),
                
                # Weekly Heatmap Tab
                dbc.Tab(label="🔥 Weekly Heatmap", tab_id="tab-heatmap", children=[
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
                            ])
                        ], className="mt-3 mb-3"),
                        dbc.Spinner(
                            dcc.Graph(id="weekly-heatmap", style={"height": "800px"}),
                            color="primary",
                            type="border"
                        )
                    ], className="p-3")
                ]),
                
                # Weather Analysis Tab
                dbc.Tab(label="🌤️ Weather Analysis", tab_id="tab-weather", children=[
                    html.Div([
                        html.Div([
                            html.H4("Weather Analysis", className="text-center mb-2"),
                            html.P(
                                "Meteorological observations from SMHI Nidingen A (station 71190) "
                                "supplemented by Vinga A (station 71380) where Nidingen lacks data.",
                                className="text-center text-muted mb-3"
                            ),
                            # Data source legend
                            dbc.Row([
                                dbc.Col([
                                    dbc.Alert([
                                        dbc.Row([
                                            dbc.Col([
                                                html.Strong("🏝️ Nidingen A (71190) · primary station"),
                                                html.Br(),
                                                html.Small(
                                                    "Temperature, wind, humidity, cloud cover, gust wind: full archive 1980–present. "
                                                    "Precipitation: 1980–2007. Pressure: 1980–1995. Visibility: 1980–2007.",
                                                    className="text-muted"
                                                ),
                                            ], md=6),
                                            dbc.Col([
                                                html.Strong("⚓ Vinga A (71380) · supplementary station"),
                                                html.Br(),
                                                html.Small(
                                                    "Precipitation: 2007–present (gap-fills Nidingen). "
                                                    "Pressure: 1996–present (gap-fills Nidingen). "
                                                    "Visibility: 2007–present (gap-fills Nidingen). "
                                                    "Full Vinga archive stored separately in weather_data_vinga.",
                                                    className="text-muted"
                                                ),
                                            ], md=6),
                                        ])
                                    ], color="light", className="mb-3 py-2 px-3",
                                       style={"borderRadius": "8px", "fontSize": "0.88rem"})
                                ], md=12)
                            ]),
                        ], className="mt-3"),

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
                                        {"label": "🌡️ Temperature (mean / min / max) — Nidingen A",    "value": "temperature"},
                                        {"label": "💨 Wind speed & gusts — Nidingen A",                 "value": "wind"},
                                        {"label": "🌧️ Precipitation — Nidingen A (≤2007) + Vinga A (2007→)", "value": "precipitation"},
                                        {"label": "☁️ Cloud cover — Nidingen A",                       "value": "cloud"},
                                        {"label": "💧 Humidity — Nidingen A",                           "value": "humidity"},
                                        {"label": "🔵 Pressure — Nidingen A (≤1995) + Vinga A (1996→)", "value": "pressure"},
                                        {"label": "👁️ Visibility (m) — Nidingen A (≤2007) + Vinga A (2007→)", "value": "visibility"},
                                    ],
                                    value=["temperature", "wind", "precipitation"],
                                    inline=False,
                                    className="mb-3",
                                ),
                            ], md=12),
                        ]),

                        # Time series plot
                        html.Div([
                            html.H5([
                                html.I(className="fas fa-chart-line me-2"),
                                "Daily Weather Time Series"
                            ], className="mb-3", style={"color": "#495057"}),
                            dbc.Spinner(
                                dcc.Graph(id="weather-timeseries-plot"),
                                color="primary",
                                type="border",
                                spinner_style={"width": "3rem", "height": "3rem"}
                            ),
                        ], className="mb-4"),
                    ], className="p-3")
                ]),

                # Summary Tab
                dbc.Tab(label="📋 Summary", tab_id="tab-summary", children=[
                    html.Div(id="summary-stats", className="p-4")
                ])
            ], id="tabs", active_tab="tab-timeseries")
        ])
    ], className="shadow-sm", style={"borderRadius": "10px", "border": "none"}),
    
    # Footer
    dbc.Row([
        dbc.Col([
            html.Hr(className="my-4"),
            html.P([
                "Nidingen Bird Ringing Station Dashboard · ",
                html.A("View on GitHub", href="#", className="text-decoration-none"),
                " · Built with Dash & Plotly"
            ], className="text-center text-muted small")
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
     Input("plot-type-toggle", "value")]
)
def update_time_series(species_codes, aggregation, start_date, end_date, plot_type):
    """Update time series plot based on filters."""
    if not species_codes:
        return go.Figure().add_annotation(
            text="Please select at least one species",
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
    if plot_type == "bar":
        # Bar chart with pastel colors
        fig = px.bar(
            df,
            x="period",
            y="count",
            color="swedish_name",
            title=f"Species Observations Over Time ({aggregation.capitalize()})",
            labels={"period": "Date", "count": "Number of Observations", "swedish_name": "Species"},
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
            title=f"Species Observations Over Time ({aggregation.capitalize()})",
            labels={"period": "Date", "count": "Number of Observations", "swedish_name": "Species"},
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
    
    # Create mapping from species_code to swedish_name
    species_name_map = df[['species_code', 'swedish_name']].drop_duplicates().set_index('species_code')['swedish_name'].to_dict()
    
    # Get species order based on dropdown selection order
    species_order = [species_name_map[code] for code in species_codes if code in species_name_map]
    
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
        title="Weight Distribution by Species",
        labels={"species_label": "Species", "weight": "Weight (g)"},
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
    
    # Create mapping from species_code to swedish_name
    species_name_map = df[['species_code', 'swedish_name']].drop_duplicates().set_index('species_code')['swedish_name'].to_dict()
    
    # Get species order based on dropdown selection order
    species_order = [species_name_map[code] for code in species_codes if code in species_name_map]
    
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
        title="Wing Length Distribution by Species",
        labels={"species_label": "Species", "wing_length": "Wing Length (mm)"},
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
            text="No age data available for selected filters",
            showarrow=False,
            font={"size": 16, "color": "#95a5a6"}
        )
    
    # Create mapping from species_code to swedish_name
    species_name_map = df[['species_code', 'swedish_name']].drop_duplicates().set_index('species_code')['swedish_name'].to_dict()
    
    # Get species order based on dropdown selection order
    species_order = [species_name_map[code] for code in species_codes if code in species_name_map]
    
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
                         'Percentage: %{y:.1f}%<br>' +
                         '<extra></extra>'
        ))
    
    # Create ordered category list for x-axis
    ordered_labels = [species_labels[s] for s in species_order if s in species_labels]
    
    fig.update_layout(
        barmode='stack',
        title="Age Distribution by Species",
        xaxis_title="Species",
        yaxis_title="Percentage (%)",
        template="plotly_white",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Arial, sans-serif", size=12, color="#495057"),
        title_font=dict(size=18, color="#2c3e50"),
        legend=dict(
            title="Age Class",
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
            text="No fat score data available for selected filters",
            showarrow=False,
            font={"size": 16, "color": "#95a5a6"}
        )
    
    # Create mapping from species_code to swedish_name
    species_name_map = df[['species_code', 'swedish_name']].drop_duplicates().set_index('species_code')['swedish_name'].to_dict()
    
    # Get species order based on dropdown selection order
    species_order = [species_name_map[code] for code in species_codes if code in species_name_map]
    
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
                             'Mean fat score: %{y:.2f}<br>' +
                             f"Std: {row['std_fat_score']:.2f}<br>" +
                             f"n={int(row['count'])}<br>" +
                             '<extra></extra>',
                showlegend=False
            ))
    
    # Create ordered category list for x-axis
    ordered_labels = [f"{s}<br>(n={int(fat_summary[fat_summary['swedish_name']==s]['count'].iloc[0])})" 
                     for s in species_order if s in fat_summary['swedish_name'].values]
    
    fig.update_layout(
        title="Average Fat Score by Species<br><sub>Error bars show standard deviation</sub>",
        xaxis_title="Species",
        yaxis_title="Fat Score (0-10)",
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
    
    for idx, species in enumerate(df['swedish_name'].unique()):
        species_df = df[df['swedish_name'] == species]
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
                         'Week: %{x}<br>' +
                         'Avg observations: %{y:.1f}<br>' +
                         '<extra></extra>'
        ))
    
    # Add season markers with pastel colors
    fig.add_vrect(
        x0=9, x1=22, 
        fillcolor="rgba(144, 238, 144, 0.15)", 
        line_width=0,
        annotation_text="Spring Migration",
        annotation_position="top left",
        annotation=dict(font_size=11, font_color="#6c757d")
    )
    fig.add_vrect(
        x0=31, x1=43,
        fillcolor="rgba(255, 218, 185, 0.15)",
        line_width=0,
        annotation_text="Autumn Migration",
        annotation_position="top left",
        annotation=dict(font_size=11, font_color="#6c757d")
    )
    
    fig.update_layout(
        title=f"Weekly Observation Pattern ({start_year}-{end_year} average)<br>" +
              "<sub>Shows both spring and autumn migration peaks</sub>",
        xaxis_title="Week of Year",
        yaxis_title="Average Weekly Observations",
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
    
    species_list = df['swedish_name'].unique()
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
                hovertemplate='Day: %{x}<br>Avg count: %{y:.2f}<extra></extra>'
            ),
            row=idx,
            col=1
        )
        
        # Update y-axis for each subplot
        fig.update_yaxes(
            title_text="Count",
            title_standoff=5,
            title_font_size=10,
            row=idx,
            col=1
        )
    
    fig.update_xaxes(title_text="Day of Year", row=n_species, col=1)
    
    fig.update_layout(
        height=220 * n_species,  # Increased height per species
        title_text=f"Daily Observation Distribution ({start_year}-{end_year})<br>" +
                   "<sub>Each row shows the temporal distribution for one species</sub>",
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
                x=season_df['swedish_name'],
                y=season_df['median'],
                error_y=dict(
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
                hovertemplate='<b>%{x}</b><br>' +
                             season.capitalize() + ' migration<br>' +
                             'Median day: %{y:.0f}<br>' +
                             'IQR: %{error_y.array:.0f} days<br>' +
                             '%{text}<br>' +
                             '<extra></extra>'
            ))
    
    # Add month labels on y-axis
    months = [(1, "Jan"), (32, "Feb"), (60, "Mar"), (91, "Apr"), (121, "May"), 
              (152, "Jun"), (182, "Jul"), (213, "Aug"), (244, "Sep"), (274, "Oct"), 
              (305, "Nov"), (335, "Dec")]
    
    fig.update_layout(
        title=f"Spring vs Autumn Migration Timing ({start_year}-{end_year})<br>" +
              "<sub>Points show median day of year, error bars show interquartile range</sub>",
        xaxis_title="Species",
        yaxis_title="Day of Year",
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
    
    fig.update_yaxes(
        tickmode='array',
        tickvals=[m[0] for m in months],
        ticktext=[m[1] for m in months]
    )
    
    return fig


@callback(
    Output("phenology-yearly-plot", "figure"),
    [Input("species-dropdown", "value"),
     Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date")]
)
def update_phenology_yearly(species_codes, start_date, end_date):
    """Update year-over-year comparison plot."""
    if not species_codes:
        return go.Figure()
    
    with BirdRingingDB(DB_PATH, read_only=True) as db:
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        
        query = BirdRingingQueries.get_phenology_weekly_distribution(
            species_codes=species_codes,
            start_year=start_year,
            end_year=end_year,
            aggregate_years=False  # Keep years separate
        )
        df = db.execute_query(query).pl().to_pandas()
    
    from plotly.subplots import make_subplots
    
    species_list = df['swedish_name'].unique()
    n_species = len(species_list)
    
    fig = make_subplots(
        rows=n_species,
        cols=1,
        subplot_titles=[f"<b>{sp}</b>" for sp in species_list],
        vertical_spacing=0.05,
        shared_xaxes=True
    )
    
    for idx, species in enumerate(species_list, start=1):
        species_df = df[df['swedish_name'] == species]
        
        for year_idx, year in enumerate(sorted(species_df['year'].unique())):
            year_df = species_df[species_df['year'] == year]
            color = PASTEL_COLORS[year_idx % len(PASTEL_COLORS)]
            
            fig.add_trace(
                go.Scatter(
                    x=year_df['week_of_year'],
                    y=year_df['count'],
                    mode='lines',
                    name=str(year),
                    line=dict(width=3, color=color),
                    showlegend=(idx == 1),  # Only show legend for first subplot
                    legendgroup=str(year),
                    hovertemplate=f'{year}<br>Week: %{{x}}<br>Count: %{{y}}<extra></extra>'
                ),
                row=idx,
                col=1
            )
        
        fig.update_yaxes(title_text="Count", row=idx, col=1)
    
    fig.update_xaxes(title_text="Week of Year", row=n_species, col=1)
    
    fig.update_layout(
        height=250 * n_species,
        title_text=f"Year-over-Year Weekly Patterns ({start_year}-{end_year})<br>" +
                   "<sub>Compare migration timing shifts between years</sub>",
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
    
    with BirdRingingDB(DB_PATH, read_only=True) as db:
        if species_codes:
            df = db.get_data_as_polars(
                filters={"species_code": species_codes}
            )
            # Filter by date using proper date objects
            df = df.filter(
                (pl.col("date") >= start_date_obj) & (pl.col("date") <= end_date_obj)
            )
        else:
            df = db.get_data_as_polars()
            df = df.filter(
                (pl.col("date") >= start_date_obj) & (pl.col("date") <= end_date_obj)
            )
        
        total_records = len(df)
        unique_species = df["species_code"].n_unique()
        unique_individuals = df["ring_number"].n_unique()
        date_range_str = f"{df['date'].min()} to {df['date'].max()}"
    
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
                        html.P("Total Observations", 
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
                        html.P("Unique Species", 
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
                        html.P("Unique Individuals", 
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
                        html.P("Date Range", 
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
    Output("weekly-heatmap", "figure"),
    Input("heatmap-year-dropdown", "value")
)
def update_weekly_heatmap(selected_year):
    """Update weekly heatmap showing normalized observations per species."""
    with BirdRingingDB(DB_PATH, read_only=True) as db:
        # Determine if we're showing all years or a specific year
        if selected_year == "all":
            year_param = None
            title_suffix = "(Average Across All Years)"
        else:
            year_param = int(selected_year)
            title_suffix = f"({selected_year})"
        
        # Get heatmap data
        query = BirdRingingQueries.get_weekly_heatmap_data(
            year=year_param,
            top_n_species=30
        )
        df = db.execute_query(query).pl()
    
    if len(df) == 0:
        return go.Figure().add_annotation(
            text="No data available for selected year",
            showarrow=False,
            font={"size": 20, "color": "#95a5a6"}
        )
    
    # Convert to pandas for easier pivoting
    df_pd = df.to_pandas()
    
    # Create a pivot table: species (rows) x weeks (columns)
    pivot_data = df_pd.pivot_table(
        index='swedish_name',
        columns='week_of_year',
        values='percent_of_total',
        fill_value=0
    )
    
    # Sort species by total observations (preserved from query order)
    species_order = df_pd.groupby('swedish_name')['percent_of_total'].sum().sort_values(ascending=False).index
    pivot_data = pivot_data.reindex(species_order)
    
    # Ensure all weeks 1-52 are present
    all_weeks = list(range(1, 53))
    for week in all_weeks:
        if week not in pivot_data.columns:
            pivot_data[week] = 0
    pivot_data = pivot_data[sorted(pivot_data.columns)]
    
    # Create heatmap with pastel color scheme
    fig = go.Figure(data=go.Heatmap(
        z=pivot_data.values,
        x=pivot_data.columns,
        y=pivot_data.index,
        colorscale=[
            [0.0, '#ffffff'],      # White for zero
            [0.2, '#FFE8B8'],      # Pastel yellow
            [0.4, '#FFD4B8'],      # Pastel orange
            [0.6, '#FFB8C3'],      # Pastel pink
            [0.8, '#E0C5E8'],      # Pastel purple
            [1.0, '#B4D4E1']       # Pastel blue
        ],
        colorbar=dict(
            title=dict(
                text="% of Total<br>Observations",
                side="right"
            ),
            thickness=15,
            len=0.7
        ),
        hoverongaps=False,
        hovertemplate='<b>%{y}</b><br>Week %{x}<br>%{z:.1f}% of observations<extra></extra>'
    ))
    
    fig.update_layout(
        title=f"Weekly Observation Patterns - Top 30 Species {title_suffix}",
        xaxis=dict(
            title="Week of Year",
            tickmode='linear',
            tick0=1,
            dtick=2,
            side='bottom'
        ),
        yaxis=dict(
            title="Species (Swedish Name)",
            tickfont=dict(size=10)
        ),
        height=800,
        template="plotly_white",
        font=dict(size=11, family="Arial, sans-serif", color="#495057"),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        title_font=dict(size=18, color="#2c3e50")
    )
    
    return fig


@callback(
    Output("weather-timeseries-plot", "figure"),
    [Input("date-range-picker", "start_date"),
     Input("date-range-picker", "end_date"),
     Input("weather-variable-checklist", "value")]
)
def update_weather_timeseries(start_date, end_date, selected_vars):
    """Update the weather time series plot for the selected date range and variables."""
    if not selected_vars:
        fig = go.Figure()
        fig.add_annotation(
            text="Please select at least one variable",
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
            text="No weather data available for the selected period",
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


if __name__ == "__main__":
    app.run(debug=True, port=8050)

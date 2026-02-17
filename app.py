"""
Bird Ringing Dashboard - Main Application

This is the main Dash application file for the bird ringing data dashboard.
It provides interactive visualizations for exploring bird observation data.
"""

import dash
from dash import dcc, html, Input, Output, callback
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

# Initialize Dash app
app = dash.Dash(
    __name__,
    title="Bird Ringing Dashboard - Nidingen",
    update_title="Loading..."
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
app.layout = html.Div([
    html.Div([
        html.H1("🐦 Nidingen Bird Ringing Station Dashboard", 
                style={"textAlign": "center", "color": "#2c3e50"}),
        html.P(f"Data from {date_range[0]} to {date_range[1]}", 
               style={"textAlign": "center", "color": "#7f8c8d"}),
    ], style={"padding": "20px", "backgroundColor": "#ecf0f1"}),
    
    # Filters Section
    html.Div([
        html.Div([
            html.Label("Select Species:", style={"fontWeight": "bold"}),
            dcc.Dropdown(
                id="species-dropdown",
                options=species_options,
                value=[species_options[0]["value"]] if species_options else [],
                multi=True,
                placeholder="Select species..."
            )
        ], style={"width": "48%", "display": "inline-block", "padding": "10px"}),
        
        html.Div([
            html.Label("Time Aggregation:", style={"fontWeight": "bold"}),
            dcc.Dropdown(
                id="aggregation-dropdown",
                options=[
                    {"label": "Daily", "value": "daily"},
                    {"label": "Weekly", "value": "weekly"},
                    {"label": "Monthly", "value": "monthly"},
                    {"label": "Yearly", "value": "yearly"}
                ],
                value="monthly",
                clearable=False
            )
        ], style={"width": "48%", "display": "inline-block", "padding": "10px"}),
        
        html.Div([
            html.Label("Date Range:", style={"fontWeight": "bold"}),
            dcc.DatePickerRange(
                id="date-range-picker",
                start_date=date_range[0],
                end_date=date_range[1],
                display_format="YYYY-MM-DD"
            )
        ], style={"padding": "10px"}),
    ], style={"backgroundColor": "#f8f9fa", "padding": "20px", "margin": "20px"}),
    
    # Main Content - Tabs
    dcc.Tabs([
        # Time Series Tab
        dcc.Tab(label="📈 Time Series", children=[
            html.Div([
                # Toggle for plot type
                html.Div([
                    html.Label("Plot Type:", style={"fontWeight": "bold", "marginRight": "10px"}),
                    dcc.RadioItems(
                        id="plot-type-toggle",
                        options=[
                            {"label": " Bar Chart", "value": "bar"},
                            {"label": " Line Chart", "value": "line"}
                        ],
                        value="bar",
                        inline=True,
                        style={"display": "inline-block"}
                    )
                ], style={"padding": "10px 20px 0px 20px"}),
                
                dcc.Loading(
                    dcc.Graph(id="time-series-plot"),
                    type="circle"
                )
            ], style={"padding": "20px"})
        ]),
        
        # Morphometrics Tab
        dcc.Tab(label="📊 Morphometrics", children=[
            html.Div([
                html.Div([
                    dcc.Loading(
                        dcc.Graph(id="weight-distribution"),
                        type="circle"
                    )
                ], style={"width": "48%", "display": "inline-block"}),
                
                html.Div([
                    dcc.Loading(
                        dcc.Graph(id="wing-length-distribution"),
                        type="circle"
                    )
                ], style={"width": "48%", "display": "inline-block"}),
            ], style={"padding": "20px"})
        ]),
        
        # Phenology Tab
        dcc.Tab(label="🌸 Phenology", children=[
            html.Div([
                html.H3("Migration Phenology Analysis", style={"textAlign": "center"}),
                html.P("Explore migration patterns throughout the year. Birds are captured during spring (northward) and autumn (southward) migration periods.",
                       style={"textAlign": "center", "color": "#666"}),
                
                # Weekly Distribution Plot
                html.Div([
                    html.H4("📊 Weekly Observation Pattern", style={"marginTop": "20px"}),
                    dcc.Loading(
                        dcc.Graph(id="phenology-weekly-plot"),
                        type="circle"
                    )
                ]),
                
                # Ridgeline Plot
                html.Div([
                    html.H4("📈 Daily Distribution by Species", style={"marginTop": "30px"}),
                    dcc.Loading(
                        dcc.Graph(id="phenology-ridgeline-plot"),
                        type="circle"
                    )
                ]),
                
                # Spring vs Autumn Comparison
                html.Div([
                    html.H4("🔄 Spring vs Autumn Migration Windows", style={"marginTop": "30px"}),
                    dcc.Loading(
                        dcc.Graph(id="phenology-seasonal-plot"),
                        type="circle"
                    )
                ]),
                
                # Year-over-Year Comparison
                html.Div([
                    html.H4("📅 Year-over-Year Comparison", style={"marginTop": "30px"}),
                    dcc.Loading(
                        dcc.Graph(id="phenology-yearly-plot"),
                        type="circle"
                    )
                ])
            ], style={"padding": "20px"})
        ]),
        
        # Weekly Heatmap Tab
        dcc.Tab(label="🔥 Weekly Heatmap", children=[
            html.Div([
                html.Div([
                    html.Label("Select Year:", style={"fontWeight": "bold"}),
                    dcc.Dropdown(
                        id="heatmap-year-dropdown",
                        options=year_options,
                        value="all",
                        clearable=False,
                        style={"width": "300px"}
                    )
                ], style={"padding": "10px 20px"}),
                dcc.Loading(
                    dcc.Graph(id="weekly-heatmap", style={"height": "800px"}),
                    type="circle"
                )
            ], style={"padding": "20px"})
        ]),
        
        # Summary Tab
        dcc.Tab(label="📋 Summary", children=[
            html.Div([
                html.Div(id="summary-stats", style={"padding": "20px"})
            ])
        ])
    ])
])


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
            font={"size": 20}
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
        # Bar chart
        fig = px.bar(
            df,
            x="period",
            y="count",
            color="swedish_name",
            title=f"Species Observations Over Time ({aggregation.capitalize()})",
            labels={"period": "Date", "count": "Number of Observations", "swedish_name": "Species"},
            barmode="group"  # Groups bars side by side for multiple species
        )
        
        # Set bar width based on aggregation level and number of data points
        num_periods = df['period'].nunique()
        
        if aggregation == "daily":
            # For daily data, use a fixed pixel width for better visibility
            # Calculate appropriate width based on time range
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
            bargap=bargap,  # Gap between bars of the same position
            bargroupgap=0.05  # Gap between groups
        )
    else:
        # Line chart with adaptive mode based on data density
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
            labels={"period": "Date", "count": "Number of Observations", "swedish_name": "Species"}
        )
        
        # Update traces to use appropriate mode
        fig.update_traces(
            mode=mode,
            marker=dict(size=marker_size),
            line=dict(width=2),
            connectgaps=False  # Don't connect over missing data
        )
    
    fig.update_layout(
        hovermode="x unified",
        template="plotly_white"
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
        labels={"species_label": "Species", "weight": "Weight (g)"}
    )
    
    fig.update_layout(template="plotly_white", showlegend=False)
    
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
        labels={"species_label": "Species", "wing_length": "Wing Length (mm)"}
    )
    
    fig.update_layout(template="plotly_white", showlegend=False)
    
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
    
    for species in df['swedish_name'].unique():
        species_df = df[df['swedish_name'] == species]
        
        fig.add_trace(go.Scatter(
            x=species_df['week_of_year'],
            y=species_df['avg_count'],
            mode='lines',
            name=species,
            fill='tozeroy',
            line=dict(width=2),
            hovertemplate='<b>%{fullData.name}</b><br>' +
                         'Week: %{x}<br>' +
                         'Avg observations: %{y:.1f}<br>' +
                         '<extra></extra>'
        ))
    
    # Add season markers
    fig.add_vrect(
        x0=9, x1=22, 
        fillcolor="rgba(144, 238, 144, 0.1)", 
        line_width=0,
        annotation_text="Spring Migration",
        annotation_position="top left"
    )
    fig.add_vrect(
        x0=31, x1=43,
        fillcolor="rgba(255, 218, 185, 0.1)",
        line_width=0,
        annotation_text="Autumn Migration",
        annotation_position="top left"
    )
    
    fig.update_layout(
        title=f"Weekly Observation Pattern ({start_year}-{end_year} average)<br>" +
              "<sub>Shows both spring and autumn migration peaks</sub>",
        xaxis_title="Week of Year",
        yaxis_title="Average Weekly Observations",
        template="plotly_white",
        hovermode="x unified",
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99
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
    
    fig = make_subplots(
        rows=n_species, 
        cols=1,
        subplot_titles=[f"<b>{sp}</b>" for sp in species_list],
        vertical_spacing=0.03,
        shared_xaxes=True
    )
    
    for idx, species in enumerate(species_list, start=1):
        species_df = df[df['swedish_name'] == species]
        
        fig.add_trace(
            go.Scatter(
                x=species_df['day_of_year'],
                y=species_df['avg_count'],
                mode='lines',
                fill='tozeroy',
                line=dict(width=1.5, color='steelblue'),
                fillcolor='rgba(70, 130, 180, 0.3)',
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
        height=200 * n_species,
        title_text=f"Daily Observation Distribution ({start_year}-{end_year})<br>" +
                   "<sub>Each row shows the temporal distribution for one species</sub>",
        template="plotly_white"
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
    
    for season, color, symbol in [('spring', 'green', 'circle'), ('autumn', 'orange', 'square')]:
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
                marker=dict(size=12, symbol=symbol, color=color),
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
        hovermode="closest"
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
    
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    
    for idx, species in enumerate(species_list, start=1):
        species_df = df[df['swedish_name'] == species]
        
        for year_idx, year in enumerate(sorted(species_df['year'].unique())):
            year_df = species_df[species_df['year'] == year]
            
            fig.add_trace(
                go.Scatter(
                    x=year_df['week_of_year'],
                    y=year_df['count'],
                    mode='lines',
                    name=str(year),
                    line=dict(width=2, color=colors[year_idx % len(colors)]),
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
        hovermode="x unified"
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
    
    return html.Div([
        html.H3("Summary Statistics"),
        html.Div([
            html.Div([
                html.H2(f"{total_records:,}", style={"color": "#3498db"}),
                html.P("Total Observations")
            ], style={"textAlign": "center", "padding": "20px", "backgroundColor": "#ecf0f1", 
                     "margin": "10px", "borderRadius": "5px", "width": "22%", "display": "inline-block"}),
            
            html.Div([
                html.H2(f"{unique_species}", style={"color": "#2ecc71"}),
                html.P("Unique Species")
            ], style={"textAlign": "center", "padding": "20px", "backgroundColor": "#ecf0f1",
                     "margin": "10px", "borderRadius": "5px", "width": "22%", "display": "inline-block"}),
            
            html.Div([
                html.H2(f"{unique_individuals:,}", style={"color": "#e74c3c"}),
                html.P("Unique Individuals")
            ], style={"textAlign": "center", "padding": "20px", "backgroundColor": "#ecf0f1",
                     "margin": "10px", "borderRadius": "5px", "width": "22%", "display": "inline-block"}),
            
            html.Div([
                html.H2(date_range_str, style={"color": "#9b59b6", "fontSize": "16px"}),
                html.P("Date Range")
            ], style={"textAlign": "center", "padding": "20px", "backgroundColor": "#ecf0f1",
                     "margin": "10px", "borderRadius": "5px", "width": "22%", "display": "inline-block"}),
        ])
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
            font={"size": 20}
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
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot_data.values,
        x=pivot_data.columns,
        y=pivot_data.index,
        colorscale='YlOrRd',
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
        font=dict(size=11)
    )
    
    return fig


if __name__ == "__main__":
    app.run(debug=True, port=8050)

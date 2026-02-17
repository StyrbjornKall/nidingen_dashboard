"""
Test script demonstrating improved phenology visualizations.
Shows multiple approaches to display migration patterns including bimodal distributions.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from db_manager import BirdRingingDB
from query_utils import BirdRingingQueries
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

DB_PATH = "data/bird_ringing.db"

# Select some representative species
SPECIES = ['GÄSMY', 'BLMES', 'RÖHAK', 'JÄSPA', 'SKPIP']

print("=" * 80)
print("PHENOLOGY VISUALIZATION OPTIONS")
print("=" * 80)

# ============================================================================
# OPTION 1: Weekly distribution with area plot (shows bimodal patterns clearly)
# ============================================================================
print("\n1. Weekly Distribution Across the Year (RECOMMENDED)")
print("-" * 80)

with BirdRingingDB(DB_PATH, read_only=True) as db:
    query = BirdRingingQueries.get_phenology_weekly_distribution(
        species_codes=SPECIES,
        start_year=2020,
        aggregate_years=True
    )
    df_weekly = db.execute_query(query).pl().to_pandas()

fig1 = go.Figure()

for species in df_weekly['swedish_name'].unique():
    species_df = df_weekly[df_weekly['swedish_name'] == species]
    
    fig1.add_trace(go.Scatter(
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

fig1.update_layout(
    title="Phenology: Weekly Observation Pattern (2020-2024 average)<br>" +
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

# Add season markers
seasons = [
    dict(x0=9, x1=22, fillcolor="rgba(144, 238, 144, 0.1)", line_width=0, name="Spring"),
    dict(x0=31, x1=43, fillcolor="rgba(255, 218, 185, 0.1)", line_width=0, name="Autumn")
]

for season in seasons:
    fig1.add_vrect(**season, annotation_text=season['name'], 
                  annotation_position="top left")

fig1.write_html("figures/phenology_option1_weekly.html")
print("✓ Created: figures/phenology_option1_weekly.html")

# ============================================================================
# OPTION 2: Ridgeline plot (small multiples showing distribution per species)
# ============================================================================
print("\n2. Ridgeline Plot (Distribution per Species)")
print("-" * 80)

with BirdRingingDB(DB_PATH, read_only=True) as db:
    query = BirdRingingQueries.get_phenology_daily_distribution(
        species_codes=SPECIES,
        start_year=2020,
        aggregate_years=True
    )
    df_daily = db.execute_query(query).pl().to_pandas()

species_list = df_daily['swedish_name'].unique()
n_species = len(species_list)

fig2 = make_subplots(
    rows=n_species, 
    cols=1,
    subplot_titles=[f"<b>{sp}</b>" for sp in species_list],
    vertical_spacing=0.03,
    shared_xaxes=True
)

for idx, species in enumerate(species_list, start=1):
    species_df = df_daily[df_daily['swedish_name'] == species]
    
    fig2.add_trace(
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
    fig2.update_yaxes(
        title_text="Count",
        title_standoff=5,
        title_font_size=10,
        row=idx,
        col=1
    )

fig2.update_xaxes(title_text="Day of Year", row=n_species, col=1)

fig2.update_layout(
    height=200 * n_species,
    title_text="Phenology: Daily Observation Distribution (Ridgeline Plot)<br>" +
               "<sub>Each row shows the temporal distribution for one species</sub>",
    template="plotly_white"
)

fig2.write_html("figures/phenology_option2_ridgeline.html")
print("✓ Created: figures/phenology_option2_ridgeline.html")

# ============================================================================
# OPTION 3: Separated Spring/Autumn metrics with error bars
# ============================================================================
print("\n3. Spring vs Autumn Migration Windows")
print("-" * 80)

with BirdRingingDB(DB_PATH, read_only=True) as db:
    query = BirdRingingQueries.get_phenology_migration_windows(
        species_codes=SPECIES,
        start_year=2020
    )
    df_seasonal = db.execute_query(query).pl().to_pandas()

fig3 = go.Figure()

# Calculate average across years for each species/season
seasonal_avg = df_seasonal.groupby(['swedish_name', 'season']).agg({
    'median': 'mean',
    'q25': 'mean',
    'q75': 'mean',
    'n_observations': 'sum'
}).reset_index()

for season, color, symbol in [('spring', 'green', 'circle'), ('autumn', 'orange', 'square')]:
    season_df = seasonal_avg[seasonal_avg['season'] == season]
    
    fig3.add_trace(go.Scatter(
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

fig3.update_layout(
    title="Phenology: Spring vs Autumn Migration Timing (2020-2024)<br>" +
          "<sub>Points show median day of year, error bars show interquartile range</sub>",
    xaxis_title="Species",
    yaxis_title="Day of Year",
    template="plotly_white",
    hovermode="closest"
)

# Add month labels on y-axis
months = [(1, "Jan"), (32, "Feb"), (60, "Mar"), (91, "Apr"), (121, "May"), 
          (152, "Jun"), (182, "Jul"), (213, "Aug"), (244, "Sep"), (274, "Oct"), 
          (305, "Nov"), (335, "Dec")]

fig3.update_yaxes(
    tickmode='array',
    tickvals=[m[0] for m in months],
    ticktext=[m[1] for m in months]
)

fig3.write_html("figures/phenology_option3_seasonal.html")
print("✓ Created: figures/phenology_option3_seasonal.html")

# ============================================================================
# OPTION 4: Heatmap (week x species)
# ============================================================================
print("\n4. Heatmap (Week × Species)")
print("-" * 80)

# Pivot the weekly data for heatmap
heatmap_data = df_weekly.pivot(
    index='swedish_name',
    columns='week_of_year',
    values='avg_count'
).fillna(0)

fig4 = go.Figure(data=go.Heatmap(
    z=heatmap_data.values,
    x=heatmap_data.columns,
    y=heatmap_data.index,
    colorscale='YlOrRd',
    hovertemplate='Species: %{y}<br>Week: %{x}<br>Avg count: %{z:.1f}<extra></extra>'
))

fig4.update_layout(
    title="Phenology: Weekly Observation Heatmap (2020-2024)<br>" +
          "<sub>Color intensity shows average weekly observation counts</sub>",
    xaxis_title="Week of Year",
    yaxis_title="Species",
    template="plotly_white"
)

fig4.write_html("figures/phenology_option4_heatmap.html")
print("✓ Created: figures/phenology_option4_heatmap.html")

# ============================================================================
# OPTION 5: Year-over-year comparison (faceted by species, showing trends)
# ============================================================================
print("\n5. Year-over-Year Comparison")
print("-" * 80)

with BirdRingingDB(DB_PATH, read_only=True) as db:
    query = BirdRingingQueries.get_phenology_weekly_distribution(
        species_codes=SPECIES,
        start_year=2020,
        aggregate_years=False  # Keep years separate
    )
    df_yearly = db.execute_query(query).pl().to_pandas()

# Create faceted plot
from plotly.subplots import make_subplots

species_list = df_yearly['swedish_name'].unique()
n_species = len(species_list)

fig5 = make_subplots(
    rows=n_species,
    cols=1,
    subplot_titles=[f"<b>{sp}</b>" for sp in species_list],
    vertical_spacing=0.05,
    shared_xaxes=True
)

colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']

for idx, species in enumerate(species_list, start=1):
    species_df = df_yearly[df_yearly['swedish_name'] == species]
    
    for year_idx, year in enumerate(sorted(species_df['year'].unique())):
        year_df = species_df[species_df['year'] == year]
        
        fig5.add_trace(
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
    
    fig5.update_yaxes(title_text="Count", row=idx, col=1)

fig5.update_xaxes(title_text="Week of Year", row=n_species, col=1)

fig5.update_layout(
    height=250 * n_species,
    title_text="Phenology: Year-over-Year Weekly Patterns<br>" +
               "<sub>Compare migration timing shifts between years</sub>",
    template="plotly_white",
    hovermode="x unified"
)

fig5.write_html("figures/phenology_option5_yearly_comparison.html")
print("✓ Created: figures/phenology_option5_yearly_comparison.html")

# ============================================================================
# Summary and recommendations
# ============================================================================
print("\n" + "=" * 80)
print("RECOMMENDATIONS")
print("=" * 80)
print("""
Based on the bimodal nature of bird migration data, here are the recommended visualizations:

1. **PRIMARY VISUALIZATION** (Option 1 - Weekly Distribution):
   - Shows the complete annual pattern with spring and autumn peaks
   - Smoothed enough to reduce noise but preserves bimodal patterns
   - Easy to compare multiple species
   - Best for dashboard main view

2. **DETAILED VIEW** (Option 2 - Ridgeline Plot):
   - Shows fine-grained distribution patterns
   - Good for exploring individual species phenology
   - Works well as a drill-down view

3. **COMPARATIVE VIEW** (Option 3 - Spring/Autumn Metrics):
   - Quantifies migration timing statistically
   - Easy to compare between species
   - Good for identifying migration window shifts
   - Best for scientific analysis

4. **OVERVIEW** (Option 4 - Heatmap):
   - Compact visualization of all species
   - Easy to spot seasonal patterns
   - Good for initial exploration

5. **TREND ANALYSIS** (Option 5 - Year-over-Year):
   - Shows temporal shifts in migration timing
   - Important for climate change analysis
   - Best for scientific/research use

DASHBOARD IMPLEMENTATION SUGGESTION:
- Use Option 1 as the default view with species selector
- Add a toggle to switch between "Weekly Pattern" and "Spring vs Autumn"
- Include year-over-year comparison as an advanced option
""")

print(f"\n✓ All visualizations saved to figures/ directory")
print(f"✓ Open the HTML files in a browser to explore interactively")

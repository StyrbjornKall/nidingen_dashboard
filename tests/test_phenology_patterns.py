"""
Test script to explore phenology patterns in bird ringing data.
This helps understand bimodal distributions (spring/autumn migration).
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from db_manager import BirdRingingDB
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import polars as pl

DB_PATH = "data/bird_ringing.db"

# Test query to get daily distribution across the year
query = """
WITH daily_counts AS (
    SELECT 
        species_code,
        swedish_name,
        EXTRACT(YEAR FROM date) as year,
        DAYOFYEAR(date) as day_of_year,
        COUNT(*) as count
    FROM ring_records
    WHERE species_code IN ('GÄSMY', 'BLMES', 'RÖHAK', 'SKPIP', 'JÄSPA')
      AND EXTRACT(YEAR FROM date) >= 2020
    GROUP BY species_code, swedish_name, year, day_of_year
)
SELECT 
    species_code,
    swedish_name,
    day_of_year,
    SUM(count) as total_count,
    AVG(count) as avg_count,
    COUNT(DISTINCT year) as n_years
FROM daily_counts
GROUP BY species_code, swedish_name, day_of_year
ORDER BY species_code, day_of_year
"""

print("Fetching phenology data...")
with BirdRingingDB(DB_PATH, read_only=True) as db:
    df = db.execute_query(query).pl()

print(f"\nData shape: {df.shape}")
print(f"\nSpecies found: {df['swedish_name'].unique().to_list()}")

# Create subplots for each species
species_list = df['swedish_name'].unique().to_list()
n_species = len(species_list)

fig = make_subplots(
    rows=n_species, 
    cols=1,
    subplot_titles=species_list,
    vertical_spacing=0.05,
    shared_xaxes=True
)

for idx, species in enumerate(species_list, start=1):
    species_df = df.filter(pl.col('swedish_name') == species).to_pandas()
    
    fig.add_trace(
        go.Scatter(
            x=species_df['day_of_year'],
            y=species_df['avg_count'],
            mode='lines',
            name=species,
            fill='tozeroy',
            line=dict(width=2)
        ),
        row=idx,
        col=1
    )
    
    # Add statistics as annotations
    print(f"\n{species}:")
    print(f"  Total observations: {species_df['total_count'].sum():.0f}")
    print(f"  Peak day: {species_df.loc[species_df['avg_count'].idxmax(), 'day_of_year']:.0f}")
    print(f"  Days with observations: {len(species_df)}")

fig.update_xaxes(title_text="Day of Year", row=n_species, col=1)
fig.update_yaxes(title_text="Avg Daily Count")

fig.update_layout(
    height=300 * n_species,
    title_text="Daily Observation Patterns Across the Year (2020-2024)",
    showlegend=False,
    template="plotly_white"
)

# Save figure
output_path = "figures/phenology_patterns_exploration.html"
Path("figures").mkdir(exist_ok=True)
fig.write_html(output_path)
print(f"\n✓ Saved exploration plot to: {output_path}")

# Also create a monthly aggregation to see patterns more clearly
query_monthly = """
SELECT 
    species_code,
    swedish_name,
    EXTRACT(YEAR FROM date) as year,
    EXTRACT(MONTH FROM date) as month,
    COUNT(*) as count
FROM ring_records
WHERE species_code IN ('GÄSMY', 'BLMES', 'RÖHAK', 'SKPIP', 'JÄSPA')
  AND EXTRACT(YEAR FROM date) >= 2020
GROUP BY species_code, swedish_name, year, month
ORDER BY species_code, year, month
"""

with BirdRingingDB(DB_PATH, read_only=True) as db:
    df_monthly = db.execute_query(query_monthly).pl().to_pandas()

print("\n\nMonthly distribution summary:")
for species in df_monthly['swedish_name'].unique():
    species_monthly = df_monthly[df_monthly['swedish_name'] == species]
    monthly_totals = species_monthly.groupby('month')['count'].sum().sort_values(ascending=False)
    print(f"\n{species} - Top 3 months:")
    for month, count in monthly_totals.head(3).items():
        print(f"  Month {int(month)}: {count} obs")

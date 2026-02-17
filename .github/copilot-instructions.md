# Bird Ringing Dashboard - Development Instructions

## Project Overview

This project is a web-based dashboard application for visualizing and analyzing bird ringing data from Nidingen ringing station in Sweden. The application uses **Plotly Dash** for the frontend, **DuckDB** for the database, and **Polars** for high-performance data processing.

**Key Features:**
- Interactive time series visualizations of bird observations
- Morphometric analysis (weight, wing length distributions)
- Phenology (migration timing) analysis
- Recapture statistics
- Multi-dimensional filtering and aggregation
- Support for millions of records with efficient query performance

## Technology Stack

- **Frontend**: Plotly Dash (Python web framework)
- **Database**: DuckDB (embedded analytical database)
- **Data Processing**: Polars (high-performance DataFrame library)
- **Visualization**: Plotly (interactive charts)
- **Language**: Python 3.9+

## Project Structure

```
märkning/
├── data/
│   ├── bird_ringing.db          # DuckDB database (generated)
│   ├── metadata/                # Species and ringer metadata
│   ├── processed/               # Processed CSV files
│   │   └── processed_nidingen_data.csv
│   └── raw/                     # Raw data files
├── figures/                     # Generated visualizations
├── notebooks/                   # Jupyter notebooks for exploration
├── src/
│   ├── db_manager.py           # DuckDB database operations
│   ├── data_processor.py       # Polars data processing utilities
│   ├── query_utils.py          # Pre-built SQL queries
│   ├── initialize_database.py  # Database setup script
│   └── preprocess_raw_data.py  # Raw data preprocessing
└── requirements.txt            # Python dependencies
```

## Data Schema

### Main Table: `ring_records`

| Column | Type | Description |
|--------|------|-------------|
| record_id | INTEGER | Primary key |
| date | DATE | Observation date |
| time | DOUBLE | Time of day (hours) |
| record_type | VARCHAR(5) | Record type code (C=capture, R=recapture) |
| ring_number | VARCHAR(50) | Unique ring identifier |
| age_code | VARCHAR(10) | Age code |
| species_code | VARCHAR(20) | Species code (e.g., GÄSMY, BLMES) |
| ringer | VARCHAR(10) | Ringer initials |
| age | VARCHAR(10) | Age description |
| wing_length | INTEGER | Wing length (mm) |
| weight | DOUBLE | Weight (grams) |
| fat_score | INTEGER | Fat score (0-10) |
| muscle_score | INTEGER | Muscle score (0-10) |
| brood_patch | DOUBLE | Brood patch score |
| moult_score | DOUBLE | Moult score |
| notes | TEXT | Additional notes |
| scientific_name | VARCHAR(100) | Scientific species name |
| swedish_name | VARCHAR(100) | Swedish species name |
| taxon_id | DOUBLE | Taxonomic ID |
| data_source | VARCHAR(50) | Data source identifier |
| created_at | TIMESTAMP | Record creation timestamp |
| updated_at | TIMESTAMP | Last update timestamp |

**Indexes:** date, species_code, (date, species_code), ring_number

### Supporting Tables

- `species_metadata`: Extended species information
- `weather_data`: Weather conditions (for future integration)
- `ringer_info`: Ringer contact information

## Core Modules

### 1. `db_manager.py` - Database Management

**Class: `BirdRingingDB`**

Main database interface for all operations.

```python
# Initialize database
with BirdRingingDB("data/bird_ringing.db") as db:
    db.initialize_schema()
    db.load_csv_to_table("data/processed/processed_nidingen_data.csv")
    
    # Query data
    df = db.get_data_as_polars(filters={"species_code": "GÄSMY"})
    
    # Get statistics
    stats = db.get_summary_stats()
```

**Key Methods:**
- `initialize_schema()`: Create database tables and indexes
- `load_csv_to_table()`: Load CSV data into database
- `get_data_as_polars()`: Retrieve data as Polars DataFrame
- `execute_query()`: Execute custom SQL queries
- `get_summary_stats()`: Get database summary statistics
- `optimize_database()`: Run optimization operations
- `export_table_to_parquet()`: Export to Parquet format

### 2. `data_processor.py` - Data Processing

**Class: `BirdDataProcessor`**

High-performance data processing using Polars.

```python
processor = BirdDataProcessor()

# Load and clean data
df = processor.load_csv("data.csv")
df = processor.clean_ring_records(df)

# Add time features
df = processor.add_time_features(df)

# Get species summary
summary = processor.get_species_summary(df)

# Calculate phenology
phenology = processor.calculate_phenology_metrics(df)
```

**Key Methods:**
- `load_csv()`: Load CSV with optimized settings
- `clean_ring_records()`: Clean and standardize data
- `add_time_features()`: Add year, month, day, season columns
- `filter_by_date_range()`: Filter by date range
- `filter_by_species()`: Filter by species
- `aggregate_daily_counts()`: Aggregate to daily counts
- `calculate_recapture_stats()`: Analyze recaptures
- `get_species_summary()`: Generate species statistics
- `calculate_phenology_metrics()`: Migration timing analysis
- `detect_outliers()`: Identify outliers in data

### 3. `query_utils.py` - Pre-built Queries

**Class: `BirdRingingQueries`**

Optimized SQL queries for common dashboard operations.

```python
from query_utils import BirdRingingQueries

# Get time series query
query = BirdRingingQueries.get_species_time_series(
    start_date="2020-01-01",
    species_codes=["GÄSMY", "BLMES"],
    aggregation="weekly"
)

# Execute with database
with BirdRingingDB("data/bird_ringing.db") as db:
    df = db.execute_query(query).pl()
```

**Available Queries:**
- `get_species_time_series()`: Time series of observations
- `get_morphometric_distributions()`: Weight/wing length data
- `get_recapture_analysis()`: Recapture statistics
- `get_phenology_by_species()`: Migration timing
- `get_ringer_statistics()`: Ringer activity
- `get_species_diversity_over_time()`: Species richness over time
- `get_conditional_body_metrics()`: Body condition metrics
- `get_year_over_year_comparison()`: Year-over-year changes

## Development Workflow

### Initial Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Initialize database:**
   ```bash
   python src/initialize_database.py
   ```

### Loading New Data

When adding new years or data:

```python
from src.db_manager import BirdRingingDB

with BirdRingingDB("data/bird_ringing.db") as db:
    # Append new data (preserves existing records)
    db.load_csv_to_table(
        csv_path="data/processed/new_data.csv",
        if_exists="append"
    )
    db.optimize_database()
```

### Building Dashboard Components

When creating new dashboard visualizations:

1. **Define query** using `BirdRingingQueries` or custom SQL
2. **Fetch data** using `BirdRingingDB.execute_query()` or `get_data_as_polars()`
3. **Process data** using `BirdDataProcessor` methods if needed
4. **Create Plotly figure** using `plotly.graph_objects` or `plotly.express`
5. **Add to Dash layout** as `dcc.Graph` component

### Example Dashboard Component

```python
import dash
from dash import dcc, html, Input, Output
import plotly.express as px
from src.db_manager import BirdRingingDB
from src.query_utils import BirdRingingQueries

app = dash.Dash(__name__)

@app.callback(
    Output("time-series-plot", "figure"),
    Input("species-dropdown", "value")
)
def update_time_series(species_codes):
    # Get data from database
    with BirdRingingDB("data/bird_ringing.db", read_only=True) as db:
        query = BirdRingingQueries.get_species_time_series(
            species_codes=species_codes,
            aggregation="monthly"
        )
        df = db.execute_query(query).pl()
    
    # Convert to pandas for Plotly
    df_pd = df.to_pandas()
    
    # Create figure
    fig = px.line(
        df_pd,
        x="period",
        y="count",
        color="swedish_name",
        title="Monthly Species Observations"
    )
    
    return fig
```

## Performance Best Practices

### Database Operations

1. **Use indexes**: Queries on date, species_code are optimized
2. **Filter early**: Apply WHERE clauses before aggregations
3. **Use read_only mode**: For dashboard queries (prevents locking)
4. **Batch operations**: Use transactions for multiple inserts
5. **Optimize regularly**: Run `db.optimize_database()` after bulk loads

### Data Processing with Polars

1. **Lazy evaluation**: Use `.lazy()` for query optimization
2. **Avoid pandas**: Polars is 10-100x faster for large datasets
3. **Use `.pl()` method**: Convert DuckDB results directly to Polars
4. **Filter before collect**: Apply filters in lazy mode
5. **Parallel processing**: Polars automatically parallelizes operations

### Dashboard Performance

1. **Cache data**: Use `@lru_cache` for repeated queries
2. **Pagination**: Limit initial data load for large tables
3. **Aggregation**: Pre-aggregate data when possible
4. **Lazy loading**: Load data only when needed
5. **Use Parquet**: For large static datasets

## Common Queries for Dashboard

### Time Series Visualization
```python
query = BirdRingingQueries.get_species_time_series(
    start_date="2020-01-01",
    end_date="2024-12-31",
    species_codes=["GÄSMY", "BLMES"],
    aggregation="monthly"
)
```

### Box Plot for Morphometrics
```python
query = BirdRingingQueries.get_morphometric_distributions(
    species_codes=["GÄSMY"],
    year=2023
)
```

### Recapture Analysis
```python
query = BirdRingingQueries.get_recapture_analysis()
```

### Phenology (Migration Timing)
```python
query = BirdRingingQueries.get_phenology_by_species(
    species_codes=["GÄSMY"],
    start_year=2020,
    end_year=2024
)
```

## Data Types and Visualization Suggestions

### Scatter Plots
- Weight vs. Wing Length (morphometric relationships)
- Date vs. Weight (seasonal patterns)
- Fat Score vs. Weight (body condition)

### Histograms
- Weight distributions by species
- Wing length distributions by age class
- Arrival day distributions (phenology)

### Box Plots
- Weight by species and year
- Wing length by age class
- Fat scores across seasons

### Line Plots
- Daily/weekly/monthly observation counts
- Cumulative arrivals (phenology curves)
- Year-over-year trends

### Heatmaps
- Species by month matrix
- Hour of day by species activity
- Year by species observation counts

### Bar Charts
- Top species by count
- Ringer activity statistics
- Species richness by year

## Testing Queries

Always test queries before adding to dashboard:

```python
with BirdRingingDB("data/bird_ringing.db", read_only=True) as db:
    query = "YOUR_SQL_QUERY"
    result = db.execute_query(query)
    print(result.pl())  # Check results
```

## Extending the Database

### Adding Weather Data

```python
# Create weather data table (already defined in schema)
with BirdRingingDB("data/bird_ringing.db") as db:
    db.initialize_schema()  # Creates weather_data table
    
    # Load weather CSV
    weather_df = pl.read_csv("weather_data.csv")
    db.conn.execute("INSERT INTO weather_data SELECT * FROM weather_df")
```

### Adding New Species Metadata

```python
with BirdRingingDB("data/bird_ringing.db") as db:
    db.conn.execute("""
        INSERT INTO species_metadata (species_code, scientific_name, ...)
        VALUES (?, ?, ...)
    """, parameters)
```

## Troubleshooting

### Database Lock Issues
- Use `read_only=True` for queries in dashboard
- Only one write connection at a time
- Close connections properly (use context manager)

### Memory Issues
- Use DuckDB's query result streaming for large results
- Apply filters before fetching data
- Use pagination for dashboard tables
- Increase `memory_limit` in BirdRingingDB.__init__

### Performance Issues
- Check if indexes are being used: `EXPLAIN query`
- Ensure database is optimized: `db.optimize_database()`
- Profile queries with `EXPLAIN ANALYZE`
- Consider materialized views for complex aggregations

## Next Steps

1. **Build dashboard layout**: Create main dashboard file with Dash components
2. **Implement filters**: Add dropdowns for species, date ranges, ringers
3. **Create visualizations**: Implement plot types as callback functions
4. **Add interactivity**: Link filters to plots with callbacks
5. **Style dashboard**: Add CSS, improve layout with dash-bootstrap-components
6. **Deploy**: Use Gunicorn for production deployment

## Useful Resources

- DuckDB SQL: https://duckdb.org/docs/sql/introduction
- Polars API: https://pola-rs.github.io/polars/py-polars/html/reference/
- Plotly Dash: https://dash.plotly.com/
- Plotly Graphs: https://plotly.com/python/

## Common Species Codes

- GÄSMY: Gärdsmyg (Winter Wren)
- BLMES: Blåmes (Blue Tit)
- SKPIP: Skärpiplärka (Rock Pipit)
- RÖHAK: Rödhake (European Robin)
- KOTRA: Koltrast (Common Blackbird)
- JÄSPA: Järnsparv (Dunnock)
- TATRA: Taltrast (Song Thrush)
- STSTR: Större strandpipare (Ringed Plover)
- VIHÄM: Vinterhämpling (Twite)

---

**Last Updated**: 2026-02-16
**Database Version**: 1.0
**Schema Version**: 1.0

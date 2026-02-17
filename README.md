# 🐦 Bird Ringing Dashboard - Nidingen Station

An interactive web dashboard for visualizing and analyzing bird ringing data from Nidingen ringing station in Sweden. Built with Plotly Dash, DuckDB, and Polars for high-performance data analysis.

## Features

- **📈 Time Series Analysis**: Visualize bird observations over time with flexible aggregation (daily, weekly, monthly, yearly)
- **📊 Morphometric Analysis**: Explore weight and wing length distributions across species and age classes
- **🌸 Phenology Tracking**: Analyze migration timing patterns and seasonal trends
- **🔄 Recapture Analysis**: Track individual birds and analyze return patterns
- **⚡ High Performance**: Efficiently handles millions of records using DuckDB and Polars
- **🎨 Interactive Filters**: Multi-dimensional filtering by species, date range, and more

## Quick Start

### Prerequisites

- Python 3.9 or higher
- pip package manager

### Installation

1. **Clone or navigate to the project directory:**
   ```bash
   cd märkning
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Initialize the database:**
   ```bash
   python src/initialize_database.py
   ```
   
   This will:
   - Create the DuckDB database
   - Load existing data from `data/processed/processed_nidingen_data.csv`
   - Create indexes for optimal query performance
   - Display summary statistics

4. **Run the dashboard:**
   ```bash
   python app.py
   ```

5. **Open your browser and navigate to:**
   ```
   http://localhost:8050
   ```

## Project Structure

```
märkning/
├── app.py                      # Main dashboard application
├── requirements.txt            # Python dependencies
├── data/
│   ├── bird_ringing.db        # DuckDB database (generated)
│   ├── processed/             # Processed CSV data
│   ├── metadata/              # Species and ringer metadata
│   └── raw/                   # Raw data files
├── src/
│   ├── db_manager.py          # Database operations
│   ├── data_processor.py      # Data processing utilities
│   ├── query_utils.py         # Pre-built SQL queries
│   ├── initialize_database.py # Database setup script
│   └── preprocess_raw_data.py # Raw data preprocessing
├── notebooks/                  # Jupyter notebooks for analysis
└── figures/                    # Generated visualizations
```

## Usage

### Dashboard Interface

The dashboard provides four main tabs:

1. **📈 Time Series**: View observation counts over time
   - Select one or multiple species
   - Choose aggregation level (daily/weekly/monthly/yearly)
   - Filter by date range

2. **📊 Morphometrics**: Analyze body measurements
   - Box plots for weight and wing length distributions
   - Compare across species and age classes
   - Identify outliers and trends

3. **🌸 Phenology**: Track migration timing
   - Median arrival dates by year
   - Quartile ranges showing variation
   - Compare timing shifts across years

4. **📋 Summary**: View overall statistics
   - Total observations
   - Unique species count
   - Individual bird counts
   - Active date ranges

### Programmatic Usage

#### Query the Database

```python
from src.db_manager import BirdRingingDB

with BirdRingingDB("data/bird_ringing.db", read_only=True) as db:
    # Get data as Polars DataFrame
    df = db.get_data_as_polars(filters={"species_code": "GÄSMY"})
    
    # Execute custom query
    result = db.execute_query("SELECT * FROM ring_records LIMIT 10")
    
    # Get summary statistics
    stats = db.get_summary_stats()
```

#### Process Data with Polars

```python
from src.data_processor import BirdDataProcessor

processor = BirdDataProcessor()

# Load and clean data
df = processor.load_csv("data.csv")
df = processor.clean_ring_records(df)

# Add time features (year, month, season, etc.)
df = processor.add_time_features(df)

# Get species summary
summary = processor.get_species_summary(df)

# Calculate phenology metrics
phenology = processor.calculate_phenology_metrics(df)
```

#### Use Pre-built Queries

```python
from src.query_utils import BirdRingingQueries

# Time series query
query = BirdRingingQueries.get_species_time_series(
    start_date="2020-01-01",
    species_codes=["GÄSMY", "BLMES"],
    aggregation="monthly"
)

# Morphometric distributions
query = BirdRingingQueries.get_morphometric_distributions(
    species_codes=["GÄSMY"],
    year=2023
)
```

## Adding New Data

To add new years or data:

```python
from src.db_manager import BirdRingingDB

with BirdRingingDB("data/bird_ringing.db") as db:
    # Append new data (preserves existing records)
    db.load_csv_to_table(
        csv_path="data/processed/new_data_2025.csv",
        if_exists="append"
    )
    
    # Optimize database after bulk load
    db.optimize_database()
```

## Data Schema

The main `ring_records` table contains:

- **Identification**: record_id, ring_number, species_code
- **Temporal**: date, time, year, month, day
- **Morphometrics**: weight, wing_length, fat_score, muscle_score
- **Metadata**: age, ringer, record_type (capture/recapture)
- **Species Info**: scientific_name, swedish_name, taxon_id

## Common Species Codes

| Code  | Swedish Name          | Scientific Name         |
|-------|-----------------------|-------------------------|
| GÄSMY | Gärdsmyg             | Troglodytes troglodytes |
| BLMES | Blåmes               | Cyanistes caeruleus     |
| SKPIP | Skärpiplärka         | Anthus petrosus         |
| RÖHAK | Rödhake              | Erithacus rubecula      |
| KOTRA | Koltrast             | Turdus merula           |
| JÄSPA | Järnsparv            | Prunella modularis      |

## Performance Optimization

The system is designed to handle millions of records efficiently:

- **Indexed Queries**: Optimized indexes on date, species_code, and ring_number
- **Parallel Processing**: Polars automatically parallelizes operations
- **Read-Only Mode**: Dashboard uses read-only connections to prevent locking
- **Lazy Evaluation**: Queries are optimized before execution
- **Efficient Storage**: DuckDB uses columnar storage for fast aggregations

## Development

For detailed development instructions, see [`.github/copilot-instructions.md`](.github/copilot-instructions.md)

### Running Tests

```bash
# Test database operations
python -m pytest tests/test_db_manager.py

# Test data processing
python -m pytest tests/test_data_processor.py
```

### Adding New Visualizations

1. Add query to `src/query_utils.py`
2. Create callback function in `app.py`
3. Add UI component to layout
4. Test with sample data

## Technology Stack

- **Frontend**: Plotly Dash
- **Database**: DuckDB (embedded analytical database)
- **Data Processing**: Polars (high-performance DataFrames)
- **Visualization**: Plotly
- **Language**: Python 3.9+

## License

[Add your license information here]

## Contributors

[Add contributor information here]

## Support

For issues and questions:
- Open an issue on GitHub
- Check the documentation in `.github/copilot-instructions.md`
- Review example notebooks in `notebooks/`

---

**Last Updated**: February 16, 2026

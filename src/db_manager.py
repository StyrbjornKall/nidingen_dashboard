"""
Database manager for bird ringing data using DuckDB.

This module provides functionality to initialize, populate, and manage
a DuckDB database for bird ringing records. Designed to handle millions
of records efficiently with support for incremental updates.
"""

import duckdb
from pathlib import Path
from typing import Optional, Union, List
import polars as pl
from datetime import datetime


class BirdRingingDB:
    """Manages DuckDB database for bird ringing data."""
    
    def __init__(self, db_path: Union[str, Path], read_only: bool = False):
        """
        Initialize the database connection.
        
        Parameters:
        -----------
        db_path : str or Path
            Path to the DuckDB database file
        read_only : bool
            If True, opens database in read-only mode
        """
        self.db_path = Path(db_path)
        self.read_only = read_only
        self.conn = duckdb.connect(str(self.db_path), read_only=read_only)
        
        # Configure DuckDB for better performance
        self.conn.execute("SET memory_limit='4GB'")
        self.conn.execute("SET threads=4")
        
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            
    def initialize_schema(self):
        """
        Create the database schema with optimized table structures.
        
        Creates tables for:
        - ring_records: Main ringing observations
        - species_metadata: Species information
        - weather_data: Weather conditions (for future use)
        - ringer_info: Ringer metadata
        """
        
        # Main ringing records table with appropriate data types and indexes
        # Create sequence for record_id
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS ring_records_seq START 1
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ring_records (
                record_id INTEGER PRIMARY KEY DEFAULT nextval('ring_records_seq'),
                date DATE NOT NULL,
                time DOUBLE,
                record_type VARCHAR(5),
                ring_number VARCHAR(50),
                age_code VARCHAR(10),
                species_code VARCHAR(20) NOT NULL,
                ringer VARCHAR(10),
                age VARCHAR(10),
                wing_length INTEGER,
                weight DOUBLE,
                fat_score INTEGER,
                muscle_score INTEGER,
                brood_patch DOUBLE,
                moult_score DOUBLE,
                notes TEXT,
                scientific_name VARCHAR(100),
                swedish_name VARCHAR(100),
                taxon_id DOUBLE,
                data_source VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes for common query patterns
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_date ON ring_records(date)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_species ON ring_records(species_code)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_date_species ON ring_records(date, species_code)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ring_number ON ring_records(ring_number)
        """)
        
        # Species metadata table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS species_metadata (
                species_code VARCHAR(20) PRIMARY KEY,
                scientific_name VARCHAR(100),
                swedish_name VARCHAR(100),
                english_name VARCHAR(100),
                taxon_id DOUBLE,
                family VARCHAR(100),
                order_name VARCHAR(100),
                additional_info TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Weather data table (for future expansion)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                weather_id INTEGER PRIMARY KEY,
                date DATE NOT NULL,
                hour INTEGER,
                temperature DOUBLE,
                wind_speed DOUBLE,
                wind_direction VARCHAR(10),
                precipitation DOUBLE,
                cloud_cover INTEGER,
                visibility DOUBLE,
                pressure DOUBLE,
                humidity INTEGER,
                data_source VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_weather_date ON weather_data(date)
        """)
        
        # Ringer information table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ringer_info (
                ringer_code VARCHAR(10) PRIMARY KEY,
                full_name VARCHAR(100),
                email VARCHAR(100),
                active_years VARCHAR(50),
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        print("Database schema initialized successfully.")
        
    def load_csv_to_table(
        self, 
        csv_path: Union[str, Path], 
        table_name: str = "ring_records",
        if_exists: str = "append"
    ):
        """
        Load data from CSV file into database table using Polars for preprocessing.
        
        Parameters:
        -----------
        csv_path : str or Path
            Path to the CSV file
        table_name : str
            Target table name
        if_exists : str
            What to do if table exists: 'append', 'replace', or 'fail'
        """
        csv_path = Path(csv_path)
        
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
            
        print(f"Loading data from {csv_path.name}...")
        
        # Read CSV with Polars for efficient processing
        # Force notes column to be string to handle mixed content
        df = pl.read_csv(
            csv_path, 
            infer_schema_length=10000,
            schema_overrides={"notes": pl.Utf8}
        )
        
        # Clean and prepare data
        df = self._prepare_ring_records(df)
        
        # Convert to DuckDB
        if if_exists == "replace":
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.initialize_schema()
            
        # Insert data efficiently - list columns explicitly (excluding record_id which auto-generates)
        columns = [col for col in df.columns if col != 'record_id']
        columns_str = ", ".join(columns)
        
        self.conn.execute(f"""
            INSERT INTO {table_name} ({columns_str})
            SELECT {columns_str} FROM df
        """)
        
        row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Successfully loaded {len(df)} records. Total records in table: {row_count}")
        
    def _prepare_ring_records(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Prepare and clean ring records data for insertion.
        
        Parameters:
        -----------
        df : pl.DataFrame
            Raw dataframe from CSV
            
        Returns:
        --------
        pl.DataFrame
            Cleaned dataframe ready for insertion
        """
        # Rename TaxonID to taxon_id if present
        if "TaxonID" in df.columns:
            df = df.rename({"TaxonID": "taxon_id"})
        
        # Ensure proper data types
        if df["date"].dtype != pl.Date:
            df = df.with_columns([
                pl.col("date").str.to_date("%Y-%m-%d", strict=False).alias("date")
            ])
        
        # Add metadata columns
        df = df.with_columns([
            pl.lit("CSV").alias("data_source"),
            pl.lit(datetime.now()).alias("created_at"),
            pl.lit(datetime.now()).alias("updated_at")
        ])
        
        return df
        
    def get_data_as_polars(
        self,
        query: Optional[str] = None,
        table_name: str = "ring_records",
        filters: Optional[dict] = None
    ) -> pl.DataFrame:
        """
        Retrieve data from database as Polars DataFrame.
        
        Parameters:
        -----------
        query : str, optional
            Custom SQL query. If None, selects from table_name
        table_name : str
            Table to query if no custom query provided
        filters : dict, optional
            Dictionary of column:value pairs for filtering
            
        Returns:
        --------
        pl.DataFrame
            Query results as Polars DataFrame
        """
        if query is None:
            query = f"SELECT * FROM {table_name}"
            
            if filters:
                where_clauses = []
                for col, val in filters.items():
                    if isinstance(val, str):
                        where_clauses.append(f"{col} = '{val}'")
                    elif isinstance(val, (list, tuple)):
                        val_str = ",".join([f"'{v}'" if isinstance(v, str) else str(v) for v in val])
                        where_clauses.append(f"{col} IN ({val_str})")
                    else:
                        where_clauses.append(f"{col} = {val}")
                        
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
        
        # Execute query and convert to Polars
        result = self.conn.execute(query).pl()
        return result
        
    def execute_query(self, query: str):
        """
        Execute a SQL query and return results.
        
        Parameters:
        -----------
        query : str
            SQL query to execute
            
        Returns:
        --------
        DuckDB result object
        """
        return self.conn.execute(query)
        
    def get_summary_stats(self) -> dict:
        """
        Get summary statistics about the database.
        
        Returns:
        --------
        dict
            Dictionary containing database statistics
        """
        stats = {}
        
        # Ring records stats
        stats["total_records"] = self.conn.execute(
            "SELECT COUNT(*) FROM ring_records"
        ).fetchone()[0]
        
        stats["date_range"] = self.conn.execute(
            "SELECT MIN(date), MAX(date) FROM ring_records"
        ).fetchone()
        
        stats["unique_species"] = self.conn.execute(
            "SELECT COUNT(DISTINCT species_code) FROM ring_records"
        ).fetchone()[0]
        
        stats["unique_ringers"] = self.conn.execute(
            "SELECT COUNT(DISTINCT ringer) FROM ring_records WHERE ringer IS NOT NULL"
        ).fetchone()[0]
        
        # Top species
        stats["top_species"] = self.conn.execute("""
            SELECT species_code, swedish_name, COUNT(*) as count 
            FROM ring_records 
            GROUP BY species_code, swedish_name 
            ORDER BY count DESC 
            LIMIT 10
        """).fetchall()
        
        return stats
        
    def optimize_database(self):
        """Run database optimization operations."""
        print("Optimizing database...")
        self.conn.execute("ANALYZE")
        self.conn.execute("CHECKPOINT")
        print("Database optimized.")
        
    def export_table_to_parquet(
        self,
        table_name: str,
        output_path: Union[str, Path],
        partition_by: Optional[List[str]] = None
    ):
        """
        Export table to Parquet format for efficient storage and sharing.
        
        Parameters:
        -----------
        table_name : str
            Name of table to export
        output_path : str or Path
            Output path for parquet file(s)
        partition_by : list of str, optional
            Columns to partition by
        """
        output_path = Path(output_path)
        
        if partition_by:
            partition_str = ", ".join(partition_by)
            self.conn.execute(f"""
                COPY (SELECT * FROM {table_name})
                TO '{output_path}'
                (FORMAT PARQUET, PARTITION_BY ({partition_str}))
            """)
        else:
            self.conn.execute(f"""
                COPY {table_name}
                TO '{output_path}'
                (FORMAT PARQUET)
            """)
            
        print(f"Exported {table_name} to {output_path}")

"""
Data processing utilities using Polars for efficient bird ringing data analysis.

This module provides high-performance data processing functions optimized
for large-scale bird ringing datasets.
"""

import polars as pl
from pathlib import Path
from typing import Optional, Union, List, Dict
from datetime import datetime, date


class BirdDataProcessor:
    """Process bird ringing data efficiently using Polars."""
    
    @staticmethod
    def load_csv(file_path: Union[str, Path], **kwargs) -> pl.DataFrame:
        """
        Load CSV file with optimized settings.
        
        Parameters:
        -----------
        file_path : str or Path
            Path to CSV file
        **kwargs : additional arguments passed to pl.read_csv
        
        Returns:
        --------
        pl.DataFrame
            Loaded dataframe
        """
        # Set default parameters with proper schema inference
        default_params = {
            "infer_schema_length": 10000,
            "ignore_errors": False,
            "try_parse_dates": True
        }
        
        # For bird ringing data, ensure notes column is read as string
        if "schema_overrides" not in kwargs:
            default_params["schema_overrides"] = {
                "notes": pl.Utf8  # Force notes to be string type
            }
        
        # Override defaults with any user-provided kwargs
        default_params.update(kwargs)
        
        return pl.read_csv(file_path, **default_params)
    
    @staticmethod
    def clean_ring_records(df: pl.DataFrame) -> pl.DataFrame:
        """
        Clean and standardize ring records data.
        
        Parameters:
        -----------
        df : pl.DataFrame
            Raw ring records dataframe
            
        Returns:
        --------
        pl.DataFrame
            Cleaned dataframe
        """
        # Strip whitespace from string columns
        string_cols = [col for col in df.columns if df[col].dtype == pl.Utf8]
        for col in string_cols:
            df = df.with_columns(pl.col(col).str.strip_chars().alias(col))
        
        # Convert date column to proper date type if needed
        if df["date"].dtype != pl.Date:
            df = df.with_columns(
                pl.col("date").str.to_date("%Y-%m-%d", strict=False).alias("date")
            )
        
        # Standardize empty strings to null
        for col in string_cols:
            df = df.with_columns(
                pl.when(pl.col(col) == "")
                .then(None)
                .otherwise(pl.col(col))
                .alias(col)
            )
        
        return df
    
    @staticmethod
    def add_time_features(df: pl.DataFrame, date_col: str = "date") -> pl.DataFrame:
        """
        Add derived time-based features for analysis.
        
        Parameters:
        -----------
        df : pl.DataFrame
            Input dataframe with date column
        date_col : str
            Name of date column
            
        Returns:
        --------
        pl.DataFrame
            Dataframe with additional time features
        """
        df = df.with_columns([
            pl.col(date_col).dt.year().alias("year"),
            pl.col(date_col).dt.month().alias("month"),
            pl.col(date_col).dt.day().alias("day"),
            pl.col(date_col).dt.weekday().alias("weekday"),
            pl.col(date_col).dt.ordinal_day().alias("day_of_year"),
            pl.col(date_col).dt.week().alias("week_of_year")
        ])
        
        # Add season (Northern Hemisphere)
        df = df.with_columns(
            pl.when(pl.col("month").is_in([12, 1, 2]))
            .then(pl.lit("Winter"))
            .when(pl.col("month").is_in([3, 4, 5]))
            .then(pl.lit("Spring"))
            .when(pl.col("month").is_in([6, 7, 8]))
            .then(pl.lit("Summer"))
            .otherwise(pl.lit("Autumn"))
            .alias("season")
        )
        
        return df
    
    @staticmethod
    def filter_by_date_range(
        df: pl.DataFrame,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        date_col: str = "date"
    ) -> pl.DataFrame:
        """
        Filter dataframe by date range.
        
        Parameters:
        -----------
        df : pl.DataFrame
            Input dataframe
        start_date : str or date, optional
            Start date (inclusive)
        end_date : str or date, optional
            End date (inclusive)
        date_col : str
            Name of date column
            
        Returns:
        --------
        pl.DataFrame
            Filtered dataframe
        """
        if start_date:
            df = df.filter(pl.col(date_col) >= start_date)
        if end_date:
            df = df.filter(pl.col(date_col) <= end_date)
        return df
    
    @staticmethod
    def filter_by_species(
        df: pl.DataFrame,
        species_codes: Union[str, List[str]],
        species_col: str = "species_code"
    ) -> pl.DataFrame:
        """
        Filter dataframe by species.
        
        Parameters:
        -----------
        df : pl.DataFrame
            Input dataframe
        species_codes : str or list of str
            Species code(s) to filter
        species_col : str
            Name of species column
            
        Returns:
        --------
        pl.DataFrame
            Filtered dataframe
        """
        if isinstance(species_codes, str):
            species_codes = [species_codes]
        return df.filter(pl.col(species_col).is_in(species_codes))
    
    @staticmethod
    def aggregate_daily_counts(
        df: pl.DataFrame,
        group_by: List[str] = ["date", "species_code"]
    ) -> pl.DataFrame:
        """
        Aggregate records to daily counts.
        
        Parameters:
        -----------
        df : pl.DataFrame
            Input dataframe
        group_by : list of str
            Columns to group by
            
        Returns:
        --------
        pl.DataFrame
            Aggregated dataframe with counts
        """
        return df.group_by(group_by).agg([
            pl.count().alias("count"),
            pl.col("weight").mean().alias("mean_weight"),
            pl.col("weight").std().alias("std_weight"),
            pl.col("wing_length").mean().alias("mean_wing_length"),
            pl.col("wing_length").std().alias("std_wing_length")
        ]).sort(group_by)
    
    @staticmethod
    def calculate_recapture_stats(df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate recapture statistics for ringed birds.
        
        Parameters:
        -----------
        df : pl.DataFrame
            Input dataframe with ring_number column
            
        Returns:
        --------
        pl.DataFrame
            Dataframe with recapture information
        """
        # Group by ring number and calculate stats
        recaptures = df.group_by("ring_number").agg([
            pl.count().alias("n_captures"),
            pl.col("date").min().alias("first_capture"),
            pl.col("date").max().alias("last_capture"),
            pl.col("species_code").first().alias("species_code")
        ])
        
        # Calculate days between first and last capture
        recaptures = recaptures.with_columns(
            (pl.col("last_capture") - pl.col("first_capture")).dt.total_days().alias("days_between")
        )
        
        # Filter for actual recaptures (more than one capture)
        recaptures = recaptures.filter(pl.col("n_captures") > 1)
        
        return recaptures.sort("n_captures", descending=True)
    
    @staticmethod
    def pivot_species_by_time(
        df: pl.DataFrame,
        time_column: str = "date",
        value_column: str = "species_code",
        agg_func: str = "count"
    ) -> pl.DataFrame:
        """
        Create a pivot table of species counts over time.
        
        Parameters:
        -----------
        df : pl.DataFrame
            Input dataframe
        time_column : str
            Column to use for time axis
        value_column : str
            Column to count/aggregate
        agg_func : str
            Aggregation function
            
        Returns:
        --------
        pl.DataFrame
            Pivoted dataframe
        """
        # First aggregate by time and species
        agg_df = df.group_by([time_column, value_column]).agg(
            pl.count().alias("count")
        )
        
        # Pivot
        pivot_df = agg_df.pivot(
            values="count",
            index=time_column,
            columns=value_column
        )
        
        return pivot_df.sort(time_column)
    
    @staticmethod
    def get_species_summary(df: pl.DataFrame) -> pl.DataFrame:
        """
        Generate summary statistics by species.
        
        Parameters:
        -----------
        df : pl.DataFrame
            Input dataframe
            
        Returns:
        --------
        pl.DataFrame
            Summary statistics by species
        """
        summary = df.group_by(["species_code", "swedish_name", "scientific_name"]).agg([
            pl.count().alias("total_records"),
            pl.col("ring_number").n_unique().alias("unique_individuals"),
            pl.col("date").min().alias("first_sighting"),
            pl.col("date").max().alias("last_sighting"),
            pl.col("weight").mean().alias("mean_weight"),
            pl.col("weight").std().alias("std_weight"),
            pl.col("wing_length").mean().alias("mean_wing_length"),
            pl.col("wing_length").std().alias("std_wing_length"),
            pl.col("fat_score").mean().alias("mean_fat_score"),
            pl.col("age").mode().first().alias("most_common_age")
        ]).sort("total_records", descending=True)
        
        return summary
    
    @staticmethod
    def calculate_phenology_metrics(
        df: pl.DataFrame,
        year_col: str = "year",
        day_of_year_col: str = "day_of_year"
    ) -> pl.DataFrame:
        """
        Calculate phenology metrics (migration timing) by species and year.
        
        Parameters:
        -----------
        df : pl.DataFrame
            Input dataframe with time features
        year_col : str
            Year column name
        day_of_year_col : str
            Day of year column name
            
        Returns:
        --------
        pl.DataFrame
            Phenology metrics
        """
        # Add time features if not present
        if day_of_year_col not in df.columns:
            df = BirdDataProcessor.add_time_features(df)
        
        phenology = df.group_by(["species_code", "swedish_name", year_col]).agg([
            pl.col(day_of_year_col).min().alias("first_arrival_day"),
            pl.col(day_of_year_col).quantile(0.25).alias("q25_arrival_day"),
            pl.col(day_of_year_col).median().alias("median_arrival_day"),
            pl.col(day_of_year_col).quantile(0.75).alias("q75_arrival_day"),
            pl.col(day_of_year_col).max().alias("last_departure_day"),
            pl.count().alias("n_observations")
        ]).sort(["species_code", year_col])
        
        return phenology
    
    @staticmethod
    def detect_outliers(
        df: pl.DataFrame,
        column: str,
        method: str = "iqr",
        threshold: float = 1.5
    ) -> pl.DataFrame:
        """
        Detect outliers in a numeric column.
        
        Parameters:
        -----------
        df : pl.DataFrame
            Input dataframe
        column : str
            Column to check for outliers
        method : str
            Method to use: 'iqr' or 'zscore'
        threshold : float
            Threshold for outlier detection
            
        Returns:
        --------
        pl.DataFrame
            Dataframe with outlier flag column
        """
        if method == "iqr":
            q1 = df[column].quantile(0.25)
            q3 = df[column].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - threshold * iqr
            upper_bound = q3 + threshold * iqr
            
            df = df.with_columns(
                ((pl.col(column) < lower_bound) | (pl.col(column) > upper_bound))
                .alias(f"{column}_outlier")
            )
        elif method == "zscore":
            mean = df[column].mean()
            std = df[column].std()
            
            df = df.with_columns(
                (pl.col(column) - mean).abs() / std > threshold
            ).alias(f"{column}_outlier")
        
        return df
    
    @staticmethod
    def merge_with_metadata(
        df: pl.DataFrame,
        metadata_df: pl.DataFrame,
        on: str = "species_code",
        how: str = "left"
    ) -> pl.DataFrame:
        """
        Merge ring records with metadata.
        
        Parameters:
        -----------
        df : pl.DataFrame
            Main dataframe
        metadata_df : pl.DataFrame
            Metadata dataframe
        on : str
            Column to join on
        how : str
            Join type: 'left', 'inner', 'outer'
            
        Returns:
        --------
        pl.DataFrame
            Merged dataframe
        """
        return df.join(metadata_df, on=on, how=how)
    
    @staticmethod
    def export_to_formats(
        df: pl.DataFrame,
        base_path: Union[str, Path],
        formats: List[str] = ["parquet", "csv"]
    ):
        """
        Export dataframe to multiple formats.
        
        Parameters:
        -----------
        df : pl.DataFrame
            Dataframe to export
        base_path : str or Path
            Base path for export (without extension)
        formats : list of str
            Formats to export to: 'parquet', 'csv', 'json'
        """
        base_path = Path(base_path)
        
        for fmt in formats:
            if fmt == "parquet":
                df.write_parquet(base_path.with_suffix(".parquet"))
                print(f"Exported to {base_path.with_suffix('.parquet')}")
            elif fmt == "csv":
                df.write_csv(base_path.with_suffix(".csv"))
                print(f"Exported to {base_path.with_suffix('.csv')}")
            elif fmt == "json":
                df.write_json(base_path.with_suffix(".json"))
                print(f"Exported to {base_path.with_suffix('.json')}")

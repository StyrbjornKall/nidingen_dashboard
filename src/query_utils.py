"""
Query utilities for the bird ringing dashboard.

This module provides pre-built queries optimized for common dashboard operations.
"""

from typing import Optional, List, Union, Dict
from datetime import date
import polars as pl


class BirdRingingQueries:
    """Collection of optimized queries for bird ringing data analysis."""
    
    @staticmethod
    def get_species_time_series(
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        species_codes: Optional[List[str]] = None,
        aggregation: str = "daily"
    ) -> str:
        """
        Generate SQL for species observation time series.
        
        Parameters:
        -----------
        start_date : str, optional
            Start date (YYYY-MM-DD)
        end_date : str, optional
            End date (YYYY-MM-DD)
        species_codes : list of str, optional
            Filter by specific species
        aggregation : str
            Time aggregation: 'daily', 'weekly', 'monthly', 'yearly'
            
        Returns:
        --------
        str
            SQL query string
        """
        # Base query
        query = """
        SELECT 
            {date_agg} as period,
            species_code,
            swedish_name,
            COUNT(*) as count,
            COUNT(DISTINCT ring_number) as unique_individuals,
            AVG(weight) as mean_weight,
            AVG(wing_length) as mean_wing_length
        FROM ring_records
        WHERE 1=1
        """
        
        # Date aggregation
        date_agg_map = {
            "daily": "date",
            "weekly": "DATE_TRUNC('week', date)",
            "monthly": "DATE_TRUNC('month', date)",
            "yearly": "DATE_TRUNC('year', date)"
        }
        query = query.format(date_agg=date_agg_map.get(aggregation, "date"))
        
        # Add filters
        if start_date:
            query += f"\n  AND date >= '{start_date}'"
        if end_date:
            query += f"\n  AND date <= '{end_date}'"
        if species_codes:
            species_list = "', '".join(species_codes)
            query += f"\n  AND species_code IN ('{species_list}')"
        
        # Group by
        query += """
        GROUP BY period, species_code, swedish_name
        ORDER BY period, species_code
        """
        
        return query
    
    @staticmethod
    def get_morphometric_distributions(
        species_codes: Optional[List[str]] = None,
        year: Optional[int] = None
    ) -> str:
        """
        Generate SQL for morphometric (weight, wing length) distributions.
        
        Parameters:
        -----------
        species_codes : list of str, optional
            Filter by specific species
        year : int, optional
            Filter by specific year
            
        Returns:
        --------
        str
            SQL query string
        """
        query = """
        SELECT 
            species_code,
            swedish_name,
            weight,
            wing_length,
            age,
            EXTRACT(YEAR FROM date) as year,
            EXTRACT(MONTH FROM date) as month
        FROM ring_records
        WHERE weight IS NOT NULL 
          AND wing_length IS NOT NULL
        """
        
        if species_codes:
            species_list = "', '".join(species_codes)
            query += f"\n  AND species_code IN ('{species_list}')"
        
        if year:
            query += f"\n  AND EXTRACT(YEAR FROM date) = {year}"
        
        return query
    
    @staticmethod
    def get_recapture_analysis() -> str:
        """
        Generate SQL for recapture analysis.
        
        Returns:
        --------
        str
            SQL query string
        """
        return """
        WITH captures AS (
            SELECT 
                ring_number,
                species_code,
                MIN(date) as first_capture,
                MAX(date) as last_capture,
                COUNT(*) as n_captures,
                COUNT(DISTINCT date) as n_capture_days
            FROM ring_records
            WHERE ring_number IS NOT NULL
              AND ring_number != ''
            GROUP BY ring_number, species_code
            HAVING COUNT(*) > 1
        )
        SELECT 
            ring_number,
            species_code,
            first_capture,
            last_capture,
            n_captures,
            n_capture_days,
            (last_capture - first_capture) as days_between,
            ROUND((last_capture - first_capture) / 365.25, 2) as years_between
        FROM captures
        ORDER BY n_captures DESC, days_between DESC
        """
    
    @staticmethod
    def get_phenology_by_species(
        species_codes: Optional[List[str]] = None,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
    ) -> str:
        """
        Generate SQL for phenology (migration timing) analysis.
        DEPRECATED: Use get_phenology_daily_distribution or get_phenology_weekly_distribution instead.
        This method is kept for backwards compatibility but doesn't capture bimodal migration patterns well.
        
        Parameters:
        -----------
        species_codes : list of str, optional
            Filter by specific species
        start_year : int, optional
            Start year
        end_year : int, optional
            End year
            
        Returns:
        --------
        str
            SQL query string
        """
        query = """
        SELECT 
            species_code,
            swedish_name,
            EXTRACT(YEAR FROM date) as year,
            MIN(DAYOFYEAR(date)) as first_day_of_year,
            QUANTILE_DISC(DAYOFYEAR(date), 0.25) as q25_day_of_year,
            QUANTILE_DISC(DAYOFYEAR(date), 0.50) as median_day_of_year,
            QUANTILE_DISC(DAYOFYEAR(date), 0.75) as q75_day_of_year,
            MAX(DAYOFYEAR(date)) as last_day_of_year,
            COUNT(*) as n_observations
        FROM ring_records
        WHERE date IS NOT NULL
        """
        
        if species_codes:
            species_list = "', '".join(species_codes)
            query += f"\n  AND species_code IN ('{species_list}')"
        
        if start_year:
            query += f"\n  AND EXTRACT(YEAR FROM date) >= {start_year}"
        
        if end_year:
            query += f"\n  AND EXTRACT(YEAR FROM date) <= {end_year}"
        
        query += """
        GROUP BY species_code, swedish_name, year
        ORDER BY species_code, year
        """
        
        return query
    
    @staticmethod
    def get_phenology_daily_distribution(
        species_codes: Optional[List[str]] = None,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        aggregate_years: bool = True
    ) -> str:
        """
        Generate SQL for detailed daily phenology distribution.
        Shows the actual distribution of observations across the year,
        capturing bimodal patterns (spring/autumn migration).
        
        Parameters:
        -----------
        species_codes : list of str, optional
            Filter by specific species
        start_year : int, optional
            Start year
        end_year : int, optional
            End year
        aggregate_years : bool
            If True, aggregates across all years. If False, keeps year separate.
            
        Returns:
        --------
        str
            SQL query string with columns: species_code, swedish_name, 
            day_of_year (or year+day_of_year), count, avg_count
        """
        if aggregate_years:
            query = """
            WITH daily_counts AS (
                SELECT 
                    species_code,
                    swedish_name,
                    EXTRACT(YEAR FROM date) as year,
                    DAYOFYEAR(date) as day_of_year,
                    COUNT(*) as count
                FROM ring_records
                WHERE date IS NOT NULL
            """
        else:
            query = """
            SELECT 
                species_code,
                swedish_name,
                EXTRACT(YEAR FROM date) as year,
                DAYOFYEAR(date) as day_of_year,
                COUNT(*) as count
            FROM ring_records
            WHERE date IS NOT NULL
            """
        
        if species_codes:
            species_list = "', '".join(species_codes)
            query += f"\n      AND species_code IN ('{species_list}')"
        
        if start_year:
            query += f"\n      AND EXTRACT(YEAR FROM date) >= {start_year}"
        
        if end_year:
            query += f"\n      AND EXTRACT(YEAR FROM date) <= {end_year}"
        
        if aggregate_years:
            query += """
                GROUP BY species_code, swedish_name, year, day_of_year
            )
            SELECT 
                species_code,
                swedish_name,
                day_of_year,
                SUM(count) as total_count,
                AVG(count) as avg_count,
                STDDEV(count) as std_count,
                COUNT(DISTINCT year) as n_years
            FROM daily_counts
            GROUP BY species_code, swedish_name, day_of_year
            ORDER BY species_code, day_of_year
            """
        else:
            query += """
            GROUP BY species_code, swedish_name, year, day_of_year
            ORDER BY species_code, year, day_of_year
            """
        
        return query
    
    @staticmethod
    def get_phenology_weekly_distribution(
        species_codes: Optional[List[str]] = None,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        aggregate_years: bool = True
    ) -> str:
        """
        Generate SQL for weekly phenology distribution.
        Smooths daily noise while preserving bimodal migration patterns.
        
        Parameters:
        -----------
        species_codes : list of str, optional
            Filter by specific species
        start_year : int, optional
            Start year
        end_year : int, optional
            End year
        aggregate_years : bool
            If True, aggregates across all years. If False, keeps year separate.
            
        Returns:
        --------
        str
            SQL query string with columns: species_code, swedish_name, 
            week_of_year, count, avg_count
        """
        if aggregate_years:
            query = """
            WITH weekly_counts AS (
                SELECT 
                    species_code,
                    swedish_name,
                    EXTRACT(YEAR FROM date) as year,
                    EXTRACT(WEEK FROM date) as week_of_year,
                    COUNT(*) as count
                FROM ring_records
                WHERE date IS NOT NULL
            """
        else:
            query = """
            SELECT 
                species_code,
                swedish_name,
                EXTRACT(YEAR FROM date) as year,
                EXTRACT(WEEK FROM date) as week_of_year,
                COUNT(*) as count
            FROM ring_records
            WHERE date IS NOT NULL
            """
        
        if species_codes:
            species_list = "', '".join(species_codes)
            query += f"\n      AND species_code IN ('{species_list}')"
        
        if start_year:
            query += f"\n      AND EXTRACT(YEAR FROM date) >= {start_year}"
        
        if end_year:
            query += f"\n      AND EXTRACT(YEAR FROM date) <= {end_year}"
        
        if aggregate_years:
            query += """
                GROUP BY species_code, swedish_name, year, week_of_year
            )
            SELECT 
                species_code,
                swedish_name,
                week_of_year,
                SUM(count) as total_count,
                AVG(count) as avg_count,
                STDDEV(count) as std_count,
                COUNT(DISTINCT year) as n_years
            FROM weekly_counts
            GROUP BY species_code, swedish_name, week_of_year
            ORDER BY species_code, week_of_year
            """
        else:
            query += """
            GROUP BY species_code, swedish_name, year, week_of_year
            ORDER BY species_code, year, week_of_year
            """
        
        return query
    
    @staticmethod
    def get_phenology_migration_windows(
        species_codes: Optional[List[str]] = None,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        spring_months: List[int] = [3, 4, 5],
        autumn_months: List[int] = [8, 9, 10]
    ) -> str:
        """
        Generate SQL for migration window analysis.
        Separates spring and autumn migration periods and calculates metrics for each.
        
        Parameters:
        -----------
        species_codes : list of str, optional
            Filter by specific species
        start_year : int, optional
            Start year
        end_year : int, optional
            End year
        spring_months : list of int
            Months to consider as spring migration (default: March, April, May)
        autumn_months : list of int
            Months to consider as autumn migration (default: August, September, October)
            
        Returns:
        --------
        str
            SQL query string with separate metrics for spring and autumn
        """
        spring_list = ", ".join(map(str, spring_months))
        autumn_list = ", ".join(map(str, autumn_months))
        
        query = f"""
        WITH seasonal_data AS (
            SELECT 
                species_code,
                swedish_name,
                EXTRACT(YEAR FROM date) as year,
                EXTRACT(MONTH FROM date) as month,
                DAYOFYEAR(date) as day_of_year,
                CASE 
                    WHEN EXTRACT(MONTH FROM date) IN ({spring_list}) THEN 'spring'
                    WHEN EXTRACT(MONTH FROM date) IN ({autumn_list}) THEN 'autumn'
                    ELSE 'other'
                END as season
            FROM ring_records
            WHERE date IS NOT NULL
              AND EXTRACT(MONTH FROM date) IN ({spring_list}, {autumn_list})
        """
        
        if species_codes:
            species_list = "', '".join(species_codes)
            query += f"\n      AND species_code IN ('{species_list}')"
        
        if start_year:
            query += f"\n      AND EXTRACT(YEAR FROM date) >= {start_year}"
        
        if end_year:
            query += f"\n      AND EXTRACT(YEAR FROM date) <= {end_year}"
        
        query += """
        )
        SELECT 
            species_code,
            swedish_name,
            year,
            season,
            COUNT(*) as n_observations,
            MIN(day_of_year) as first_obs,
            QUANTILE_DISC(day_of_year, 0.25) as q25,
            QUANTILE_DISC(day_of_year, 0.50) as median,
            QUANTILE_DISC(day_of_year, 0.75) as q75,
            MAX(day_of_year) as last_obs,
            q75 - q25 as iqr_days
        FROM seasonal_data
        GROUP BY species_code, swedish_name, year, season
        ORDER BY species_code, year, season
        """
        
        return query
    
    @staticmethod
    def get_ringer_statistics(
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> str:
        """
        Generate SQL for ringer activity statistics.
        
        Parameters:
        -----------
        start_date : str, optional
            Start date (YYYY-MM-DD)
        end_date : str, optional
            End date (YYYY-MM-DD)
            
        Returns:
        --------
        str
            SQL query string
        """
        query = """
        SELECT 
            ringer,
            COUNT(*) as total_rings,
            COUNT(DISTINCT species_code) as unique_species,
            COUNT(DISTINCT date) as active_days,
            MIN(date) as first_record,
            MAX(date) as last_record
        FROM ring_records
        WHERE ringer IS NOT NULL 
          AND ringer != ''
        """
        
        if start_date:
            query += f"\n  AND date >= '{start_date}'"
        if end_date:
            query += f"\n  AND date <= '{end_date}'"
        
        query += """
        GROUP BY ringer
        ORDER BY total_rings DESC
        """
        
        return query
    
    @staticmethod
    def get_species_diversity_over_time(
        aggregation: str = "monthly"
    ) -> str:
        """
        Generate SQL for species diversity over time.
        
        Parameters:
        -----------
        aggregation : str
            Time aggregation: 'daily', 'weekly', 'monthly', 'yearly'
            
        Returns:
        --------
        str
            SQL query string
        """
        date_agg_map = {
            "daily": "date",
            "weekly": "DATE_TRUNC('week', date)",
            "monthly": "DATE_TRUNC('month', date)",
            "yearly": "DATE_TRUNC('year', date)"
        }
        
        date_agg = date_agg_map.get(aggregation, "date")
        
        return f"""
        SELECT 
            {date_agg} as period,
            COUNT(DISTINCT species_code) as species_richness,
            COUNT(*) as total_observations
        FROM ring_records
        GROUP BY period
        ORDER BY period
        """
    
    @staticmethod
    def get_conditional_body_metrics(
        metric: str = "weight",
        group_by: List[str] = ["species_code", "age"]
    ) -> str:
        """
        Generate SQL for body condition metrics grouped by categories.
        
        Parameters:
        -----------
        metric : str
            Metric to analyze: 'weight', 'wing_length', 'fat_score'
        group_by : list of str
            Columns to group by
            
        Returns:
        --------
        str
            SQL query string
        """
        group_cols = ", ".join(group_by)
        
        return f"""
        SELECT 
            {group_cols},
            COUNT(*) as n,
            AVG({metric}) as mean_{metric},
            STDDEV({metric}) as std_{metric},
            MIN({metric}) as min_{metric},
            QUANTILE_DISC({metric}, 0.25) as q25_{metric},
            QUANTILE_DISC({metric}, 0.50) as median_{metric},
            QUANTILE_DISC({metric}, 0.75) as q75_{metric},
            MAX({metric}) as max_{metric}
        FROM ring_records
        WHERE {metric} IS NOT NULL
        GROUP BY {group_cols}
        ORDER BY {group_cols}
        """
    
    @staticmethod
    def get_year_over_year_comparison(
        species_codes: Optional[List[str]] = None
    ) -> str:
        """
        Generate SQL for year-over-year species count comparison.
        
        Parameters:
        -----------
        species_codes : list of str, optional
            Filter by specific species
            
        Returns:
        --------
        str
            SQL query string
        """
        query = """
        WITH yearly_counts AS (
            SELECT 
                species_code,
                swedish_name,
                EXTRACT(YEAR FROM date) as year,
                COUNT(*) as count
            FROM ring_records
            WHERE 1=1
        """
        
        if species_codes:
            species_list = "', '".join(species_codes)
            query += f"\n      AND species_code IN ('{species_list}')"
        
        query += """
            GROUP BY species_code, swedish_name, year
        ),
        with_lag AS (
            SELECT 
                *,
                LAG(count) OVER (PARTITION BY species_code ORDER BY year) as prev_year_count
            FROM yearly_counts
        )
        SELECT 
            species_code,
            swedish_name,
            year,
            count,
            prev_year_count,
            count - prev_year_count as absolute_change,
            ROUND(100.0 * (count - prev_year_count) / NULLIF(prev_year_count, 0), 2) as percent_change
        FROM with_lag
        ORDER BY species_code, year
        """
        
        return query
    
    @staticmethod
    def get_weekly_heatmap_data(
        year: Optional[int] = None,
        top_n_species: int = 30
    ) -> str:
        """
        Generate SQL for weekly observation heatmap data.
        
        Parameters:
        -----------
        year : int, optional
            Specific year to query. If None, averages across all years
        top_n_species : int
            Number of top species to include
            
        Returns:
        --------
        str
            SQL query string
        """
        if year is not None:
            # Query for specific year
            query = f"""
            WITH top_species AS (
                SELECT species_code, swedish_name, COUNT(*) as total_obs
                FROM ring_records
                WHERE EXTRACT(YEAR FROM date) = {year}
                GROUP BY species_code, swedish_name
                ORDER BY total_obs DESC
                LIMIT {top_n_species}
            ),
            weekly_counts AS (
                SELECT 
                    r.species_code,
                    r.swedish_name,
                    EXTRACT(WEEK FROM r.date) as week_of_year,
                    COUNT(*) as count
                FROM ring_records r
                INNER JOIN top_species ts ON r.species_code = ts.species_code
                WHERE EXTRACT(YEAR FROM r.date) = {year}
                GROUP BY r.species_code, r.swedish_name, week_of_year
            ),
            species_totals AS (
                SELECT 
                    species_code,
                    swedish_name,
                    SUM(count) as total_count
                FROM weekly_counts
                GROUP BY species_code, swedish_name
            )
            SELECT 
                wc.species_code,
                wc.swedish_name,
                wc.week_of_year,
                wc.count,
                st.total_count,
                ROUND(100.0 * wc.count / st.total_count, 2) as percent_of_total
            FROM weekly_counts wc
            JOIN species_totals st ON wc.species_code = st.species_code
            ORDER BY st.total_count DESC, wc.week_of_year
            """
        else:
            # Query averaging across all years
            query = f"""
            WITH top_species AS (
                SELECT species_code, swedish_name, COUNT(*) as total_obs
                FROM ring_records
                GROUP BY species_code, swedish_name
                ORDER BY total_obs DESC
                LIMIT {top_n_species}
            ),
            weekly_counts AS (
                SELECT 
                    r.species_code,
                    r.swedish_name,
                    EXTRACT(WEEK FROM r.date) as week_of_year,
                    COUNT(*) as count,
                    COUNT(DISTINCT EXTRACT(YEAR FROM r.date)) as n_years
                FROM ring_records r
                INNER JOIN top_species ts ON r.species_code = ts.species_code
                GROUP BY r.species_code, r.swedish_name, week_of_year
            ),
            species_totals AS (
                SELECT 
                    species_code,
                    swedish_name,
                    SUM(count) as total_count,
                    AVG(n_years) as avg_years
                FROM weekly_counts
                GROUP BY species_code, swedish_name
            )
            SELECT 
                wc.species_code,
                wc.swedish_name,
                wc.week_of_year,
                ROUND(wc.count / wc.n_years, 1) as avg_count,
                st.total_count,
                ROUND(100.0 * (wc.count / wc.n_years) / (st.total_count / st.avg_years), 2) as percent_of_total
            FROM weekly_counts wc
            JOIN species_totals st ON wc.species_code = st.species_code
            ORDER BY st.total_count DESC, wc.week_of_year
            """
        
        return query

    # ------------------------------------------------------------------
    # Weather queries
    # ------------------------------------------------------------------

    @staticmethod
    def get_weather_for_date_range(
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        aggregation: str = "hourly",
    ) -> str:
        """
        Retrieve SMHI weather observations for a date range.

        Parameters
        ----------
        start_date : str, optional
            Inclusive start date, ``YYYY-MM-DD``.
        end_date : str, optional
            Inclusive end date, ``YYYY-MM-DD``.
        aggregation : str
            One of ``'hourly'``, ``'daily'``, ``'weekly'``, ``'monthly'``.
            For anything coarser than hourly, values are averaged (precipitation
            is summed instead).

        Returns
        -------
        str
            SQL query string.
        """
        where_parts = ["1=1"]
        if start_date:
            where_parts.append(f"CAST(observation_time AS DATE) >= '{start_date}'")
        if end_date:
            where_parts.append(f"CAST(observation_time AS DATE) <= '{end_date}'")
        where_clause = " AND ".join(where_parts)

        if aggregation == "hourly":
            return f"""
            SELECT
                observation_time,
                CAST(observation_time AS DATE)         AS date,
                EXTRACT(HOUR FROM observation_time)    AS hour,
                temperature,
                wind_direction,
                wind_speed,
                gust_wind,
                humidity,
                precipitation,
                pressure,
                cloud_cover,
                temperature_quality,
                wind_speed_quality,
                precipitation_quality
            FROM weather_data
            WHERE {where_clause}
            ORDER BY observation_time
            """

        # Coarser aggregation
        agg_map = {
            "daily":   "CAST(observation_time AS DATE)",
            "weekly":  "DATE_TRUNC('week', observation_time)::DATE",
            "monthly": "DATE_TRUNC('month', observation_time)::DATE",
        }
        period_expr = agg_map.get(aggregation, "CAST(observation_time AS DATE)")

        return f"""
        SELECT
            {period_expr}                    AS period,
            AVG(temperature)                 AS mean_temperature,
            MIN(temperature)                 AS min_temperature,
            MAX(temperature)                 AS max_temperature,
            AVG(wind_speed)                  AS mean_wind_speed,
            MAX(gust_wind)                   AS max_gust,
            AVG(wind_direction)              AS mean_wind_direction,
            AVG(humidity)                    AS mean_humidity,
            SUM(precipitation)               AS total_precipitation,
            AVG(pressure)                    AS mean_pressure,
            AVG(cloud_cover)                 AS mean_cloud_cover,
            COUNT(*)                         AS n_obs
        FROM weather_data
        WHERE {where_clause}
        GROUP BY period
        ORDER BY period
        """

    @staticmethod
    def get_weather_joined_with_ringing(
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        species_codes: Optional[List[str]] = None,
        weather_aggregation: str = "daily",
        max_gap_hours: int = 2,
    ) -> str:
        """
        Join ringing records with weather data to enable weather-correlation
        analysis.

        Two join strategies are available via *weather_aggregation*:

        ``'daily'`` (recommended, default)
            Ringing counts per (date, species) are joined to **daily
            aggregated** weather (mean/min/max/sum).  This is robust to any
            temporal gaps in the weather archive, including the 3-hourly
            synoptic era (1982-1994), and always returns a ``data_completeness``
            column (fraction of 24 hours that have data, e.g. 0.33 for the
            pre-1996 era).

        ``'nearest'``
            Each ringing group (date + whole hour) is matched to the
            **nearest** weather observation using DuckDB's ``ASOF JOIN``.
            The result includes a ``weather_match_hours`` column showing the
            gap between the capture time and the matched observation.
            Weather columns are set to NULL when the gap exceeds
            *max_gap_hours*, so callers always receive trustworthy values.
            In the 2020+ era virtually all gaps are ≤ 1 h.

        Parameters
        ----------
        start_date : str, optional
            Inclusive start date ``YYYY-MM-DD``.
        end_date : str, optional
            Inclusive end date ``YYYY-MM-DD``.
        species_codes : list of str, optional
            Filter by species.  ``None`` returns all species.
        weather_aggregation : str
            ``'daily'`` or ``'nearest'``.
        max_gap_hours : int
            Only used when *weather_aggregation* is ``'nearest'``.  Weather
            columns are nullified when the nearest observation is more than
            this many hours away (default 2).  Use 4 for pre-1996 synoptic
            data (observations every 3 hours).

        Returns
        -------
        str
            SQL query string.
        """
        ring_where = ["r.date IS NOT NULL"]
        if start_date:
            ring_where.append(f"r.date >= '{start_date}'")
        if end_date:
            ring_where.append(f"r.date <= '{end_date}'")
        if species_codes:
            sp_list = "', '".join(species_codes)
            ring_where.append(f"r.species_code IN ('{sp_list}')")
        ring_where_sql = " AND ".join(ring_where)

        # ------------------------------------------------------------------
        # Nearest-observation join via ASOF JOIN
        # ------------------------------------------------------------------
        # DuckDB ASOF JOIN matches each left row to the greatest
        # observation_time that is <= the capture timestamp.  We then compute
        # the gap and null-out weather columns that are too stale.
        # ------------------------------------------------------------------
        if weather_aggregation == "nearest":
            return f"""
            WITH ringing AS (
                -- One row per (date, whole-hour, species) used as the time anchor.
                SELECT
                    r.date,
                    FLOOR(r.time)::INTEGER                               AS capture_hour,
                    (r.date::TIMESTAMP
                        + FLOOR(r.time)::INTEGER * INTERVAL '1 hour')::TIMESTAMPTZ
                                                                         AS capture_ts,
                    r.species_code,
                    r.swedish_name,
                    COUNT(*)                                             AS captures,
                    AVG(r.weight)                                        AS mean_weight,
                    AVG(r.fat_score)                                     AS mean_fat_score
                FROM ring_records r
                WHERE {ring_where_sql}
                GROUP BY
                    r.date,
                    FLOOR(r.time)::INTEGER,
                    (r.date::TIMESTAMP + FLOOR(r.time)::INTEGER * INTERVAL '1 hour')::TIMESTAMPTZ,
                    r.species_code,
                    r.swedish_name
            ),
            joined AS (
                -- ASOF JOIN picks the latest weather_data row with
                -- observation_time <= capture_ts.
                SELECT
                    ri.date,
                    ri.capture_hour,
                    ri.species_code,
                    ri.swedish_name,
                    ri.captures,
                    ri.mean_weight,
                    ri.mean_fat_score,
                    w.observation_time                                   AS weather_ts,
                    ROUND(
                        ABS(EPOCH(ri.capture_ts) - EPOCH(w.observation_time))
                        / 3600.0, 1
                    )                                                    AS weather_match_hours,
                    w.temperature,
                    w.wind_direction,
                    w.wind_speed,
                    w.gust_wind,
                    w.humidity,
                    w.precipitation,
                    w.pressure,
                    w.cloud_cover
                FROM ringing ri
                ASOF JOIN weather_data w
                    ON w.observation_time <= ri.capture_ts
            )
            -- Null-out weather columns where the gap is too large.
            -- weather_match_hours is always present so callers can diagnose NULLs.
            SELECT
                date,
                capture_hour,
                species_code,
                swedish_name,
                captures,
                mean_weight,
                mean_fat_score,
                weather_ts,
                weather_match_hours,
                CASE WHEN weather_match_hours <= {max_gap_hours} THEN temperature    ELSE NULL END AS temperature,
                CASE WHEN weather_match_hours <= {max_gap_hours} THEN wind_direction  ELSE NULL END AS wind_direction,
                CASE WHEN weather_match_hours <= {max_gap_hours} THEN wind_speed      ELSE NULL END AS wind_speed,
                CASE WHEN weather_match_hours <= {max_gap_hours} THEN gust_wind       ELSE NULL END AS gust_wind,
                CASE WHEN weather_match_hours <= {max_gap_hours} THEN humidity        ELSE NULL END AS humidity,
                CASE WHEN weather_match_hours <= {max_gap_hours} THEN precipitation   ELSE NULL END AS precipitation,
                CASE WHEN weather_match_hours <= {max_gap_hours} THEN pressure        ELSE NULL END AS pressure,
                CASE WHEN weather_match_hours <= {max_gap_hours} THEN cloud_cover     ELSE NULL END AS cloud_cover
            FROM joined
            ORDER BY date, capture_hour, species_code
            """

        # ------------------------------------------------------------------
        # Daily aggregation (default, most robust)
        # ------------------------------------------------------------------
        return f"""
        WITH ringing AS (
            SELECT
                r.date,
                r.species_code,
                r.swedish_name,
                COUNT(*)            AS captures,
                AVG(r.weight)       AS mean_weight,
                AVG(r.fat_score)    AS mean_fat_score
            FROM ring_records r
            WHERE {ring_where_sql}
            GROUP BY r.date, r.species_code, r.swedish_name
        ),
        weather AS (
            SELECT
                CAST(observation_time AS DATE)   AS date,
                AVG(temperature)                 AS mean_temperature,
                MIN(temperature)                 AS min_temperature,
                MAX(temperature)                 AS max_temperature,
                AVG(wind_speed)                  AS mean_wind_speed,
                MAX(gust_wind)                   AS max_gust,
                AVG(wind_direction)              AS mean_wind_direction,
                AVG(humidity)                    AS mean_humidity,
                SUM(precipitation)               AS total_precipitation,
                AVG(pressure)                    AS mean_pressure,
                AVG(cloud_cover)                 AS mean_cloud_cover,
                -- fraction of 24 h slots that have data (pre-1996 ≈ 0.33)
                COUNT(temperature) / 24.0        AS data_completeness
            FROM weather_data
            GROUP BY CAST(observation_time AS DATE)
        )
        SELECT
            ri.*,
            w.mean_temperature,
            w.min_temperature,
            w.max_temperature,
            w.mean_wind_speed,
            w.max_gust,
            w.mean_wind_direction,
            w.mean_humidity,
            w.total_precipitation,
            w.mean_pressure,
            w.mean_cloud_cover,
            w.data_completeness
        FROM ringing ri
        LEFT JOIN weather w ON ri.date = w.date
        ORDER BY ri.date, ri.species_code
        """

    @staticmethod
    def get_daily_weather_summary(
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> str:
        """
        Return a compact daily weather summary table, useful for dashboard
        overview plots (temperature, wind, rain, cloud).

        Parameters
        ----------
        start_date, end_date : str, optional
            Date range in ``YYYY-MM-DD`` format.

        Returns
        -------
        str
            SQL query string.
        """
        where_parts = ["1=1"]
        if start_date:
            where_parts.append(f"CAST(observation_time AS DATE) >= '{start_date}'")
        if end_date:
            where_parts.append(f"CAST(observation_time AS DATE) <= '{end_date}'")
        where_clause = " AND ".join(where_parts)

        return f"""
        SELECT
            CAST(observation_time AS DATE)   AS date,
            EXTRACT(YEAR  FROM observation_time)::INTEGER AS year,
            EXTRACT(MONTH FROM observation_time)::INTEGER AS month,
            EXTRACT(DOY   FROM observation_time)::INTEGER AS day_of_year,
            AVG(temperature)                 AS mean_temperature,
            MIN(temperature)                 AS min_temperature,
            MAX(temperature)                 AS max_temperature,
            AVG(wind_speed)                  AS mean_wind_speed,
            MAX(gust_wind)                   AS max_gust,
            AVG(wind_direction)              AS mean_wind_direction,
            AVG(humidity)                    AS mean_humidity,
            SUM(precipitation)               AS total_precipitation,
            AVG(pressure)                    AS mean_pressure,
            AVG(cloud_cover)                 AS mean_cloud_cover,
            COUNT(temperature) * 1.0 / 24.0 AS data_completeness   -- 1.0 = full hourly day; ~0.33 = 3-hourly synoptic (pre-1996)
        FROM weather_data
        WHERE {where_clause}
        GROUP BY CAST(observation_time AS DATE)
        ORDER BY date
        """

    @staticmethod
    def get_weather_at_capture_time(
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        species_codes: Optional[List[str]] = None,
        max_gap_hours: int = 2,
    ) -> str:
        """
        Return one row per ringing **record** (not aggregated group) with the
        nearest weather observation attached via ``ASOF JOIN``.

        This is the most granular weather join available.  Each individual
        capture gets the weather reading closest to its whole-hour capture time.
        The ``weather_match_hours`` column always tells you how large the gap is;
        weather columns are NULL when the gap exceeds *max_gap_hours*.

        **When to use this vs** ``get_weather_joined_with_ringing``

        * Use this query when you need **record-level** analysis, e.g. plotting
          individual fat scores against temperature at the moment of capture.
        * Use ``get_weather_joined_with_ringing(weather_aggregation='daily')``
          when you only need daily counts or species totals alongside daily mean
          weather — it is faster and robust to any temporal gaps.

        Parameters
        ----------
        start_date, end_date : str, optional
            Date range in ``YYYY-MM-DD`` format.
        species_codes : list of str, optional
            Restrict to these species codes.  ``None`` returns all species.
        max_gap_hours : int
            Weather columns are set to NULL when the nearest observation is
            more than this many hours from the capture time (default 2).
            Set to 4 to retain values from the pre-1996 3-hourly era.

        Returns
        -------
        str
            SQL query string.
        """
        where_parts = ["r.date IS NOT NULL", "r.time IS NOT NULL"]
        if start_date:
            where_parts.append(f"r.date >= '{start_date}'")
        if end_date:
            where_parts.append(f"r.date <= '{end_date}'")
        if species_codes:
            sp_list = "', '".join(species_codes)
            where_parts.append(f"r.species_code IN ('{sp_list}')")
        where_sql = " AND ".join(where_parts)

        return f"""
        WITH records AS (
            SELECT
                r.record_id,
                r.date,
                r.time,
                -- Build a proper timestamp: integer hours + fractional minutes, cast to TIMESTAMPTZ
                -- to match weather_data.observation_time for ASOF JOIN.
                (r.date::TIMESTAMP
                    + FLOOR(r.time)::INTEGER * INTERVAL '1 hour'
                    + FLOOR((r.time - FLOOR(r.time)) * 60)::INTEGER * INTERVAL '1 minute'
                )::TIMESTAMPTZ                                       AS capture_ts,
                r.ring_number,
                r.species_code,
                r.swedish_name,
                r.age,
                r.weight,
                r.wing_length,
                r.fat_score,
                r.muscle_score
            FROM ring_records r
            WHERE {where_sql}
        ),
        joined AS (
            SELECT
                rc.record_id,
                rc.date,
                rc.time,
                rc.ring_number,
                rc.species_code,
                rc.swedish_name,
                rc.age,
                rc.weight,
                rc.wing_length,
                rc.fat_score,
                rc.muscle_score,
                w.observation_time                                       AS weather_ts,
                ROUND(
                    ABS(EPOCH(rc.capture_ts) - EPOCH(w.observation_time))
                    / 3600.0, 2
                )                                                        AS weather_match_hours,
                w.temperature,
                w.wind_direction,
                w.wind_speed,
                w.gust_wind,
                w.humidity,
                w.precipitation,
                w.pressure,
                w.cloud_cover
            FROM records rc
            ASOF JOIN weather_data w
                ON w.observation_time <= rc.capture_ts
        )
        SELECT
            record_id,
            date,
            time,
            ring_number,
            species_code,
            swedish_name,
            age,
            weight,
            wing_length,
            fat_score,
            muscle_score,
            weather_ts,
            weather_match_hours,
            CASE WHEN weather_match_hours <= {max_gap_hours} THEN temperature    ELSE NULL END AS temperature,
            CASE WHEN weather_match_hours <= {max_gap_hours} THEN wind_direction  ELSE NULL END AS wind_direction,
            CASE WHEN weather_match_hours <= {max_gap_hours} THEN wind_speed      ELSE NULL END AS wind_speed,
            CASE WHEN weather_match_hours <= {max_gap_hours} THEN gust_wind       ELSE NULL END AS gust_wind,
            CASE WHEN weather_match_hours <= {max_gap_hours} THEN humidity        ELSE NULL END AS humidity,
            CASE WHEN weather_match_hours <= {max_gap_hours} THEN precipitation   ELSE NULL END AS precipitation,
            CASE WHEN weather_match_hours <= {max_gap_hours} THEN pressure        ELSE NULL END AS pressure,
            CASE WHEN weather_match_hours <= {max_gap_hours} THEN cloud_cover     ELSE NULL END AS cloud_cover
        FROM joined
        ORDER BY date, time, species_code
        """

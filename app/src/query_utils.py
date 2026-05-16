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
    def get_weekly_weight_by_species(
        species_codes: Optional[List[str]] = None,
        year: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> str:
        """
        Return mean (and sample-size) weight per species per week-of-year.

        Parameters
        ----------
        species_codes : list of str, optional
            Filter by specific species.
        year : int, optional
            If given, restrict to that calendar year.
            If ``None``, averages across all years within the optional
            *start_date* / *end_date* range.
        start_date, end_date : str, optional
            Inclusive date bounds ``YYYY-MM-DD``.  Only used when ``year``
            is ``None`` (i.e. all-years average mode).

        Returns
        -------
        str
            SQL query string with columns:
            ``species_code``, ``swedish_name``, ``week_of_year``,
            ``mean_weight``, ``min_weight``, ``max_weight``, ``n``.
        """
        where_parts = ["weight IS NOT NULL", "weight > 0"]
        if species_codes:
            sp = "', '".join(species_codes)
            where_parts.append(f"species_code IN ('{sp}')")
        if year is not None:
            where_parts.append(f"EXTRACT(YEAR FROM date) = {year}")
        else:
            if start_date:
                where_parts.append(f"date >= '{start_date}'")
            if end_date:
                where_parts.append(f"date <= '{end_date}'")
        where_sql = " AND ".join(where_parts)

        return f"""
        SELECT
            species_code,
            swedish_name,
            CAST(EXTRACT(WEEK FROM date) AS INTEGER)  AS week_of_year,
            AVG(weight)                               AS mean_weight,
            MIN(weight)                               AS min_weight,
            MAX(weight)                               AS max_weight,
            COUNT(*)                                  AS n
        FROM ring_records
        WHERE {where_sql}
        GROUP BY species_code, swedish_name, week_of_year
        ORDER BY species_code, week_of_year
        """

    @staticmethod
    def get_yearly_weight_by_species(
        species_codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> str:
        """
        Return mean, min, max weight per species per year, for trend analysis.

        Parameters
        ----------
        species_codes : list of str, optional
            Filter by specific species.
        start_date, end_date : str, optional
            Date range in ``YYYY-MM-DD`` format.

        Returns
        -------
        str
            SQL query string with columns:
            ``species_code``, ``swedish_name``, ``year``,
            ``mean_weight``, ``min_weight``, ``max_weight``, ``n``.
        """
        where_parts = ["weight IS NOT NULL", "weight > 0"]
        if species_codes:
            sp = "', '".join(species_codes)
            where_parts.append(f"species_code IN ('{sp}')")
        if start_date:
            where_parts.append(f"date >= '{start_date}'")
        if end_date:
            where_parts.append(f"date <= '{end_date}'")
        where_sql = " AND ".join(where_parts)

        return f"""
        SELECT
            species_code,
            swedish_name,
            CAST(EXTRACT(YEAR FROM date) AS INTEGER)  AS year,
            AVG(weight)                               AS mean_weight,
            MIN(weight)                               AS min_weight,
            MAX(weight)                               AS max_weight,
            COUNT(*)                                  AS n
        FROM ring_records
        WHERE {where_sql}
        GROUP BY species_code, swedish_name, year
        ORDER BY species_code, year
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
        top_n_species: Optional[int] = 30
    ) -> str:
        """
        Generate SQL for weekly observation heatmap data.

        Top-N species are ALWAYS ranked by total observations across all years,
        so the species set is stable regardless of which year is selected in the
        UI.  A CROSS JOIN with weeks 1-52 ensures every (species, week) pair is
        present in the result; missing observations are filled with 0.

        Parameters:
        -----------
        year : int, optional
            Specific year to display. If None, averages across all years.
        top_n_species : int, optional
            Number of top species to include. None means all species.

        Returns:
        --------
        str
            SQL query string
        """
        limit_clause = f"LIMIT {top_n_species}" if top_n_species is not None else ""

        if year is not None:
            query = f"""
            WITH top_species AS (
                -- Ranked by total observations across ALL years (year-independent)
                SELECT species_code, swedish_name, COUNT(*) AS total_obs
                FROM ring_records
                GROUP BY species_code, swedish_name
                ORDER BY total_obs DESC
                {limit_clause}
            ),
            all_weeks AS (
                SELECT generate_series AS week_of_year
                FROM generate_series(1, 52)
            ),
            weekly_counts AS (
                SELECT
                    r.species_code,
                    CAST(EXTRACT(WEEK FROM r.date) AS INTEGER) AS week_of_year,
                    COUNT(*) AS count
                FROM ring_records r
                INNER JOIN top_species ts ON r.species_code = ts.species_code
                WHERE EXTRACT(YEAR FROM r.date) = {year}
                GROUP BY r.species_code, week_of_year
            ),
            species_year_totals AS (
                SELECT species_code, SUM(count) AS total_count
                FROM weekly_counts
                GROUP BY species_code
            ),
            grid AS (
                SELECT ts.species_code, ts.swedish_name, ts.total_obs,
                       CAST(aw.week_of_year AS INTEGER) AS week_of_year
                FROM top_species ts
                CROSS JOIN all_weeks aw
            )
            SELECT
                g.species_code,
                g.swedish_name,
                g.week_of_year,
                COALESCE(wc.count, 0) AS count,
                COALESCE(syt.total_count, 0) AS total_count,
                CASE WHEN COALESCE(syt.total_count, 0) = 0 THEN 0.0
                     ELSE ROUND(100.0 * COALESCE(wc.count, 0) / syt.total_count, 2)
                END AS percent_of_total
            FROM grid g
            LEFT JOIN weekly_counts wc
                ON g.species_code = wc.species_code AND g.week_of_year = wc.week_of_year
            LEFT JOIN species_year_totals syt ON g.species_code = syt.species_code
            ORDER BY g.total_obs DESC, g.week_of_year
            """
        else:
            query = f"""
            WITH top_species AS (
                -- Ranked by total observations across ALL years
                SELECT species_code, swedish_name, COUNT(*) AS total_obs
                FROM ring_records
                GROUP BY species_code, swedish_name
                ORDER BY total_obs DESC
                {limit_clause}
            ),
            all_weeks AS (
                SELECT generate_series AS week_of_year
                FROM generate_series(1, 52)
            ),
            weekly_counts AS (
                SELECT
                    r.species_code,
                    CAST(EXTRACT(WEEK FROM r.date) AS INTEGER) AS week_of_year,
                    COUNT(*) AS count,
                    COUNT(DISTINCT EXTRACT(YEAR FROM r.date)) AS n_years
                FROM ring_records r
                INNER JOIN top_species ts ON r.species_code = ts.species_code
                GROUP BY r.species_code, week_of_year
            ),
            species_totals AS (
                SELECT species_code, SUM(count) AS total_count
                FROM weekly_counts
                GROUP BY species_code
            ),
            grid AS (
                SELECT ts.species_code, ts.swedish_name, ts.total_obs,
                       CAST(aw.week_of_year AS INTEGER) AS week_of_year
                FROM top_species ts
                CROSS JOIN all_weeks aw
            )
            SELECT
                g.species_code,
                g.swedish_name,
                g.week_of_year,
                COALESCE(
                    ROUND(CAST(wc.count AS DOUBLE) / NULLIF(wc.n_years, 0), 1),
                    0.0
                ) AS avg_count,
                COALESCE(st.total_count, 0) AS total_count,
                CASE WHEN COALESCE(st.total_count, 0) = 0 THEN 0.0
                     ELSE ROUND(100.0 * COALESCE(wc.count, 0) / st.total_count, 2)
                END AS percent_of_total
            FROM grid g
            LEFT JOIN weekly_counts wc
                ON g.species_code = wc.species_code AND g.week_of_year = wc.week_of_year
            LEFT JOIN species_totals st ON g.species_code = st.species_code
            ORDER BY g.total_obs DESC, g.week_of_year
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
                visibility,
                cloud_cover,
                temperature_quality,
                wind_speed_quality,
                precipitation_quality,
                visibility_quality
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
            AVG(visibility)                  AS mean_visibility,
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
            aggregated** weather (mean/min/max/sum).  Weather is aggregated
            over the **ringing window (03:00–13:00 UTC)** only, matching the
            hours when most birds are captured.  Precipitation, pressure,
            and visibility are gap-filled from Vinga A via ``COALESCE``.
            This is robust to any temporal gaps in the weather archive,
            including the 3-hourly synoptic era (1982-1994), and always
            returns a ``data_completeness`` column (fraction of the 10
            ringing-window hours that have data).

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
                    w.visibility,
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
                CASE WHEN weather_match_hours <= {max_gap_hours} THEN visibility      ELSE NULL END AS visibility,
                CASE WHEN weather_match_hours <= {max_gap_hours} THEN cloud_cover     ELSE NULL END AS cloud_cover
            FROM joined
            ORDER BY date, capture_hour, species_code
            """

        # ------------------------------------------------------------------
        # Daily aggregation (default, most robust)
        # Weather is aggregated over the ringing window (03:00–13:00 UTC)
        # to match the hours when most birds are captured.
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
                CAST(w.observation_time AS DATE)   AS date,
                AVG(w.temperature)                 AS mean_temperature,
                MIN(w.temperature)                 AS min_temperature,
                MAX(w.temperature)                 AS max_temperature,
                AVG(w.wind_speed)                  AS mean_wind_speed,
                MAX(w.gust_wind)                   AS max_gust,
                AVG(w.wind_direction)              AS mean_wind_direction,
                AVG(w.humidity)                    AS mean_humidity,
                SUM(COALESCE(w.precipitation, v.precipitation))  AS total_precipitation,
                AVG(COALESCE(w.pressure, v.pressure))            AS mean_pressure,
                AVG(COALESCE(w.visibility, v.visibility))        AS mean_visibility,
                AVG(w.cloud_cover)                 AS mean_cloud_cover,
                -- fraction of the 10 ringing-window hours that have data
                COUNT(w.temperature) / 10.0        AS data_completeness
            FROM weather_data w
            LEFT JOIN weather_data_vinga v ON w.observation_time = v.observation_time
            WHERE EXTRACT(HOUR FROM w.observation_time) >= 3
              AND EXTRACT(HOUR FROM w.observation_time) <= 13
            GROUP BY CAST(w.observation_time AS DATE)
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
            w.mean_visibility,
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

        Queries the precomputed ``weather_daily`` table — one row per calendar
        date — which is built by ``fetch_smhi_weather.py``.  This avoids
        GROUP BY aggregation over 300 000+ hourly rows on every request,
        making the weather dashboard tab near-instant.

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
            where_parts.append(f"date >= '{start_date}'")
        if end_date:
            where_parts.append(f"date <= '{end_date}'")
        where_clause = " AND ".join(where_parts)

        return f"""
        SELECT
            date,
            year,
            month,
            day_of_year,
            mean_temperature,
            min_temperature,
            max_temperature,
            mean_wind_speed,
            max_gust,
            mean_wind_direction,
            mean_humidity,
            total_precipitation,
            mean_pressure,
            mean_visibility,
            mean_cloud_cover,
            data_completeness,
            vinga_gap_fill_used
        FROM weather_daily
        WHERE {where_clause}
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
                w.visibility,
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
            CASE WHEN weather_match_hours <= {max_gap_hours} THEN visibility      ELSE NULL END AS visibility,
            CASE WHEN weather_match_hours <= {max_gap_hours} THEN cloud_cover     ELSE NULL END AS cloud_cover
        FROM joined
        ORDER BY date, time, species_code
        """

    # ------------------------------------------------------------------
    # Taxonomy / metadata queries
    # ------------------------------------------------------------------

    @staticmethod
    @staticmethod
    def get_rediscoveries_map_data(
        species_codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        direction: str = "both",
    ) -> str:
        """
        Return geographic data for the rediscoveries (återfynd) map.

        Combines two datasets into one result set:

        * **outbound** – Nidingen-ringed birds found elsewhere in the world.
          The coordinates (latitude, longitude) and event_date are those of the
          finding location / event, taken from the ``fynd`` table.
        * **inbound** – Foreign-ringed birds that appeared at Nidingen.
          The coordinates and ring_date are those of the *original* ringing
          location, taken from the ``frring`` table, joined via the ``fynd``
          table (which records when the bird was encountered at Nidingen).

        Parameters
        ----------
        species_codes : list of str, optional
            Filter by species code(s) (e.g. ``['RÖHAK', 'BLMES']``).
            If None, all species are returned.
        start_date : str, optional
            Earliest event date to include (``YYYY-MM-DD``).  For outbound
            events this is the find date; for inbound events this is the date
            the bird was found at Nidingen.
        end_date : str, optional
            Latest event date to include.
        direction : str
            ``'outbound'``, ``'inbound'``, or ``'both'`` (default).

        Returns
        -------
        str
            SQL query returning one row per rediscovery event with columns:
            ring_number, species_code, swedish_name, english_name,
            event_date, ring_date, latitude, longitude, city, locality,
            find_type, distance_km, days_since_ring, direction.
        """
        # Build species WHERE clause (shared between the two UNION arms)
        species_filter_fynd = ""
        species_filter_frring = ""
        if species_codes:
            codes = "', '".join(species_codes)
            species_filter_fynd   = f"  AND f.species_code IN ('{codes}')\n"
            species_filter_frring = f"  AND fr.species_code IN ('{codes}')\n"

        date_filter_outbound = ""
        date_filter_inbound  = ""
        if start_date:
            date_filter_outbound += f"  AND f.date >= '{start_date}'\n"
            date_filter_inbound  += f"  AND fj.date >= '{start_date}'\n"
        if end_date:
            date_filter_outbound += f"  AND f.date <= '{end_date}'\n"
            date_filter_inbound  += f"  AND fj.date <= '{end_date}'\n"

        outbound_sql = f"""
    SELECT
        f.ring_number,
        f.species_code,
        al.swedish_name,
        sm.english_name,
        f.date                  AS event_date,
        rr.ring_date,
        f.latitude,
        f.longitude,
        f.city,
        f.locality,
        f.find_type,
        f.distance_km,
        f.days_since_ring,
        'outbound'              AS direction
    FROM fynd f
    INNER JOIN (
        SELECT ring_number, MIN(date) AS ring_date
        FROM ring_records
        GROUP BY ring_number
    ) rr ON f.ring_number = rr.ring_number
    LEFT JOIN artkod_lookup    al ON f.species_code    = al.artkod
    LEFT JOIN species_metadata sm ON lower(al.swedish_name) = lower(sm.swedish_name)
    WHERE f.latitude  IS NOT NULL
      AND f.longitude IS NOT NULL
      AND NOT (f.latitude  BETWEEN 57.25 AND 57.35
           AND f.longitude BETWEEN 11.85 AND 11.95)
{species_filter_fynd}{date_filter_outbound}"""

        inbound_sql = f"""
    SELECT
        fr.ring_number,
        fr.species_code,
        al.swedish_name,
        sm.english_name,
        fj.date                 AS event_date,
        fr.date                 AS ring_date,
        fr.latitude,
        fr.longitude,
        fr.city,
        fr.locality,
        fj.find_type,
        fj.distance_km,
        fj.days_since_ring,
        'inbound'               AS direction
    FROM frring fr
    INNER JOIN fynd fj ON fr.ring_number = fj.ring_number
    LEFT JOIN artkod_lookup    al ON fr.species_code    = al.artkod
    LEFT JOIN species_metadata sm ON lower(al.swedish_name) = lower(sm.swedish_name)
    WHERE fr.latitude  IS NOT NULL
      AND fr.longitude IS NOT NULL
      AND NOT (fr.latitude  BETWEEN 57.25 AND 57.35
           AND fr.longitude BETWEEN 11.85 AND 11.95)
{species_filter_frring}{date_filter_inbound}"""

        if direction == "outbound":
            return outbound_sql
        elif direction == "inbound":
            return inbound_sql
        else:
            return f"{outbound_sql}\n    UNION ALL\n{inbound_sql}"

    @staticmethod
    def get_rediscoveries_species_options() -> str:
        """
        Return distinct species from the ``fynd`` and ``frring`` tables with
        their Swedish names (via ``artkod_lookup``).

        Used to populate the species dropdown on the Återfynd tab.

        Returns
        -------
        str
            SQL query returning: species_code, swedish_name (sorted).
        """
        return """
    SELECT
        src.species_code,
        al.swedish_name
    FROM (
        SELECT DISTINCT species_code FROM fynd
        UNION
        SELECT DISTINCT species_code FROM frring
    ) src
    LEFT JOIN artkod_lookup al ON src.species_code = al.artkod
    WHERE src.species_code IS NOT NULL
    ORDER BY src.species_code
    """

    @staticmethod
    def get_species_with_taxonomy(
        species_codes: Optional[List[str]] = None,
    ) -> str:
        """
        Return species list enriched with taxonomic metadata.

        Joins ``ring_records`` with ``species_metadata`` on ``swedish_name``
        to provide order, family, English name, and taxonomic sort order.

        Parameters
        ----------
        species_codes : list of str, optional
            Filter to specific species codes.

        Returns
        -------
        str
            SQL query returning: species_code, swedish_name, english_name,
            scientific_name, order_scientific_name, family_english_name,
            family_scientific_name, taxon_order, total_records.
        """
        query = """
        SELECT
            r.species_code,
            r.swedish_name,
            m.english_name,
            m.scientific_name,
            m.order_scientific_name,
            m.family_english_name,
            m.family_scientific_name,
            m.taxon_order,
            COUNT(*) AS total_records
        FROM ring_records r
        LEFT JOIN species_metadata m ON r.swedish_name = m.swedish_name
        WHERE 1=1
        """
        if species_codes:
            species_list = "', '".join(species_codes)
            query += f"\n  AND r.species_code IN ('{species_list}')"

        query += """
        GROUP BY r.species_code, r.swedish_name, m.english_name,
                 m.scientific_name, m.order_scientific_name,
                 m.family_english_name, m.family_scientific_name,
                 m.taxon_order
        ORDER BY m.taxon_order NULLS LAST, r.species_code
        """
        return query

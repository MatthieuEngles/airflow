"""BigQuery client for fetching dashboard data."""
from google.cloud import bigquery
from django.conf import settings
import pandas as pd
from functools import lru_cache
from datetime import datetime, timedelta


def get_client():
    """Get BigQuery client with credentials."""
    return bigquery.Client(project=settings.GCP_PROJECT_ID)


def query_to_dataframe(query: str) -> pd.DataFrame:
    """Execute query and return as DataFrame."""
    client = get_client()
    return client.query(query).to_dataframe()


class DashboardDataService:
    """Service for fetching dashboard data from BigQuery GOLD tables."""

    def __init__(self):
        self.project = settings.GCP_PROJECT_ID
        self.dataset = settings.BQ_DATASET_GOLD
        self._last_valid_date = None
        self._last_valid_year_month = None

    def _table(self, name: str) -> str:
        """Get fully qualified table name."""
        return f"`{self.project}.{self.dataset}.{name}`"

    def get_last_valid_date(self) -> str:
        """Get the last date with valid (non-null, non-aberrant) data."""
        if self._last_valid_date is not None:
            return self._last_valid_date

        query = f"""
        SELECT MAX(pickup_date) as last_valid_date
        FROM {self._table('daily_summary')}
        WHERE total_trips > 100
          AND total_trips IS NOT NULL
          AND total_revenue > 0
          AND total_revenue IS NOT NULL
          AND avg_trip_revenue BETWEEN 5 AND 200
        """
        df = query_to_dataframe(query)
        if not df.empty and pd.notna(df.iloc[0]['last_valid_date']):
            self._last_valid_date = df.iloc[0]['last_valid_date']
        return self._last_valid_date

    def get_last_valid_year_month(self) -> str:
        """Get the last year_month with valid data."""
        if self._last_valid_year_month is not None:
            return self._last_valid_year_month

        last_date = self.get_last_valid_date()
        if last_date is not None:
            if hasattr(last_date, 'strftime'):
                self._last_valid_year_month = last_date.strftime('%Y-%m')
            else:
                self._last_valid_year_month = str(last_date)[:7]
        return self._last_valid_year_month

    def get_kpi_summary(self) -> dict:
        """Get key performance indicators."""
        last_valid_date = self.get_last_valid_date()
        date_filter = f"AND pickup_date <= '{last_valid_date}'" if last_valid_date else ""

        query = f"""
        SELECT
            SUM(total_trips) as total_trips,
            SUM(total_revenue) as total_revenue,
            SUM(total_distance_miles) as total_distance,
            AVG(avg_trip_revenue) as avg_fare,
            AVG(avg_tip) as avg_tip,
            MIN(pickup_date) as data_start_date,
            MAX(pickup_date) as data_end_date,
            COUNT(DISTINCT pickup_date) as total_days
        FROM {self._table('daily_summary')}
        WHERE total_trips > 0
          AND total_revenue > 0
          {date_filter}
        """
        df = query_to_dataframe(query)
        if df.empty:
            return {}
        row = df.iloc[0]
        return {
            'total_trips': int(row['total_trips']) if pd.notna(row['total_trips']) else 0,
            'total_revenue': float(row['total_revenue']) if pd.notna(row['total_revenue']) else 0,
            'total_distance': float(row['total_distance']) if pd.notna(row['total_distance']) else 0,
            'avg_fare': float(row['avg_fare']) if pd.notna(row['avg_fare']) else 0,
            'avg_tip': float(row['avg_tip']) if pd.notna(row['avg_tip']) else 0,
            'data_start_date': row['data_start_date'],
            'data_end_date': row['data_end_date'],
            'total_days': int(row['total_days']) if pd.notna(row['total_days']) else 0,
        }

    def get_monthly_trends(self, limit: int = 120) -> pd.DataFrame:
        """Get monthly trends data."""
        last_valid_ym = self.get_last_valid_year_month()
        ym_filter = f"AND year_month <= '{last_valid_ym}'" if last_valid_ym else ""

        query = f"""
        SELECT
            year_month,
            year,
            month,
            total_trips,
            total_revenue,
            fare_revenue,
            tip_revenue,
            avg_daily_trips,
            revenue_per_trip,
            revenue_per_trip AS avg_fare,
            avg_distance,
            avg_duration
        FROM {self._table('monthly_trends')}
        WHERE total_trips > 0
          AND total_revenue > 0
          {ym_filter}
        ORDER BY year DESC, month DESC
        LIMIT {limit}
        """
        return query_to_dataframe(query)

    def get_daily_summary(self, days: int = 365) -> pd.DataFrame:
        """Get daily summary data."""
        last_valid_date = self.get_last_valid_date()
        date_filter = f"AND pickup_date <= '{last_valid_date}'" if last_valid_date else ""

        query = f"""
        SELECT
            pickup_date,
            year,
            month,
            day_of_week,
            total_trips,
            total_revenue,
            avg_distance_miles,
            avg_duration_minutes,
            avg_trip_revenue,
            avg_tip,
            credit_card_trips,
            cash_trips
        FROM {self._table('daily_summary')}
        WHERE total_trips > 100
          AND total_revenue > 0
          AND avg_trip_revenue BETWEEN 5 AND 200
          {date_filter}
        ORDER BY pickup_date DESC
        LIMIT {days}
        """
        return query_to_dataframe(query)

    def get_hourly_patterns(self) -> pd.DataFrame:
        """Get hourly patterns data."""
        query = f"""
        SELECT
            pickup_hour,
            pickup_day_of_week,
            day_name,
            day_type,
            total_trips,
            avg_distance,
            avg_duration,
            avg_fare,
            avg_tip,
            avg_speed
        FROM {self._table('hourly_patterns')}
        ORDER BY pickup_day_of_week, pickup_hour
        """
        return query_to_dataframe(query)

    def get_payment_analysis(self) -> pd.DataFrame:
        """Get payment analysis data."""
        last_valid_ym = self.get_last_valid_year_month()
        ym_filter = f"AND year_month <= '{last_valid_ym}'" if last_valid_ym else ""

        query = f"""
        SELECT
            year_month,
            year,
            month,
            payment_type_name,
            trip_count,
            total_revenue,
            avg_fare,
            avg_tip,
            avg_tip_pct,
            pct_of_monthly_trips
        FROM {self._table('payment_analysis')}
        WHERE trip_count > 0
          AND total_revenue > 0
          {ym_filter}
        ORDER BY year DESC, month DESC, trip_count DESC
        """
        return query_to_dataframe(query)

    def get_distance_distribution(self) -> pd.DataFrame:
        """Get trip distance distribution."""
        query = f"""
        SELECT
            distance_bucket,
            bucket_order,
            trip_count,
            pct_of_total,
            avg_duration_minutes,
            avg_fare,
            avg_tip,
            total_revenue
        FROM {self._table('trip_distance_distribution')}
        ORDER BY bucket_order
        """
        return query_to_dataframe(query)

    def get_location_stats(self, limit: int = 300) -> pd.DataFrame:
        """Get top locations by trip count."""
        query = f"""
        SELECT
            location_id,
            pickup_trips,
            dropoff_trips,
            total_trips,
            total_revenue,
            avg_fare,
            popularity_rank
        FROM {self._table('location_stats')}
        ORDER BY popularity_rank
        LIMIT {limit}
        """
        return query_to_dataframe(query)

    def get_vendor_comparison(self) -> pd.DataFrame:
        """Get vendor comparison data."""
        last_valid_ym = self.get_last_valid_year_month()
        ym_filter = f"AND year_month <= '{last_valid_ym}'" if last_valid_ym else ""

        query = f"""
        SELECT
            year_month,
            year,
            month,
            vendor_name,
            trip_count,
            total_revenue,
            avg_distance,
            avg_duration,
            avg_fare,
            avg_tip,
            market_share_pct
        FROM {self._table('vendor_comparison')}
        WHERE trip_count > 0
          AND total_revenue > 0
          {ym_filter}
        ORDER BY year DESC, month DESC, vendor_name
        """
        return query_to_dataframe(query)

    def get_year_over_year(self) -> pd.DataFrame:
        """Get year over year comparison."""
        last_valid_date = self.get_last_valid_date()
        # Get the last valid year (exclude current incomplete year if data is partial)
        if last_valid_date is not None:
            if hasattr(last_valid_date, 'year'):
                last_valid_year = last_valid_date.year
            else:
                last_valid_year = int(str(last_valid_date)[:4])
            year_filter = f"AND year <= {last_valid_year}"
        else:
            year_filter = ""

        query = f"""
        SELECT
            year,
            total_trips,
            total_revenue,
            total_tips,
            avg_distance,
            avg_duration,
            avg_fare,
            trips_yoy_change_pct,
            revenue_yoy_change_pct
        FROM {self._table('year_over_year')}
        WHERE total_trips > 0
          AND total_revenue > 0
          {year_filter}
        ORDER BY year
        """
        return query_to_dataframe(query)

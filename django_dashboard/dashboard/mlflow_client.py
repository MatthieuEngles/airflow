"""MLflow client for fetching forecasts and model information."""
import mlflow
from mlflow.tracking import MlflowClient
from django.conf import settings
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Any


class MLflowService:
    """Service for fetching ML forecasts and model info from MLflow."""

    def __init__(self):
        self.tracking_uri = getattr(settings, 'MLFLOW_TRACKING_URI', 'http://localhost:5555')
        self.experiment_name = "nyc_taxi_forecasting"
        self.client = None
        self._connected = False

        try:
            mlflow.set_tracking_uri(self.tracking_uri)
            self.client = MlflowClient(self.tracking_uri)
            # Test connection by trying to list experiments
            self.client.search_experiments(max_results=1)
            self._connected = True
        except Exception as e:
            print(f"Warning: Could not connect to MLflow at {self.tracking_uri}: {e}")
            self._connected = False

    def is_connected(self) -> bool:
        """Check if MLflow server is available."""
        return self._connected

    def get_experiment(self) -> Optional[Any]:
        """Get the forecasting experiment."""
        if not self._connected or not self.client:
            return None
        try:
            return self.client.get_experiment_by_name(self.experiment_name)
        except Exception:
            return None

    def get_recent_runs(self, limit: int = 10) -> List[Dict]:
        """Get recent ML runs with their metrics."""
        if not self._connected or not self.client:
            return []
        try:
            experiment = self.get_experiment()
            if not experiment:
                return []

            runs = self.client.search_runs(
                experiment_ids=[experiment.experiment_id],
                order_by=["start_time DESC"],
                max_results=limit,
            )

            results = []
            for run in runs:
                run_data = {
                    'run_id': run.info.run_id,
                    'run_name': run.info.run_name or run.info.run_id[:8],
                    'status': run.info.status,
                    'start_time': datetime.fromtimestamp(run.info.start_time / 1000),
                    'model_type': run.data.params.get('model_type', 'unknown'),
                    'target_metric': run.data.params.get('target_metric', 'unknown'),
                    'forecast_horizon': run.data.params.get('forecast_horizon_days', 'N/A'),
                    'mae': run.data.metrics.get('mae'),
                    'rmse': run.data.metrics.get('rmse'),
                    'mape': run.data.metrics.get('mape'),
                }
                results.append(run_data)

            return results
        except Exception as e:
            print(f"Error fetching MLflow runs: {e}")
            return []

    def get_best_run(self, target_metric: str = 'total_trips') -> Optional[Dict]:
        """Get the best performing run for a given target metric."""
        if not self._connected or not self.client:
            return None
        try:
            experiment = self.get_experiment()
            if not experiment:
                return None

            runs = self.client.search_runs(
                experiment_ids=[experiment.experiment_id],
                filter_string=f"params.target_metric = '{target_metric}'",
                order_by=["metrics.mape ASC"],
                max_results=1,
            )

            if not runs:
                return None

            run = runs[0]
            return {
                'run_id': run.info.run_id,
                'run_name': run.info.run_name or run.info.run_id[:8],
                'model_type': run.data.params.get('model_type', 'unknown'),
                'target_metric': target_metric,
                'mae': run.data.metrics.get('mae'),
                'rmse': run.data.metrics.get('rmse'),
                'mape': run.data.metrics.get('mape'),
            }
        except Exception as e:
            print(f"Error fetching best run: {e}")
            return None

    def get_forecast_data(self, run_id: str) -> Optional[pd.DataFrame]:
        """Get forecast data from a specific run."""
        if not self._connected or not self.client:
            return None
        try:
            # Download artifacts
            artifacts_path = self.client.download_artifacts(run_id, "future_forecast.csv")
            df = pd.read_csv(artifacts_path)
            df['date'] = pd.to_datetime(df['date'])
            return df
        except Exception as e:
            print(f"Error fetching forecast data: {e}")
            return None

    def get_validation_data(self, run_id: str) -> Optional[pd.DataFrame]:
        """Get validation forecast data from a specific run."""
        if not self._connected or not self.client:
            return None
        try:
            artifacts_path = self.client.download_artifacts(run_id, "forecast_validation.csv")
            df = pd.read_csv(artifacts_path)
            df['ds'] = pd.to_datetime(df['ds'])
            return df
        except Exception as e:
            print(f"Error fetching validation data: {e}")
            return None

    def get_registered_models(self) -> List[Dict]:
        """Get all registered models."""
        if not self._connected or not self.client:
            return []
        try:
            models = self.client.search_registered_models()
            results = []
            for model in models:
                if 'nyc_taxi' in model.name:
                    latest_version = None
                    if model.latest_versions:
                        latest_version = model.latest_versions[0]

                    results.append({
                        'name': model.name,
                        'latest_version': latest_version.version if latest_version else None,
                        'stage': latest_version.current_stage if latest_version else None,
                        'description': model.description,
                    })
            return results
        except Exception as e:
            print(f"Error fetching registered models: {e}")
            return []

    def get_model_comparison(self) -> pd.DataFrame:
        """Get comparison of all models for each target metric."""
        if not self._connected or not self.client:
            return pd.DataFrame()
        try:
            experiment = self.get_experiment()
            if not experiment:
                return pd.DataFrame()

            runs = self.client.search_runs(
                experiment_ids=[experiment.experiment_id],
                filter_string="status = 'FINISHED'",
                max_results=100,
            )

            data = []
            for run in runs:
                data.append({
                    'model_type': run.data.params.get('model_type', 'unknown'),
                    'target_metric': run.data.params.get('target_metric', 'unknown'),
                    'mae': run.data.metrics.get('mae'),
                    'rmse': run.data.metrics.get('rmse'),
                    'mape': run.data.metrics.get('mape'),
                    'run_id': run.info.run_id,
                    'start_time': datetime.fromtimestamp(run.info.start_time / 1000),
                })

            return pd.DataFrame(data)
        except Exception as e:
            print(f"Error fetching model comparison: {e}")
            return pd.DataFrame()

    def get_latest_forecast_with_history(self, target_metric: str = 'total_trips', history_days: int = 90) -> Dict:
        """Get the latest forecast combined with historical data."""
        from .bigquery_client import DashboardDataService

        result = {
            'historical': None,
            'forecast': None,
            'validation': None,
            'model_info': None,
        }

        try:
            # Get best model run
            best_run = self.get_best_run(target_metric)
            if not best_run:
                return result

            result['model_info'] = best_run

            # Get forecast data
            forecast_df = self.get_forecast_data(best_run['run_id'])
            if forecast_df is not None:
                result['forecast'] = forecast_df

            # Get validation data
            validation_df = self.get_validation_data(best_run['run_id'])
            if validation_df is not None:
                result['validation'] = validation_df

            # Get historical data
            bq_service = DashboardDataService()
            historical_df = bq_service.get_daily_summary(days=history_days)
            if not historical_df.empty:
                historical_df = historical_df.rename(columns={'pickup_date': 'date'})
                if target_metric == 'total_trips':
                    historical_df['value'] = historical_df['total_trips']
                elif target_metric == 'total_revenue':
                    historical_df['value'] = historical_df['total_revenue']
                else:
                    historical_df['value'] = historical_df['avg_trip_revenue']
                result['historical'] = historical_df[['date', 'value']]

            return result

        except Exception as e:
            print(f"Error getting forecast with history: {e}")
            return result

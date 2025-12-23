"""
NYC Yellow Cab ML Forecasting DAG

Time series forecasting models for trip demand prediction.
Integrates with MLflow for experiment tracking and model registry.

Uses @task.virtualenv to install mlflow in isolated environment (avoids Flask conflicts).

Available models:
- Prophet (Facebook)
- ARIMA/SARIMA
- XGBoost with time features
- LightGBM with time features
- Exponential Smoothing (Holt-Winters)
"""

from airflow import DAG
from airflow.sdk import task
from airflow.models.param import Param
from datetime import datetime, timedelta
import os

# Configuration
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'data-curriculum-478510')
BQ_DATASET_GOLD = os.environ.get('BQ_DATASET_GOLD', 'nyc_taxi_gold')
MLFLOW_TRACKING_URI = os.environ.get('MLFLOW_TRACKING_URI', 'http://mlflow_server:5000')

default_args = {
    "owner": "data-science",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
}

# Available models configuration
AVAILABLE_MODELS = {
    "prophet": "Facebook Prophet - Best for data with strong seasonality",
    "arima": "SARIMA - Classical statistical approach",
    "xgboost": "XGBoost - Gradient boosting with time features",
    "lightgbm": "LightGBM - Fast gradient boosting",
    "holtwinters": "Holt-Winters - Exponential smoothing",
}

# Requirements for virtualenv tasks
MLFLOW_REQUIREMENTS = ["mlflow>=2.9.0", "requests"]

with DAG(
    dag_id="nyc_taxi_ml_forecasting",
    description="Train time series forecasting models for NYC Taxi demand",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",  # Run on the first day of each month
    catchup=False,
    default_args=default_args,
    tags=["nyc-taxi", "ml", "forecasting", "mlflow"],
    params={
        "model_type": Param(
            default="prophet",
            type="string",
            enum=list(AVAILABLE_MODELS.keys()),
            description="Model type to train"
        ),
        "forecast_horizon_days": Param(
            default=30,
            type="integer",
            minimum=7,
            maximum=365,
            description="Number of days to forecast"
        ),
        "training_years": Param(
            default=2,
            type="integer",
            minimum=1,
            maximum=10,
            description="Years of historical data for training"
        ),
        "target_metric": Param(
            default="total_trips",
            type="string",
            enum=["total_trips", "total_revenue", "avg_trip_revenue"],
            description="Metric to forecast"
        ),
        "register_model": Param(
            default=False,
            type="boolean",
            description="Register model in MLflow Model Registry"
        ),
    },
) as dag:

    @task.virtualenv(
        task_id="setup_mlflow",
        requirements=MLFLOW_REQUIREMENTS,
        system_site_packages=False,
    )
    def setup_mlflow(mlflow_uri: str) -> dict:
        """Initialize MLflow experiment."""
        import mlflow

        mlflow.set_tracking_uri(mlflow_uri)

        experiment_name = "nyc_taxi_forecasting"
        experiment = mlflow.get_experiment_by_name(experiment_name)

        if experiment is None:
            experiment_id = mlflow.create_experiment(
                experiment_name,
                tags={"project": "nyc_taxi", "type": "time_series"}
            )
        else:
            experiment_id = experiment.experiment_id

        mlflow.set_experiment(experiment_name)

        print(f"MLflow Tracking URI: {mlflow_uri}")
        print(f"Experiment: {experiment_name} (ID: {experiment_id})")

        return {"experiment_id": experiment_id, "experiment_name": experiment_name}

    @task(task_id="fetch_training_data")
    def fetch_training_data(**context) -> dict:
        """Fetch historical data from BigQuery GOLD layer."""
        from google.cloud import bigquery
        import pandas as pd

        params = context["params"]
        training_years = params["training_years"]
        target_metric = params["target_metric"]

        client = bigquery.Client(project=GCP_PROJECT_ID)

        query = f"""
        SELECT
            pickup_date as ds,
            {target_metric} as y,
            year,
            month,
            day_of_week,
            total_trips,
            total_revenue,
            avg_trip_revenue,
            avg_distance_miles,
            avg_duration_minutes
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.daily_summary`
        WHERE pickup_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {training_years} YEAR)
        ORDER BY pickup_date
        """

        df = client.query(query).to_dataframe(create_bqstorage_client=False)

        # Convert BigQuery dbdate to proper datetime for Parquet compatibility
        # Also convert all object/dbdate columns to avoid serialization issues
        df['ds'] = pd.to_datetime(df['ds'])

        # Convert any remaining problematic dtypes
        for col in df.columns:
            if df[col].dtype == 'object' or 'date' in str(df[col].dtype).lower():
                try:
                    df[col] = pd.to_datetime(df[col])
                except (ValueError, TypeError):
                    df[col] = df[col].astype(str)

        # Save to temp file for next tasks - use CSV to avoid pyarrow type issues
        temp_path = "/tmp/training_data.csv"
        df.to_csv(temp_path, index=False)

        print(f"Fetched {len(df)} days of training data")
        print(f"Date range: {df['ds'].min()} to {df['ds'].max()}")
        print(f"Target metric: {target_metric}")

        return {
            "data_path": "/tmp/training_data.csv",
            "n_records": len(df),
            "date_min": str(df['ds'].min()),
            "date_max": str(df['ds'].max()),
            "target_metric": target_metric,
        }

    @task.virtualenv(
        task_id="train_model",
        requirements=MLFLOW_REQUIREMENTS + [
            "pandas>=2.0.0",
            "numpy>=1.24.0",
            "scikit-learn>=1.3.0",
            "prophet>=1.1.0",
            "statsmodels>=0.14.0",
            "xgboost>=2.0.0",
            "lightgbm>=4.0.0",
        ],
        system_site_packages=False,
    )
    def train_model(
        data_info: dict,
        mlflow_info: dict,
        mlflow_uri: str,
        model_type: str,
        forecast_horizon: int,
        target_metric: str,
    ) -> dict:
        """Train the selected forecasting model."""
        import mlflow
        import pandas as pd
        import numpy as np
        from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error
        import pickle
        from datetime import datetime

        def create_time_features(df):
            """Create time-based features for ML models."""
            df = df.copy()
            df['year'] = df['ds'].dt.year
            df['month'] = df['ds'].dt.month
            df['day'] = df['ds'].dt.day
            df['dayofweek'] = df['ds'].dt.dayofweek
            df['dayofyear'] = df['ds'].dt.dayofyear
            df['weekofyear'] = df['ds'].dt.isocalendar().week.astype(int)
            df['quarter'] = df['ds'].dt.quarter
            df['is_weekend'] = df['ds'].dt.dayofweek.isin([5, 6]).astype(int)
            df['is_month_start'] = df['ds'].dt.is_month_start.astype(int)
            df['is_month_end'] = df['ds'].dt.is_month_end.astype(int)
            df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
            df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
            df['day_sin'] = np.sin(2 * np.pi * df['dayofweek'] / 7)
            df['day_cos'] = np.cos(2 * np.pi * df['dayofweek'] / 7)
            return df

        def train_prophet(train_df, val_df):
            from prophet import Prophet
            model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=False,
                changepoint_prior_scale=0.05,
            )
            model.fit(train_df[['ds', 'y']])
            future = val_df[['ds']].copy()
            forecast = model.predict(future)
            return model, forecast['yhat'].values

        def train_arima(train_df, val_df):
            from statsmodels.tsa.statespace.sarimax import SARIMAX
            import warnings
            warnings.filterwarnings('ignore')
            model = SARIMAX(
                train_df['y'],
                order=(1, 1, 1),
                seasonal_order=(1, 1, 1, 7),
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            fitted_model = model.fit(disp=False)
            forecast = fitted_model.forecast(steps=len(val_df))
            return fitted_model, forecast.values

        def train_xgboost(train_df, val_df):
            import xgboost as xgb
            train_features = create_time_features(train_df)
            val_features = create_time_features(val_df)
            feature_cols = [c for c in train_features.columns if c not in ['ds', 'y']]
            X_train = train_features[feature_cols]
            y_train = train_features['y']
            X_val = val_features[feature_cols]
            model = xgb.XGBRegressor(
                n_estimators=200, max_depth=6, learning_rate=0.1,
                subsample=0.8, colsample_bytree=0.8, random_state=42,
            )
            model.fit(X_train, y_train)
            return model, model.predict(X_val)

        def train_lightgbm(train_df, val_df):
            import lightgbm as lgb
            train_features = create_time_features(train_df)
            val_features = create_time_features(val_df)
            feature_cols = [c for c in train_features.columns if c not in ['ds', 'y']]
            X_train = train_features[feature_cols]
            y_train = train_features['y']
            X_val = val_features[feature_cols]
            model = lgb.LGBMRegressor(
                n_estimators=200, max_depth=6, learning_rate=0.1,
                subsample=0.8, colsample_bytree=0.8, random_state=42, verbose=-1,
            )
            model.fit(X_train, y_train)
            return model, model.predict(X_val)

        def train_holtwinters(train_df, val_df):
            from statsmodels.tsa.holtwinters import ExponentialSmoothing
            model = ExponentialSmoothing(
                train_df['y'],
                seasonal_periods=7,
                trend='add',
                seasonal='add',
                damped_trend=True,
            )
            fitted_model = model.fit()
            return fitted_model, fitted_model.forecast(steps=len(val_df)).values

        # Load data
        df = pd.read_csv(data_info["data_path"])
        df['ds'] = pd.to_datetime(df['ds'])

        # Split data: last forecast_horizon days for validation
        train_df = df.iloc[:-forecast_horizon].copy()
        val_df = df.iloc[-forecast_horizon:].copy()

        # Get last observed date in training set for run naming
        last_train_date = train_df['ds'].max().strftime('%Y%m%d')

        mlflow.set_tracking_uri(mlflow_uri)
        mlflow.set_experiment(mlflow_info["experiment_name"])

        with mlflow.start_run(run_name=f"{model_type}_{target_metric}_trained_until_{last_train_date}"):
            # Log parameters
            mlflow.log_params({
                "model_type": model_type,
                "target_metric": target_metric,
                "forecast_horizon_days": forecast_horizon,
                "training_samples": len(train_df),
                "validation_samples": len(val_df),
                "date_range_start": str(train_df['ds'].min()),
                "date_range_end": str(train_df['ds'].max()),
            })

            # Train model based on type
            if model_type == "prophet":
                model, forecast = train_prophet(train_df, val_df)
            elif model_type == "arima":
                model, forecast = train_arima(train_df, val_df)
            elif model_type == "xgboost":
                model, forecast = train_xgboost(train_df, val_df)
            elif model_type == "lightgbm":
                model, forecast = train_lightgbm(train_df, val_df)
            elif model_type == "holtwinters":
                model, forecast = train_holtwinters(train_df, val_df)
            else:
                raise ValueError(f"Unknown model type: {model_type}")

            # Calculate metrics
            y_true = val_df['y'].values
            y_pred = forecast[:len(y_true)]

            mae = mean_absolute_error(y_true, y_pred)
            rmse = np.sqrt(mean_squared_error(y_true, y_pred))
            mape = mean_absolute_percentage_error(y_true, y_pred) * 100

            # Log metrics
            mlflow.log_metrics({"mae": mae, "rmse": rmse, "mape": mape})

            # Save model artifact
            model_path = f"/tmp/{model_type}_model.pkl"
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
            mlflow.log_artifact(model_path)

            # Save forecast
            forecast_df = pd.DataFrame({
                'ds': val_df['ds'].values,
                'y_true': y_true,
                'y_pred': y_pred,
            })
            forecast_path = "/tmp/forecast_validation.csv"
            forecast_df.to_csv(forecast_path, index=False)
            mlflow.log_artifact(forecast_path)

            run_id = mlflow.active_run().info.run_id

            print(f"Model: {model_type}")
            print(f"MAE: {mae:,.2f}")
            print(f"RMSE: {rmse:,.2f}")
            print(f"MAPE: {mape:.2f}%")
            print(f"MLflow Run ID: {run_id}")

            return {
                "run_id": run_id,
                "model_type": model_type,
                "mae": float(mae),
                "rmse": float(rmse),
                "mape": float(mape),
                "model_path": model_path,
            }

    @task.virtualenv(
        task_id="generate_future_forecast",
        requirements=MLFLOW_REQUIREMENTS + [
            "pandas>=2.0.0",
            "numpy>=1.24.0",
            "prophet>=1.1.0",
            "statsmodels>=0.14.0",
            "xgboost>=2.0.0",
            "lightgbm>=4.0.0",
        ],
        system_site_packages=False,
    )
    def generate_future_forecast(
        training_result: dict,
        data_info: dict,
        mlflow_uri: str,
        model_type: str,
        forecast_horizon: int,
    ) -> dict:
        """Generate forecast for future dates."""
        import mlflow
        import pandas as pd
        import numpy as np
        import pickle
        from datetime import timedelta

        def create_time_features(df):
            df = df.copy()
            df['year'] = df['ds'].dt.year
            df['month'] = df['ds'].dt.month
            df['day'] = df['ds'].dt.day
            df['dayofweek'] = df['ds'].dt.dayofweek
            df['dayofyear'] = df['ds'].dt.dayofyear
            df['weekofyear'] = df['ds'].dt.isocalendar().week.astype(int)
            df['quarter'] = df['ds'].dt.quarter
            df['is_weekend'] = df['ds'].dt.dayofweek.isin([5, 6]).astype(int)
            df['is_month_start'] = df['ds'].dt.is_month_start.astype(int)
            df['is_month_end'] = df['ds'].dt.is_month_end.astype(int)
            df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
            df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
            df['day_sin'] = np.sin(2 * np.pi * df['dayofweek'] / 7)
            df['day_cos'] = np.cos(2 * np.pi * df['dayofweek'] / 7)
            return df

        # Load model
        with open(training_result["model_path"], 'rb') as f:
            model = pickle.load(f)

        # Load original data for context
        df = pd.read_csv(data_info["data_path"])
        df['ds'] = pd.to_datetime(df['ds'])
        last_date = df['ds'].max()

        # Generate future dates
        future_dates = pd.date_range(
            start=last_date + timedelta(days=1),
            periods=forecast_horizon,
            freq='D'
        )

        # Generate forecast based on model type
        if model_type == "prophet":
            future_df = pd.DataFrame({'ds': future_dates})
            forecast = model.predict(future_df)
            predictions = forecast['yhat'].values
        elif model_type in ["xgboost", "lightgbm"]:
            future_df = create_time_features(pd.DataFrame({'ds': future_dates}))
            feature_cols = [c for c in future_df.columns if c != 'ds']
            predictions = model.predict(future_df[feature_cols])
        elif model_type == "arima":
            predictions = model.forecast(steps=forecast_horizon)
        elif model_type == "holtwinters":
            predictions = model.forecast(steps=forecast_horizon)
        else:
            raise ValueError(f"Unknown model type: {model_type}")

        # Handle array types
        if hasattr(predictions, 'values'):
            predictions = predictions.values

        # Save future forecast
        future_forecast_df = pd.DataFrame({
            'date': future_dates,
            'forecast': predictions,
        })

        forecast_path = "/tmp/future_forecast.csv"
        future_forecast_df.to_csv(forecast_path, index=False)

        # Log to MLflow
        mlflow.set_tracking_uri(mlflow_uri)
        with mlflow.start_run(run_id=training_result["run_id"]):
            mlflow.log_artifact(forecast_path)

        print(f"Generated {forecast_horizon}-day forecast")
        print(f"Forecast period: {future_dates[0]} to {future_dates[-1]}")
        print(f"Predicted range: {predictions.min():,.0f} - {predictions.max():,.0f}")

        return {
            "forecast_path": forecast_path,
            "forecast_start": str(future_dates[0]),
            "forecast_end": str(future_dates[-1]),
            "mean_forecast": float(predictions.mean()),
        }

    @task.virtualenv(
        task_id="register_model",
        requirements=MLFLOW_REQUIREMENTS,
        system_site_packages=False,
    )
    def register_model_task(
        training_result: dict,
        mlflow_uri: str,
        model_type: str,
        target_metric: str,
        should_register: bool,
    ) -> dict:
        """Register model in MLflow Model Registry."""
        import mlflow

        if not should_register:
            print("Model registration skipped (register_model=False)")
            return {"registered": False}

        mlflow.set_tracking_uri(mlflow_uri)

        model_name = f"nyc_taxi_{target_metric}_{model_type}"

        # Note: mlflow.register_model requires the model to be logged via log_model()
        # Since we use log_artifact(), registration is not supported for now
        # This would require using mlflow.sklearn.log_model() or mlflow.pyfunc.log_model()
        print(f"Model registration requested but not implemented yet.")
        print(f"Model artifacts are available at: runs:/{training_result['run_id']}/")
        print(f"To enable registration, the model must be logged via mlflow.log_model()")

        return {
            "registered": False,
            "model_name": model_name,
            "reason": "log_artifact used instead of log_model - registration not supported",
        }

    @task(task_id="log_completion")
    def log_completion(training_result: dict, forecast_result: dict, registration_result: dict):
        """Log pipeline completion summary."""
        print("=" * 60)
        print("ML FORECASTING PIPELINE COMPLETE")
        print("=" * 60)
        print(f"Model Type: {training_result['model_type']}")
        print(f"MLflow Run ID: {training_result['run_id']}")
        print("-" * 40)
        print("Validation Metrics:")
        print(f"  MAE:  {training_result['mae']:,.2f}")
        print(f"  RMSE: {training_result['rmse']:,.2f}")
        print(f"  MAPE: {training_result['mape']:.2f}%")
        print("-" * 40)
        print(f"Forecast Period: {forecast_result['forecast_start']} to {forecast_result['forecast_end']}")
        print(f"Mean Forecast: {forecast_result['mean_forecast']:,.0f}")
        print("-" * 40)
        if registration_result.get("registered"):
            print(f"Model Registered: {registration_result['model_name']} v{registration_result['version']}")
        else:
            print("Model Registration: Skipped")
        print("=" * 60)
        print(f"\nView results at: {MLFLOW_TRACKING_URI}")

    # DAG Flow - pass params explicitly to virtualenv tasks
    mlflow_setup = setup_mlflow(mlflow_uri=MLFLOW_TRACKING_URI)
    data = fetch_training_data()

    # Extract params for virtualenv tasks (they can't access context directly)
    @task(task_id="extract_params")
    def extract_params(**context) -> dict:
        return {
            "model_type": context["params"]["model_type"],
            "forecast_horizon": context["params"]["forecast_horizon_days"],
            "target_metric": context["params"]["target_metric"],
            "register_model": context["params"]["register_model"],
        }

    params = extract_params()

    training = train_model(
        data_info=data,
        mlflow_info=mlflow_setup,
        mlflow_uri=MLFLOW_TRACKING_URI,
        model_type=params["model_type"],
        forecast_horizon=params["forecast_horizon"],
        target_metric=params["target_metric"],
    )

    forecast = generate_future_forecast(
        training_result=training,
        data_info=data,
        mlflow_uri=MLFLOW_TRACKING_URI,
        model_type=params["model_type"],
        forecast_horizon=params["forecast_horizon"],
    )

    registration = register_model_task(
        training_result=training,
        mlflow_uri=MLFLOW_TRACKING_URI,
        model_type=params["model_type"],
        target_metric=params["target_metric"],
        should_register=params["register_model"],
    )

    [forecast, registration] >> log_completion(training, forecast, registration)

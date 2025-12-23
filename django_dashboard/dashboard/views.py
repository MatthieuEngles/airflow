"""Views for NYC Taxi Dashboard."""
from django.shortcuts import render
from django.http import JsonResponse
from .bigquery_client import DashboardDataService
from .mlflow_client import MLflowService
from . import charts


def index(request):
    """Main dashboard view."""
    try:
        service = DashboardDataService()

        # Get KPIs
        kpis = service.get_kpi_summary()

        # Get data for charts
        monthly_trends = service.get_monthly_trends()
        hourly_patterns = service.get_hourly_patterns()
        payment_analysis = service.get_payment_analysis()
        distance_dist = service.get_distance_distribution()
        vendor_data = service.get_vendor_comparison()
        yoy_data = service.get_year_over_year()
        location_stats = service.get_location_stats()
        daily_summary = service.get_daily_summary(days=90)

        # Generate charts
        context = {
            'kpis': kpis,
            'monthly_trips_chart': charts.create_monthly_trips_chart(monthly_trends),
            'monthly_revenue_chart': charts.create_monthly_revenue_chart(monthly_trends),
            'hourly_heatmap': charts.create_hourly_heatmap(hourly_patterns),
            'payment_pie_chart': charts.create_payment_pie_chart(payment_analysis),
            'distance_chart': charts.create_distance_distribution_chart(distance_dist),
            'vendor_chart': charts.create_vendor_market_share_chart(vendor_data),
            'yoy_chart': charts.create_yoy_comparison_chart(yoy_data),
            'avg_metrics_chart': charts.create_avg_metrics_chart(monthly_trends),
            'locations_map': charts.create_locations_map(location_stats),
            'locations_chart': charts.create_top_locations_chart(location_stats),
            'daily_chart': charts.create_daily_trends_chart(daily_summary),
            'tip_chart': charts.create_tip_analysis_chart(payment_analysis),
        }

        return render(request, 'dashboard/index.html', context)

    except Exception as e:
        return render(request, 'dashboard/error.html', {'error': str(e)})


def api_kpis(request):
    """API endpoint for KPIs."""
    try:
        service = DashboardDataService()
        kpis = service.get_kpi_summary()
        return JsonResponse(kpis)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def api_monthly_trends(request):
    """API endpoint for monthly trends."""
    try:
        service = DashboardDataService()
        df = service.get_monthly_trends()
        return JsonResponse(df.to_dict(orient='records'), safe=False)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def api_hourly_patterns(request):
    """API endpoint for hourly patterns."""
    try:
        service = DashboardDataService()
        df = service.get_hourly_patterns()
        return JsonResponse(df.to_dict(orient='records'), safe=False)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def trends_view(request):
    """Trends analysis page."""
    try:
        service = DashboardDataService()

        monthly_trends = service.get_monthly_trends()
        yoy_data = service.get_year_over_year()
        daily_summary = service.get_daily_summary(days=365)

        context = {
            'monthly_trips_chart': charts.create_monthly_trips_chart(monthly_trends),
            'monthly_revenue_chart': charts.create_monthly_revenue_chart(monthly_trends),
            'yoy_chart': charts.create_yoy_comparison_chart(yoy_data),
            'avg_metrics_chart': charts.create_avg_metrics_chart(monthly_trends),
            'daily_chart': charts.create_daily_trends_chart(daily_summary),
        }

        return render(request, 'dashboard/trends.html', context)

    except Exception as e:
        return render(request, 'dashboard/error.html', {'error': str(e)})


def patterns_view(request):
    """Patterns analysis page."""
    try:
        service = DashboardDataService()

        hourly_patterns = service.get_hourly_patterns()
        distance_dist = service.get_distance_distribution()
        location_stats = service.get_location_stats()

        context = {
            'hourly_heatmap': charts.create_hourly_heatmap(hourly_patterns),
            'distance_chart': charts.create_distance_distribution_chart(distance_dist),
            'locations_map': charts.create_locations_map(location_stats),
            'locations_chart': charts.create_top_locations_chart(location_stats),
        }

        return render(request, 'dashboard/patterns.html', context)

    except Exception as e:
        return render(request, 'dashboard/error.html', {'error': str(e)})


def payments_view(request):
    """Payments analysis page."""
    try:
        service = DashboardDataService()

        payment_analysis = service.get_payment_analysis()
        vendor_data = service.get_vendor_comparison()

        context = {
            'payment_pie_chart': charts.create_payment_pie_chart(payment_analysis),
            'vendor_chart': charts.create_vendor_market_share_chart(vendor_data),
            'tip_chart': charts.create_tip_analysis_chart(payment_analysis),
        }

        return render(request, 'dashboard/payments.html', context)

    except Exception as e:
        return render(request, 'dashboard/error.html', {'error': str(e)})


def forecasts_view(request):
    """ML Forecasts page."""
    try:
        mlflow_service = MLflowService()

        # Get target metric from query params
        target_metric = request.GET.get('metric', 'total_trips')

        # Get recent runs
        recent_runs = mlflow_service.get_recent_runs(limit=20)

        # Get model comparison data
        model_comparison = mlflow_service.get_model_comparison()

        # Get forecast with historical data
        forecast_data = mlflow_service.get_latest_forecast_with_history(
            target_metric=target_metric,
            history_days=90
        )

        # Generate charts
        context = {
            'target_metric': target_metric,
            'recent_runs': recent_runs,
            'model_info': forecast_data.get('model_info'),
            'has_forecast': forecast_data.get('forecast') is not None,
            'forecast_chart': charts.create_forecast_chart(
                historical_df=forecast_data.get('historical'),
                forecast_df=forecast_data.get('forecast'),
                validation_df=forecast_data.get('validation'),
                model_info=forecast_data.get('model_info')
            ) if forecast_data.get('forecast') is not None else None,
            'model_comparison_chart': charts.create_model_comparison_chart(model_comparison),
            'accuracy_chart': charts.create_forecast_accuracy_chart(
                forecast_data.get('validation')
            ) if forecast_data.get('validation') is not None else None,
            'runs_history_chart': charts.create_runs_history_chart(recent_runs),
            'registered_models': mlflow_service.get_registered_models(),
        }

        return render(request, 'dashboard/forecasts.html', context)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return render(request, 'dashboard/error.html', {
            'error': str(e),
            'hint': 'Make sure MLflow server is running on port 5555 and you have run at least one ML forecast DAG.'
        })


def api_forecast(request):
    """API endpoint for forecast data."""
    try:
        mlflow_service = MLflowService()
        target_metric = request.GET.get('metric', 'total_trips')

        forecast_data = mlflow_service.get_latest_forecast_with_history(
            target_metric=target_metric,
            history_days=90
        )

        result = {
            'model_info': forecast_data.get('model_info'),
            'has_forecast': forecast_data.get('forecast') is not None,
        }

        if forecast_data.get('forecast') is not None:
            result['forecast'] = forecast_data['forecast'].to_dict(orient='records')

        if forecast_data.get('historical') is not None:
            result['historical'] = forecast_data['historical'].to_dict(orient='records')

        return JsonResponse(result)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def api_ml_runs(request):
    """API endpoint for ML runs."""
    try:
        mlflow_service = MLflowService()
        runs = mlflow_service.get_recent_runs(limit=50)
        return JsonResponse(runs, safe=False)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)

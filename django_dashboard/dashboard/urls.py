"""URL configuration for dashboard app."""
from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('trends/', views.trends_view, name='trends'),
    path('patterns/', views.patterns_view, name='patterns'),
    path('payments/', views.payments_view, name='payments'),
    path('forecasts/', views.forecasts_view, name='forecasts'),

    # API endpoints
    path('api/kpis/', views.api_kpis, name='api_kpis'),
    path('api/monthly-trends/', views.api_monthly_trends, name='api_monthly_trends'),
    path('api/hourly-patterns/', views.api_hourly_patterns, name='api_hourly_patterns'),
    path('api/forecast/', views.api_forecast, name='api_forecast'),
    path('api/ml-runs/', views.api_ml_runs, name='api_ml_runs'),
]

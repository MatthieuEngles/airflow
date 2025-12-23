"""Chart generation using Plotly."""
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import json
import os


def format_number(n, decimals=0):
    """Format large numbers with K, M, B suffixes."""
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.{decimals}f}B"
    elif n >= 1_000_000:
        return f"{n/1_000_000:.{decimals}f}M"
    elif n >= 1_000:
        return f"{n/1_000:.{decimals}f}K"
    return f"{n:.{decimals}f}"


def create_monthly_trips_chart(df: pd.DataFrame) -> str:
    """Create monthly trips trend chart."""
    df = df.sort_values(['year', 'month'])

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['year_month'],
        y=df['total_trips'],
        mode='lines+markers',
        name='Total Trips',
        line=dict(color='#2563eb', width=2),
        marker=dict(size=4),
        hovertemplate='%{x}<br>Trips: %{y:,.0f}<extra></extra>'
    ))

    fig.update_layout(
        title='Monthly Trip Volume',
        xaxis_title='Month',
        yaxis_title='Number of Trips',
        hovermode='x unified',
        template='plotly_white',
        height=400,
        margin=dict(l=60, r=20, t=60, b=60)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_monthly_revenue_chart(df: pd.DataFrame) -> str:
    """Create monthly revenue trend chart."""
    df = df.sort_values(['year', 'month'])

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['year_month'],
        y=df['total_revenue'],
        mode='lines+markers',
        name='Total Revenue',
        line=dict(color='#16a34a', width=2),
        fill='tozeroy',
        fillcolor='rgba(22, 163, 74, 0.1)',
        hovertemplate='%{x}<br>Revenue: $%{y:,.0f}<extra></extra>'
    ))

    fig.update_layout(
        title='Monthly Revenue',
        xaxis_title='Month',
        yaxis_title='Revenue ($)',
        hovermode='x unified',
        template='plotly_white',
        height=400,
        margin=dict(l=60, r=20, t=60, b=60)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_hourly_heatmap(df: pd.DataFrame) -> str:
    """Create hourly patterns heatmap."""
    if df.empty:
        return "<p class='text-gray-500'>No hourly pattern data available.</p>"

    pivot = df.pivot(index='day_name', columns='pickup_hour', values='total_trips')

    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    pivot = pivot.reindex(day_order)

    if pivot.empty:
        return "<p class='text-gray-500'>No hourly pattern data available.</p>"

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns,
        y=pivot.index,
        colorscale='YlOrRd',
        hovertemplate='Hour: %{x}<br>Day: %{y}<br>Trips: %{z:,.0f}<extra></extra>'
    ))

    fig.update_layout(
        title='Trip Volume by Hour and Day of Week',
        xaxis_title='Hour of Day',
        yaxis_title='Day of Week',
        template='plotly_white',
        height=400,
        margin=dict(l=100, r=20, t=60, b=60)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_payment_pie_chart(df: pd.DataFrame) -> str:
    """Create payment type distribution pie chart."""
    if df.empty:
        return "<p class='text-gray-500'>No payment data available.</p>"

    latest_month = df['year_month'].max()
    month_data = df[df['year_month'] == latest_month]

    if month_data.empty:
        return "<p class='text-gray-500'>No payment data for the latest month.</p>"

    fig = go.Figure(data=[go.Pie(
        labels=month_data['payment_type_name'],
        values=month_data['trip_count'],
        hole=0.4,
        marker_colors=['#2563eb', '#16a34a', '#f59e0b', '#ef4444', '#8b5cf6', '#6b7280'],
        hovertemplate='%{label}<br>Trips: %{value:,.0f}<br>%{percent}<extra></extra>'
    )])

    fig.update_layout(
        title=f'Payment Type Distribution ({latest_month})',
        template='plotly_white',
        height=400,
        margin=dict(l=20, r=20, t=60, b=20)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_distance_distribution_chart(df: pd.DataFrame) -> str:
    """Create trip distance distribution bar chart."""
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df['distance_bucket'],
        y=df['trip_count'],
        marker_color='#8b5cf6',
        text=df['pct_of_total'].apply(lambda x: f'{x:.1f}%'),
        textposition='outside',
        hovertemplate='%{x}<br>Trips: %{y:,.0f}<br>%{text}<extra></extra>'
    ))

    fig.update_layout(
        title='Trip Distribution by Distance',
        xaxis_title='Distance Range',
        yaxis_title='Number of Trips',
        template='plotly_white',
        height=400,
        margin=dict(l=60, r=20, t=60, b=60)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_vendor_market_share_chart(df: pd.DataFrame) -> str:
    """Create vendor market share over time chart."""
    df = df.sort_values(['year', 'month'])

    fig = go.Figure()

    for vendor in df['vendor_name'].unique():
        vendor_data = df[df['vendor_name'] == vendor]
        fig.add_trace(go.Scatter(
            x=vendor_data['year_month'],
            y=vendor_data['market_share_pct'],
            mode='lines',
            name=vendor,
            stackgroup='one',
            hovertemplate='%{x}<br>' + vendor + '<br>Share: %{y:.1f}%<extra></extra>'
        ))

    fig.update_layout(
        title='Vendor Market Share Over Time',
        xaxis_title='Month',
        yaxis_title='Market Share (%)',
        hovermode='x unified',
        template='plotly_white',
        height=400,
        margin=dict(l=60, r=20, t=60, b=60)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_yoy_comparison_chart(df: pd.DataFrame) -> str:
    """Create year-over-year comparison chart."""
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Annual Trip Volume', 'Annual Revenue')
    )

    # Trips
    fig.add_trace(
        go.Bar(
            x=df['year'],
            y=df['total_trips'],
            name='Trips',
            marker_color='#2563eb',
            hovertemplate='Year: %{x}<br>Trips: %{y:,.0f}<extra></extra>'
        ),
        row=1, col=1
    )

    # Revenue
    fig.add_trace(
        go.Bar(
            x=df['year'],
            y=df['total_revenue'],
            name='Revenue',
            marker_color='#16a34a',
            hovertemplate='Year: %{x}<br>Revenue: $%{y:,.0f}<extra></extra>'
        ),
        row=1, col=2
    )

    fig.update_layout(
        title='Year-over-Year Comparison',
        showlegend=False,
        template='plotly_white',
        height=400,
        margin=dict(l=60, r=20, t=80, b=60)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_avg_metrics_chart(df: pd.DataFrame) -> str:
    """Create average metrics over time chart."""
    df = df.sort_values(['year', 'month'])

    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            'Average Trip Distance (miles)',
            'Average Trip Duration (min)',
            'Average Fare ($)',
            'Revenue per Trip ($)'
        )
    )

    # Distance
    fig.add_trace(
        go.Scatter(x=df['year_month'], y=df['avg_distance'], mode='lines',
                   line=dict(color='#2563eb'), name='Avg Distance'),
        row=1, col=1
    )

    # Duration
    fig.add_trace(
        go.Scatter(x=df['year_month'], y=df['avg_duration'], mode='lines',
                   line=dict(color='#16a34a'), name='Avg Duration'),
        row=1, col=2
    )

    # Fare
    fig.add_trace(
        go.Scatter(x=df['year_month'], y=df['avg_fare'], mode='lines',
                   line=dict(color='#f59e0b'), name='Avg Fare'),
        row=2, col=1
    )

    # Revenue per trip
    fig.add_trace(
        go.Scatter(x=df['year_month'], y=df['revenue_per_trip'], mode='lines',
                   line=dict(color='#8b5cf6'), name='Revenue/Trip'),
        row=2, col=2
    )

    fig.update_layout(
        title='Trip Metrics Trends',
        showlegend=False,
        template='plotly_white',
        height=600,
        margin=dict(l=60, r=20, t=80, b=60)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_top_locations_chart(df: pd.DataFrame, top_n: int = 20) -> str:
    """Create top locations bar chart."""
    top_df = df.head(top_n)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=top_df['location_id'].astype(str),
        x=top_df['pickup_trips'],
        name='Pickups',
        orientation='h',
        marker_color='#2563eb',
        hovertemplate='Zone %{y}<br>Pickups: %{x:,.0f}<extra></extra>'
    ))

    fig.add_trace(go.Bar(
        y=top_df['location_id'].astype(str),
        x=top_df['dropoff_trips'],
        name='Dropoffs',
        orientation='h',
        marker_color='#f59e0b',
        hovertemplate='Zone %{y}<br>Dropoffs: %{x:,.0f}<extra></extra>'
    ))

    fig.update_layout(
        title=f'Top {top_n} Taxi Zones by Trip Volume',
        xaxis_title='Number of Trips',
        yaxis_title='Taxi Zone ID',
        barmode='group',
        template='plotly_white',
        height=600,
        margin=dict(l=80, r=20, t=60, b=60),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_locations_map(df: pd.DataFrame) -> str:
    """Create choropleth map of NYC taxi zones showing trip volume."""
    if df.empty:
        return "<p class='text-gray-500'>No location data available.</p>"

    # Load GeoJSON file
    geojson_path = os.path.join(os.path.dirname(__file__), '..', 'static', 'nyc_taxi_zones.geojson')

    if not os.path.exists(geojson_path):
        return "<p class='text-gray-500'>GeoJSON file not found. Map cannot be displayed.</p>"

    try:
        with open(geojson_path, 'r') as f:
            geojson = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        return f"<p class='text-gray-500'>Error loading GeoJSON: {e}</p>"

    # Create a mapping from LocationID to zone name
    zone_names = {}
    for feature in geojson.get('features', []):
        props = feature.get('properties', {})
        loc_id = props.get('LocationID')
        if loc_id is not None:
            zone_name = props.get('zone', f'Zone {loc_id}')
            borough = props.get('borough', 'Unknown')
            zone_names[loc_id] = {'zone': zone_name, 'borough': borough}

    # Add zone info to dataframe
    df = df.copy()
    # Ensure location_id is int to match GeoJSON LocationID type
    df['location_id'] = df['location_id'].astype(int)
    df['zone_name'] = df['location_id'].map(lambda x: zone_names.get(x, {}).get('zone', f'Zone {x}'))
    df['borough'] = df['location_id'].map(lambda x: zone_names.get(x, {}).get('borough', 'Unknown'))

    # Create choropleth map
    fig = px.choropleth_mapbox(
        df,
        geojson=geojson,
        locations='location_id',
        featureidkey='properties.LocationID',
        color='total_trips',
        color_continuous_scale='YlOrRd',
        mapbox_style='carto-positron',
        zoom=9.5,
        center={'lat': 40.7128, 'lon': -73.95},
        opacity=0.7,
        hover_name='zone_name',
        hover_data={
            'location_id': True,
            'borough': True,
            'total_trips': ':,.0f',
            'pickup_trips': ':,.0f',
            'dropoff_trips': ':,.0f',
            'total_revenue': ':$,.0f'
        },
        labels={
            'total_trips': 'Total Trips',
            'pickup_trips': 'Pickups',
            'dropoff_trips': 'Dropoffs',
            'total_revenue': 'Revenue',
            'location_id': 'Zone ID',
            'borough': 'Borough'
        }
    )

    fig.update_layout(
        title='NYC Taxi Zone Activity Map',
        margin=dict(l=0, r=0, t=50, b=0),
        height=600,
        coloraxis_colorbar=dict(
            title='Total Trips',
            tickformat=',d'
        )
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_daily_trends_chart(df: pd.DataFrame) -> str:
    """Create daily trends chart for recent data."""
    df = df.sort_values('pickup_date')

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(
            x=df['pickup_date'],
            y=df['total_trips'],
            mode='lines',
            name='Trips',
            line=dict(color='#2563eb', width=1),
            hovertemplate='%{x}<br>Trips: %{y:,.0f}<extra></extra>'
        ),
        secondary_y=False
    )

    fig.add_trace(
        go.Scatter(
            x=df['pickup_date'],
            y=df['total_revenue'],
            mode='lines',
            name='Revenue',
            line=dict(color='#16a34a', width=1),
            hovertemplate='%{x}<br>Revenue: $%{y:,.0f}<extra></extra>'
        ),
        secondary_y=True
    )

    fig.update_layout(
        title='Daily Trip Volume and Revenue',
        hovermode='x unified',
        template='plotly_white',
        height=400,
        margin=dict(l=60, r=60, t=60, b=60),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )

    fig.update_yaxes(title_text="Trips", secondary_y=False)
    fig.update_yaxes(title_text="Revenue ($)", secondary_y=True)

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_tip_analysis_chart(df: pd.DataFrame) -> str:
    """Create tip analysis by payment type chart."""
    if df.empty:
        return "<p class='text-gray-500'>No tip data available.</p>"

    latest_months = df['year_month'].unique()[:12]
    recent_df = df[df['year_month'].isin(latest_months)]

    if recent_df.empty:
        return "<p class='text-gray-500'>No recent tip data available.</p>"

    agg = recent_df.groupby('payment_type_name').agg({
        'avg_tip': 'mean',
        'avg_tip_pct': 'mean',
        'trip_count': 'sum'
    }).reset_index()

    if agg.empty:
        return "<p class='text-gray-500'>No tip data by payment type available.</p>"

    fig = make_subplots(rows=1, cols=2, subplot_titles=('Average Tip ($)', 'Average Tip %'))

    colors = ['#2563eb', '#16a34a', '#f59e0b', '#ef4444', '#8b5cf6', '#6b7280']

    fig.add_trace(
        go.Bar(
            x=agg['payment_type_name'],
            y=agg['avg_tip'],
            marker_color=colors[:len(agg)],
            hovertemplate='%{x}<br>Avg Tip: $%{y:.2f}<extra></extra>'
        ),
        row=1, col=1
    )

    fig.add_trace(
        go.Bar(
            x=agg['payment_type_name'],
            y=agg['avg_tip_pct'],
            marker_color=colors[:len(agg)],
            hovertemplate='%{x}<br>Avg Tip: %{y:.1f}%<extra></extra>'
        ),
        row=1, col=2
    )

    fig.update_layout(
        title='Tip Analysis by Payment Type (Last 12 Months)',
        showlegend=False,
        template='plotly_white',
        height=400,
        margin=dict(l=60, r=20, t=80, b=100)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


# =============================================================================
# ML FORECAST CHARTS
# =============================================================================

def create_forecast_chart(historical_df: pd.DataFrame, forecast_df: pd.DataFrame,
                          validation_df: pd.DataFrame = None, model_info: dict = None) -> str:
    """Create forecast chart with historical data and predictions."""
    fig = go.Figure()

    # Historical data
    if historical_df is not None and not historical_df.empty:
        fig.add_trace(go.Scatter(
            x=historical_df['date'],
            y=historical_df['value'],
            mode='lines',
            name='Historical',
            line=dict(color='#2563eb', width=2),
            hovertemplate='%{x}<br>Actual: %{y:,.0f}<extra></extra>'
        ))

    # Validation predictions (if available)
    if validation_df is not None and not validation_df.empty:
        fig.add_trace(go.Scatter(
            x=validation_df['ds'],
            y=validation_df['y_true'],
            mode='lines',
            name='Actual (Validation)',
            line=dict(color='#2563eb', width=2, dash='dot'),
            hovertemplate='%{x}<br>Actual: %{y:,.0f}<extra></extra>'
        ))

        fig.add_trace(go.Scatter(
            x=validation_df['ds'],
            y=validation_df['y_pred'],
            mode='lines',
            name='Predicted (Validation)',
            line=dict(color='#f59e0b', width=2),
            hovertemplate='%{x}<br>Predicted: %{y:,.0f}<extra></extra>'
        ))

    # Future forecast
    if forecast_df is not None and not forecast_df.empty:
        fig.add_trace(go.Scatter(
            x=forecast_df['date'],
            y=forecast_df['forecast'],
            mode='lines',
            name='Forecast',
            line=dict(color='#16a34a', width=3),
            hovertemplate='%{x}<br>Forecast: %{y:,.0f}<extra></extra>'
        ))

        # Add confidence band (approximation: ±15%)
        fig.add_trace(go.Scatter(
            x=pd.concat([forecast_df['date'], forecast_df['date'][::-1]]),
            y=pd.concat([forecast_df['forecast'] * 1.15, (forecast_df['forecast'] * 0.85)[::-1]]),
            fill='toself',
            fillcolor='rgba(22, 163, 74, 0.1)',
            line=dict(color='rgba(255,255,255,0)'),
            name='Confidence Band',
            showlegend=True,
            hoverinfo='skip'
        ))

    # Build title
    title = 'Trip Demand Forecast'
    if model_info:
        title = f"Trip Demand Forecast - {model_info.get('model_type', '').upper()}"
        if model_info.get('mape'):
            title += f" (MAPE: {model_info['mape']:.1f}%)"

    fig.update_layout(
        title=title,
        xaxis_title='Date',
        yaxis_title='Value',
        hovermode='x unified',
        template='plotly_white',
        height=500,
        margin=dict(l=60, r=20, t=80, b=60),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_model_comparison_chart(df: pd.DataFrame) -> str:
    """Create model comparison chart showing metrics for different models."""
    if df.empty:
        return "<p>No model comparison data available</p>"

    # Get best run per model type
    best_runs = df.sort_values('mape').groupby('model_type').first().reset_index()

    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=('MAE (Lower is Better)', 'RMSE (Lower is Better)', 'MAPE % (Lower is Better)')
    )

    colors = {
        'prophet': '#2563eb',
        'arima': '#16a34a',
        'xgboost': '#f59e0b',
        'lightgbm': '#ef4444',
        'lstm': '#8b5cf6',
        'holtwinters': '#06b6d4',
    }

    bar_colors = [colors.get(m, '#6b7280') for m in best_runs['model_type']]

    # MAE
    fig.add_trace(
        go.Bar(
            x=best_runs['model_type'],
            y=best_runs['mae'],
            marker_color=bar_colors,
            hovertemplate='%{x}<br>MAE: %{y:,.0f}<extra></extra>'
        ),
        row=1, col=1
    )

    # RMSE
    fig.add_trace(
        go.Bar(
            x=best_runs['model_type'],
            y=best_runs['rmse'],
            marker_color=bar_colors,
            hovertemplate='%{x}<br>RMSE: %{y:,.0f}<extra></extra>'
        ),
        row=1, col=2
    )

    # MAPE
    fig.add_trace(
        go.Bar(
            x=best_runs['model_type'],
            y=best_runs['mape'],
            marker_color=bar_colors,
            hovertemplate='%{x}<br>MAPE: %{y:.1f}%<extra></extra>'
        ),
        row=1, col=3
    )

    fig.update_layout(
        title='Model Performance Comparison',
        showlegend=False,
        template='plotly_white',
        height=400,
        margin=dict(l=60, r=20, t=80, b=80)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_forecast_accuracy_chart(validation_df: pd.DataFrame) -> str:
    """Create forecast accuracy scatter plot (actual vs predicted)."""
    if validation_df is None or validation_df.empty:
        return "<p>No validation data available</p>"

    fig = go.Figure()

    # Scatter plot
    fig.add_trace(go.Scatter(
        x=validation_df['y_true'],
        y=validation_df['y_pred'],
        mode='markers',
        name='Predictions',
        marker=dict(color='#2563eb', size=8, opacity=0.6),
        hovertemplate='Actual: %{x:,.0f}<br>Predicted: %{y:,.0f}<extra></extra>'
    ))

    # Perfect prediction line
    min_val = min(validation_df['y_true'].min(), validation_df['y_pred'].min())
    max_val = max(validation_df['y_true'].max(), validation_df['y_pred'].max())

    fig.add_trace(go.Scatter(
        x=[min_val, max_val],
        y=[min_val, max_val],
        mode='lines',
        name='Perfect Prediction',
        line=dict(color='#ef4444', dash='dash', width=2),
    ))

    fig.update_layout(
        title='Forecast Accuracy: Actual vs Predicted',
        xaxis_title='Actual Value',
        yaxis_title='Predicted Value',
        template='plotly_white',
        height=400,
        margin=dict(l=60, r=20, t=60, b=60),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def create_runs_history_chart(runs: list) -> str:
    """Create chart showing ML runs history with metrics."""
    if not runs:
        return "<p>No ML runs available</p>"

    df = pd.DataFrame(runs)
    df = df.sort_values('start_time')

    fig = go.Figure()

    # MAPE over time by model type
    for model_type in df['model_type'].unique():
        model_df = df[df['model_type'] == model_type]
        fig.add_trace(go.Scatter(
            x=model_df['start_time'],
            y=model_df['mape'],
            mode='markers+lines',
            name=model_type,
            marker=dict(size=10),
            hovertemplate=f'{model_type}<br>' + '%{x}<br>MAPE: %{y:.1f}%<extra></extra>'
        ))

    fig.update_layout(
        title='Model Performance Over Time (MAPE)',
        xaxis_title='Run Date',
        yaxis_title='MAPE (%)',
        template='plotly_white',
        height=400,
        margin=dict(l=60, r=20, t=60, b=60),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)

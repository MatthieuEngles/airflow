FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Collect static files
RUN python manage.py collectstatic --noinput

# Expose port
EXPOSE 8050

# Run with gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8050", "--workers", "2", "nyc_taxi_dashboard.wsgi:application"]

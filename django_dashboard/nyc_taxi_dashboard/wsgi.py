"""WSGI config for NYC Taxi Dashboard project."""
import os
from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'nyc_taxi_dashboard.settings')
application = get_wsgi_application()

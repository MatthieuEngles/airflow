FROM apache/airflow:3.1.5

# Install additional requirements
COPY --chmod=644 requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

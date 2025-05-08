FROM apache/airflow:3.0.0-python3.12

COPY requirement.txt .
RUN pip install --no-cache-dir -r requirement.txt
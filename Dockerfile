FROM apache/airflow:3.1.8

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt && \
    pip install --no-cache-dir apache-airflow-providers-postgres --no-deps

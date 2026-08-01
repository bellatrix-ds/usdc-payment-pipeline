FROM apache/airflow:2.10.3

USER root
RUN apt-get update && apt-get install -y gcc g++ && rm -rf /var/lib/apt/lists/*

USER airflow
# Copy only the deps needed by DAG operators (excludes apache-airflow itself)
COPY requirements.txt /tmp/requirements.txt
RUN grep -v "^apache-airflow" /tmp/requirements.txt | grep -v "^#" | grep -v "^$" \
    | pip install --no-cache-dir -r /dev/stdin

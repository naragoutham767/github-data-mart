FROM apache/airflow:2.9.0-python3.10

# Switch to root user to install system dependencies
USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-11-jre-headless \
        curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

# Create necessary directories
RUN mkdir -p /opt/airflow/landing /opt/airflow/bronze /opt/airflow/silver

# Copy project files
COPY . /opt/airflow/

# Set proper permissions
RUN chown -R airflow:root /opt/airflow

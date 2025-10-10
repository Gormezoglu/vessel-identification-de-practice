# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables for Spark
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH


# Install dependencies: Java (required by Spark), wget (to download Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    procps \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -C /opt && \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}" && \
    rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Download PostgreSQL JDBC Driver
RUN wget -q "https://jdbc.postgresql.org/download/postgresql-42.3.3.jar" -P "${SPARK_HOME}/jars/"


# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set the working directory
WORKDIR /app

# Copy the application script and data
COPY vessel_identification_spark.py .
COPY case_study_dataset_202509152039.csv .

# Set the entrypoint to run the spark job
ENTRYPOINT ["spark-submit", "vessel_identification_spark.py"]

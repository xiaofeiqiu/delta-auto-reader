# Delta Lake Data Reader Docker Image
# Supports Spark 3.4, Delta Lake 2.2.0, PySpark, Pandas, and Polars

FROM openjdk:11-jdk-slim

# Set environment variables
ENV SPARK_VERSION=3.4.4
ENV HADOOP_VERSION=3
ENV DELTA_VERSION=2.4.0
ENV PYTHON_VERSION=3.9
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3.9 \
    python3.9-dev \
    python3-pip \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create symbolic links for python
RUN ln -sf /usr/bin/python3.9 /usr/bin/python3 && \
    ln -sf /usr/bin/python3.9 /usr/bin/python

# Download and install Spark 3.4.4
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    tar xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" /opt/spark && \
    rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Download Delta Lake JAR for Spark 3.4
RUN wget -q "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_VERSION}/delta-core_2.12-${DELTA_VERSION}.jar" -O /opt/spark/jars/delta-core_2.12-${DELTA_VERSION}.jar && \
    wget -q "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" -O /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar

# Upgrade pip and install Python packages
RUN python3 -m pip install --upgrade pip

# Install compatible versions
RUN pip install \
    pyspark==3.4.4 \
    delta-spark==2.4.0 \
    deltalake \
    pandas==2.1.4 \
    polars==0.20.31 \
    pyarrow==14.0.2 \
    numpy==1.24.4 \
    jupyter==1.0.0 \
    notebook==7.0.8

# Create working directory
WORKDIR /workspace

# Configure Spark to use Delta Lake
RUN echo 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' >> /opt/spark/conf/spark-defaults.conf && \
    echo 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' >> /opt/spark/conf/spark-defaults.conf

# Create entry point script
RUN echo '#!/bin/bash\n\
if [ "$1" = "jupyter" ]; then\n\
    jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token="" --NotebookApp.password=""\n\
elif [ "$1" = "pyspark" ]; then\n\
    pyspark\n\
elif [ "$1" = "spark-shell" ]; then\n\
    spark-shell\n\
elif [ "$1" = "bash" ]; then\n\
    /bin/bash\n\
else\n\
    exec "$@"\n\
fi' > /usr/local/bin/entrypoint.sh && \
    chmod +x /usr/local/bin/entrypoint.sh

# Expose Jupyter port
EXPOSE 8888 4040

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Default command
CMD ["jupyter"]
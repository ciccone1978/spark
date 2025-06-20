# Use official OpenJDK base image with Python
FROM openjdk:11-jdk-slim

# Set environment variables
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS="lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''"

# AWS SDK and Hadoop versions for compatibility
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV DELTA_VERSION=2.4.0

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create symbolic link for python
RUN ln -s /usr/bin/python3 /usr/bin/python

# Download and install Spark
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" $SPARK_HOME \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Download AWS SDK and Hadoop AWS JARs for S3 support
RUN mkdir -p $SPARK_HOME/jars/aws \
    && cd $SPARK_HOME/jars/aws \
    && wget -q "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && wget -q "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    && wget -q "https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar" \
    && cp *.jar $SPARK_HOME/jars/

# Optional: Download Delta Lake JARs for advanced analytics
RUN cd $SPARK_HOME/jars \
    && wget -q "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_VERSION}/delta-core_2.12-${DELTA_VERSION}.jar" \
    && wget -q "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"

# Install Python packages
RUN pip3 install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    jupyterlab \
    jupyter \
    pandas \
    numpy \
    matplotlib \
    seaborn \
    scikit-learn \
    plotly \
    boto3 \
    botocore \
    s3fs \
    minio \
    requests \
    sqlalchemy \
    psycopg2-binary \
    pylint \
    black \
    ipykernel \
    pytest \
    delta-spark

# Create Spark configuration directory and set S3 configurations
RUN mkdir -p $SPARK_HOME/conf

# Create spark-defaults.conf with S3 configurations
RUN echo "# Spark S3 Configuration for MinIO/AWS S3" > $SPARK_HOME/conf/spark-defaults.conf \
    && echo "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" >> $SPARK_HOME/conf/spark-defaults.conf \
    && echo "spark.hadoop.fs.s3a.path.style.access=true" >> $SPARK_HOME/conf/spark-defaults.conf \
    && echo "spark.hadoop.fs.s3a.connection.ssl.enabled=false" >> $SPARK_HOME/conf/spark-defaults.conf \
    && echo "spark.hadoop.fs.s3a.attempts.maximum=3" >> $SPARK_HOME/conf/spark-defaults.conf \
    && echo "spark.hadoop.fs.s3a.connection.establish.timeout=5000" >> $SPARK_HOME/conf/spark-defaults.conf \
    && echo "spark.hadoop.fs.s3a.connection.timeout=10000" >> $SPARK_HOME/conf/spark-defaults.conf \
    && echo "spark.sql.adaptive.enabled=true" >> $SPARK_HOME/conf/spark-defaults.conf \
    && echo "spark.sql.adaptive.coalescePartitions.enabled=true" >> $SPARK_HOME/conf/spark-defaults.conf \
    && echo "spark.serializer=org.apache.spark.serializer.KryoSerializer" >> $SPARK_HOME/conf/spark-defaults.conf

# Create log4j configuration to reduce verbose logging
RUN echo "# Log4j Configuration" > $SPARK_HOME/conf/log4j2.properties \
    && echo "rootLogger.level=WARN" >> $SPARK_HOME/conf/log4j2.properties \
    && echo "rootLogger.appenderRef.console.ref=console" >> $SPARK_HOME/conf/log4j2.properties \
    && echo "appender.console.type=Console" >> $SPARK_HOME/conf/log4j2.properties \
    && echo "appender.console.name=console" >> $SPARK_HOME/conf/log4j2.properties \
    && echo "appender.console.layout.type=PatternLayout" >> $SPARK_HOME/conf/log4j2.properties \
    && echo "appender.console.layout.pattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n" >> $SPARK_HOME/conf/log4j2.properties \
    && echo "logger.spark.name=org.apache.spark" >> $SPARK_HOME/conf/log4j2.properties \
    && echo "logger.spark.level=WARN" >> $SPARK_HOME/conf/log4j2.properties

# Create workspace directory
WORKDIR /workspace

# Copy example configuration files
RUN mkdir -p /workspace/config

# Create example MinIO configuration file
RUN echo "# MinIO Configuration Example" > /workspace/config/minio-config.py \
    && echo "MINIO_ENDPOINT = 'localhost:9000'" >> /workspace/config/minio-config.py \
    && echo "MINIO_ACCESS_KEY = 'minioadmin'" >> /workspace/config/minio-config.py \
    && echo "MINIO_SECRET_KEY = 'minioadmin'" >> /workspace/config/minio-config.py \
    && echo "MINIO_SECURE = False" >> /workspace/config/minio-config.py \
    && echo "BUCKET_NAME = 'taxi-data'" >> /workspace/config/minio-config.py

# Create example Spark session configuration
RUN echo "from pyspark.sql import SparkSession" > /workspace/config/spark-session-example.py \
    && echo "" >> /workspace/config/spark-session-example.py \
    && echo "def create_spark_session_with_s3():" >> /workspace/config/spark-session-example.py \
    && echo "    spark = SparkSession.builder \\" >> /workspace/config/spark-session-example.py \
    && echo "        .appName('MinIOSparkApp') \\" >> /workspace/config/spark-session-example.py \
    && echo "        .config('spark.hadoop.fs.s3a.endpoint', 'http://localhost:9000') \\" >> /workspace/config/spark-session-example.py \
    && echo "        .config('spark.hadoop.fs.s3a.access.key', 'minioadmin') \\" >> /workspace/config/spark-session-example.py \
    && echo "        .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin') \\" >> /workspace/config/spark-session-example.py \
    && echo "        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262') \\" >> /workspace/config/spark-session-example.py \
    && echo "        .getOrCreate()" >> /workspace/config/spark-session-example.py \
    && echo "    return spark" >> /workspace/config/spark-session-example.py

# Create test script to verify S3 connectivity
RUN echo "#!/usr/bin/env python3" > /workspace/test-s3-connection.py \
    && echo "from pyspark.sql import SparkSession" >> /workspace/test-s3-connection.py \
    && echo "import sys" >> /workspace/test-s3-connection.py \
    && echo "" >> /workspace/test-s3-connection.py \
    && echo "def test_s3_connection():" >> /workspace/test-s3-connection.py \
    && echo "    try:" >> /workspace/test-s3-connection.py \
    && echo "        spark = SparkSession.builder \\" >> /workspace/test-s3-connection.py \
    && echo "            .appName('S3ConnectionTest') \\" >> /workspace/test-s3-connection.py \
    && echo "            .config('spark.hadoop.fs.s3a.endpoint', 'http://localhost:9000') \\" >> /workspace/test-s3-connection.py \
    && echo "            .config('spark.hadoop.fs.s3a.access.key', 'minioadmin') \\" >> /workspace/test-s3-connection.py \
    && echo "            .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin') \\" >> /workspace/test-s3-connection.py \
    && echo "            .config('spark.hadoop.fs.s3a.path.style.access', 'true') \\" >> /workspace/test-s3-connection.py \
    && echo "            .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false') \\" >> /workspace/test-s3-connection.py \
    && echo "            .getOrCreate()" >> /workspace/test-s3-connection.py \
    && echo "        " >> /workspace/test-s3-connection.py \
    && echo "        # Test basic functionality" >> /workspace/test-s3-connection.py \
    && echo "        print('Spark session created successfully!')" >> /workspace/test-s3-connection.py \
    && echo "        print(f'Spark version: {spark.version}')" >> /workspace/test-s3-connection.py \
    && echo "        " >> /workspace/test-s3-connection.py \
    && echo "        # Try to list buckets (this will fail if MinIO is not running)" >> /workspace/test-s3-connection.py \
    && echo "        sc = spark.sparkContext" >> /workspace/test-s3-connection.py \
    && echo "        hadoop_conf = sc._jsc.hadoopConfiguration()" >> /workspace/test-s3-connection.py \
    && echo "        print('Hadoop configuration loaded successfully!')" >> /workspace/test-s3-connection.py \
    && echo "        " >> /workspace/test-s3-connection.py \
    && echo "        spark.stop()" >> /workspace/test-s3-connection.py \
    && echo "        return True" >> /workspace/test-s3-connection.py \
    && echo "    except Exception as e:" >> /workspace/test-s3-connection.py \
    && echo "        print(f'Error: {e}')" >> /workspace/test-s3-connection.py \
    && echo "        return False" >> /workspace/test-s3-connection.py \
    && echo "" >> /workspace/test-s3-connection.py \
    && echo "if __name__ == '__main__':" >> /workspace/test-s3-connection.py \
    && echo "    success = test_s3_connection()" >> /workspace/test-s3-connection.py \
    && echo "    sys.exit(0 if success else 1)" >> /workspace/test-s3-connection.py \
    && chmod +x /workspace/test-s3-connection.py

# Expose ports
EXPOSE 8888 4040 7077 8080 9000 9001

# Create startup script
RUN echo '#!/bin/bash' > /start.sh \
    && echo '' >> /start.sh \
    && echo 'echo "=== Spark with S3/MinIO Support ==="' >> /start.sh \
    && echo 'echo "Available JAR files:"' >> /start.sh \
    && echo 'ls -la $SPARK_HOME/jars/hadoop-aws* $SPARK_HOME/jars/aws-java-sdk* 2>/dev/null || echo "AWS JARs not found in jars directory"' >> /start.sh \
    && echo 'echo ""' >> /start.sh \
    && echo 'echo "Starting Spark cluster..."' >> /start.sh \
    && echo '$SPARK_HOME/sbin/start-master.sh' >> /start.sh \
    && echo '$SPARK_HOME/sbin/start-worker.sh spark://$(hostname):7077' >> /start.sh \
    && echo 'echo ""' >> /start.sh \
    && echo 'echo "=== Service URLs ==="' >> /start.sh \
    && echo 'echo "Spark Master UI: http://localhost:8080"' >> /start.sh \
    && echo 'echo "Jupyter Lab: http://localhost:8888"' >> /start.sh \
    && echo 'echo "Spark Application UI: http://localhost:4040 (when running jobs)"' >> /start.sh \
    && echo 'echo ""' >> /start.sh \
    && echo 'echo "=== Configuration Files ==="' >> /start.sh \
    && echo 'echo "MinIO config example: /workspace/config/minio-config.py"' >> /start.sh \
    && echo 'echo "Spark session example: /workspace/config/spark-session-example.py"' >> /start.sh \
    && echo 'echo "S3 connectivity test: /workspace/test-s3-connection.py"' >> /start.sh \
    && echo 'echo ""' >> /start.sh \
    && echo 'echo "=== Usage Tips ==="' >> /start.sh \
    && echo 'echo "1. Start MinIO server first (if not running externally)"' >> /start.sh \
    && echo 'echo "2. Test S3 connection: python /workspace/test-s3-connection.py"' >> /start.sh \
    && echo 'echo "3. Use the example configurations in /workspace/config/"' >> /start.sh \
    && echo 'echo ""' >> /start.sh \
    && echo 'exec jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token="" --NotebookApp.password=""' >> /start.sh \
    && chmod +x /start.sh

# Set the default command
CMD ["/start.sh"]
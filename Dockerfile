FROM openjdk:11-jre-slim

# Install required packages
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install Spark
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" $SPARK_HOME \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Add Spark to PATH
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Set working directory
WORKDIR /app

# Copy application files
COPY target/scala-2.12/spark-size-calculator-assembly-*.jar /app/spark-size-calculator.jar
COPY scripts/ /app/scripts/
COPY src/main/resources/ /app/resources/

# Create logs directory
RUN mkdir -p /app/logs

# Set entrypoint
ENTRYPOINT ["spark-submit", "--class", "com.boomerang.spark.SizeCalculatorJob", "/app/spark-size-calculator.jar"]
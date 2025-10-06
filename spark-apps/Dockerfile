# Sử dụng Spark base image
FROM apache/spark:3.5.0

# Switch to root user để cài đặt packages
USER root

# Cài đặt Python dependencies
COPY requirements.txt /opt/spark-apps/
RUN pip install --no-cache-dir -r /opt/spark-apps/requirements.txt

# Tạo thư mục cho applications
RUN mkdir -p /opt/spark-apps
RUN chmod -R 755 /opt/spark-apps

# Copy spark applications
COPY simple_spark_engine.py /opt/spark-apps/

# Switch về spark user
USER spark

# Set working directory
WORKDIR /opt/spark-apps
FROM bitnami/spark:3.5.1

USER root

# Copy JAR into Spark jars directory
COPY SparkS3CSVFilter-assembly-0.1.jar /opt/spark/jars/

# Environment variables for AWS
ENV AWS_REGION=us-east-1

# Default command (not used in k8s)
CMD ["/opt/spark/bin/spark-submit", \
     "--class", "com.example.Main", \
     "--master", "local[*]", \
     "/opt/spark/jars/SparkS3CSVFilter-assembly-0.1.jar", \
     "s3a://demo-test-poc-scala/input/students.csv", \
     "s3a://demo-test-poc-scala/output"]

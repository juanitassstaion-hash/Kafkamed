FROM apache/spark:3.5.0
USER root
RUN pip install pymongo==4.8.0 numpy
COPY consumer_spark.py /opt/spark/work-dir/
WORKDIR /opt/spark/work-dir

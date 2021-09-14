from pyspark.sql import SparkSession
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == "__main__":

    session = SparkSession.builder.appName("taxi data analysis").master("local[*]").getOrCreate()
    dataFrameReader = session.read

    green2020 = dataFrameReader.option("header", "true").option("inferSchema", value = True).csv("data/green_tripdata_2020-05.csv")
    green2019 = dataFrameReader.option("header", "true").option("inferSchema", value = True).csv("data/green_tripdata_2019-05.csv")
    yellow2020 = dataFrameReader.option("header", "true").option("inferSchema", value = True).csv("data/yellow_tripdata_2020-05.csv")
    yellow2019 = dataFrameReader.option("header", "true").option("inferSchema", value = True).csv("data/yellow_tripdata_2019-05.csv")

    green2020.printSchema()
    green2019.printSchema()
    yellow2020.printSchema()
    yellow2019.printSchema()

    green2020_pcpt = green2020.corr("passenger_count", "payment_type")
    green2019_pcpt = green2019.corr("passenger_count", "payment_type")
    yellow2020_pcpt = yellow2020.corr("passenger_count", "payment_type")
    yellow2019_pcpt = yellow2019.corr("passenger_count", "payment_type")

    #all_joined = green_tripdata_2020_05.join(green_tripdata_2019_05, "outer")

    
    session.stop()
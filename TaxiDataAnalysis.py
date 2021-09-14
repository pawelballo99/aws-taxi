from pyspark.sql import SparkSession
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == "__main__":

    session = SparkSession.builder.appName("taxi data analysis").master("local[*]").getOrCreate()

    dataFrameReader = session.read

    green_tripdata_2020_05 = dataFrameReader.option("header", "true").csv("data/green_tripdata_2020-05.csv")
    green_tripdata_2019_05 = dataFrameReader.option("header", "true").csv("data/green_tripdata_2019-05.csv")
    yellow_tripdata_2020_05 = dataFrameReader.option("header", "true").csv("data/yellow_tripdata_2020-05.csv")
    yellow_tripdata_2019_05 = dataFrameReader.option("header", "true").csv("data/yellow_tripdata_2019-05.csv")
    
    print("green2020")
    green_tripdata_2020_05.select("lpep_pickup_datetime").show()

    print("green2019")
    green_tripdata_2019_05.select("lpep_pickup_datetime").show()

    #all_joined = green_tripdata_2020_05.join(green_tripdata_2019_05, "outer")

    session.stop()
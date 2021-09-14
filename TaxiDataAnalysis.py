from pyspark.sql import SparkSession
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == "__main__":

    #session = SparkSession.builder.appName("taxi data analysis").master("local[*]").getOrCreate()
    session = SparkSession.builder.appName("taxi data analysis").getOrCreate()
    dataFrameReader = session.read

    green2020 = dataFrameReader.option("header", "true").option("inferSchema", value = True).csv("data/green_tripdata_2020-05.csv").select("passenger_count", "payment_type")
    green2019 = dataFrameReader.option("header", "true").option("inferSchema", value = True).csv("data/green_tripdata_2019-05.csv").select("passenger_count", "payment_type")
    yellow2020 = dataFrameReader.option("header", "true").option("inferSchema", value = True).csv("data/yellow_tripdata_2020-05.csv").select("passenger_count", "payment_type")
    yellow2019 = dataFrameReader.option("header", "true").option("inferSchema", value = True).csv("data/yellow_tripdata_2019-05.csv").select("passenger_count", "payment_type")


    #grouped by year, color and all joined
    pcpt_2020 = green2020.join(yellow2020, on=["passenger_count", "payment_type"], how='left_outer')
    pcpt_2019 = green2019.join(yellow2019, on=["passenger_count", "payment_type"], how='left_outer')

    yellow_pcpt = yellow2020.join(yellow2019, on=["passenger_count", "payment_type"], how='left_outer')
    green_pcpt = green2020.join(green2019, on=["passenger_count", "payment_type"], how='left_outer')

    all_pcpt = yellow_pcpt.join(green_pcpt,on=["passenger_count", "payment_type"], how='left_outer')

    #calculate correaltion for each group
    green2020_pcpt_corr = green2020.corr("passenger_count", "payment_type")
    green2019_pcpt_corr = green2019.corr("passenger_count", "payment_type")
    yellow2020_pcpt_corr = yellow2020.corr("passenger_count", "payment_type")
    yellow2019_pcpt_corr = yellow2019.corr("passenger_count", "payment_type")

    pcpt_2020_corr = pcpt_2020.corr("passenger_count", "payment_type")
    pcpt_2019_corr = pcpt_2019.corr("passenger_count", "payment_type")

    yellow_pcpt_corr = yellow_pcpt.corr("passenger_count", "payment_type")
    green_pcpt_corr = green_pcpt.corr("passenger_count", "payment_type")

    all_pcpt_corr = all_pcpt.corr("passenger_count", "payment_type")

    #printings
    print("Correlation of green taxi in may of 2020:" + green2020_pcpt_corr)
    print("Correlation of green taxi in may of 2019:" + green2019_pcpt_corr)
    print("Correlation of yellow taxi in may of 2020:" + yellow2020_pcpt_corr)
    print("Correlation of yellow taxi in may of 2019:" + yellow2019_pcpt_corr)

    print("Correlation of all taxi in may of 2020:" + pcpt_2020_corr)
    print("Correlation of all taxi in may of 2019:" + pcpt_2019_corr)

    print("Correlation of all green taxi in may of 2020 and 2019:" + green_pcpt_corr)
    print("Correlation of all yellow taxi in may of 2020 and 2019:" + yellow_pcpt_corr)

    print("Correlation of all taxi in may of 2020 and 2019:" + all_pcpt_corr)

    

    session.stop()
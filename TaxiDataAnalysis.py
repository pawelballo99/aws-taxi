from pyspark.sql import SparkSession

if __name__=="__main__":
    session = SparkSession.builder.appName("TaxiDataAnalysis").getOrCreate()
    sc = session.sparkContext
    sc.setLogLevel("ERROR")
    dataFrameReader = session.read
    responses = dataFrameReader \
        .option("header", "true") \
        .option("inferSchema", value = True) \
        .csv("s3://taxi-data-new-york/green_tripdata_2019_05.csv")
    print("Initial commit")
    print("== Schema==")
    responses.printSchema()

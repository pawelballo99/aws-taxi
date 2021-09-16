from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import *
from matplotlib import *
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import boto3
import io
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
rcParams['agg.path.chunksize'] = 10000000

localSpark = False
LOCAL_PATH = 'data/'
S3_PATH = 's3://taxi-data-new-york/'

session = SparkSession.builder.appName("TaxiDataAnalysis").master("local[*]").getOrCreate() if localSpark else SparkSession.builder.appName("TaxiDataAnalysis").getOrCreate() 
sc = session.sparkContext
sc.setLogLevel("ERROR")
dataFrameReader = session.read



def loadData(csvName):
    path = (LOCAL_PATH if localSpark else S3_PATH)+csvName
    dataFrame = dataFrameReader.option("header", "true").option("inferSchema", value = True).csv(path).select("passenger_count", "payment_type")
    return dataFrame.filter((dataFrame.payment_type == 1) | (dataFrame.payment_type == 2) | (dataFrame.payment_type == 3) | (dataFrame.payment_type == 4) | (dataFrame.payment_type == 5) | (dataFrame.payment_type == 6))

def createPlot(dataFrame, name):
    corr = dataFrame.corr("passenger_count", "payment_type")
    print("Correlation "+ name +": "+ str(corr))
    x = dataFrame.toPandas()["passenger_count"].values.tolist()
    y = dataFrame.toPandas()["payment_type"].values.tolist()
    fig = plt.figure(figsize = [12, 5])
    plt.subplot(1, 2, 1)
    sns.regplot(x = x, y = y, fit_reg = False,
            x_jitter = 0.1, y_jitter = 0.1, scatter_kws = {'alpha' : 1/3})
    plt.ylabel("Passenger Count")
    plt.xlabel("Payment Type")
    plt.subplot(1, 2, 2)
    bins_x = np.arange(-0.5, 9.5, 1)
    bins_y = np.arange(0.5, 6.5, 1)
    plt.hist2d( x =x, y = y,
            bins = [bins_x, bins_y])
    plt.colorbar()
    plt.ylabel("Passenger Count")
    plt.xlabel("Payment Type")

    if (not localSpark):
        img_data = io.BytesIO()
        plt.savefig(img_data, format='png')
        img_data.seek(0)
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('taxi-data-new-york')
        bucket.put_object(Body=img_data, ContentType='png', Key=name)
    else:
        plt.savefig(name, format='png')


if __name__ == "__main__":


    green2020= loadData('green_tripdata_2019-05.csv')
    green2019= loadData('green_tripdata_2020-05.csv')
    yellow2020= loadData('yellow_tripdata_2019-05.csv')
    yellow2019= loadData('yellow_tripdata_2020-05.csv')
    

    #grouped by year, color and all joined
    pcpt_2020 = green2020.union(yellow2020)
    pcpt_2019 = green2019.union(yellow2019)
    yellow_pcpt = yellow2020.union(yellow2019)
    green_pcpt = green2020.union(green2019)
    all_pcpt = yellow_pcpt.union(green_pcpt)

    #creating and saving plots
    createPlot(green2019, "green2019")
    createPlot(green2020, "green2020")
    createPlot(yellow2020, "yellow2020")
    createPlot(yellow2019, "yellow2019")
    createPlot(pcpt_2020, "pcpt_2020")
    createPlot(pcpt_2019, "pcpt_2019")
    createPlot(yellow_pcpt, "yellow_pcpt")
    createPlot(green_pcpt, "green_pcpt")
    createPlot(all_pcpt, "all_pcpt")

    session.stop()
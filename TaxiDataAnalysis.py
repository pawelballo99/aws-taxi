from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import *
from matplotlib import *
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import scipy.stats
import boto3
import io
import time
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
rcParams['agg.path.chunksize'] = 10000000

localSpark = False
LOCAL_PATH = 'data/'
S3_PATH = 's3://taxi-data-new-york/'

start = time.time()
session = SparkSession.builder.appName("TaxiDataAnalysis").master("local[*]").getOrCreate() if localSpark else SparkSession.builder.appName("TaxiDataAnalysis").getOrCreate() 
sc = session.sparkContext
sc.setLogLevel("ERROR")
dataFrameReader = session.read



def loadData(csvName):
    path = (LOCAL_PATH if localSpark else S3_PATH)+csvName
    dataFrame = dataFrameReader.option("header", "true").option("inferSchema", value = True).csv(path).select("passenger_count", "payment_type")
    return dataFrame.filter((dataFrame.payment_type == 1) | (dataFrame.payment_type == 2) | (dataFrame.payment_type == 3) | (dataFrame.payment_type == 4) | (dataFrame.payment_type == 5) | (dataFrame.payment_type == 6))

def createPlot(dataFrame, name):
    
    x = dataFrame.toPandas()["passenger_count"].values.tolist()
    y = dataFrame.toPandas()["payment_type"].values.tolist()
    corr = scipy.stats.spearmanr(x, y).correlation
    plt.figure(figsize = [12, 5])
    plt.suptitle(name+"\nCorrelation: "+ str(round(corr,4)), fontsize=14)
    plt.subplot(1, 2, 1)
    sns.regplot(x = x, y = y, fit_reg = False,
            x_jitter = 0.1, y_jitter = 0.1, scatter_kws = {'alpha' : 1/3})
    plt.xlabel("Passenger Count")
    plt.ylabel("Payment Type")
    plt.subplot(1, 2, 2)
    plt.hist2d( x =x, y = y,
            bins = [np.arange(-0.5, 9.5 + 1, 1), np.arange(0.5, 6.5 + 1, 1)])
    plt.colorbar()
    plt.xlabel("Passenger Count")
    plt.ylabel("Payment Type")

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
    createPlot(green2019, "Green taxis in May 2019")
    createPlot(green2020, "Green taxis in May 2020")
    createPlot(yellow2020, "Yellow taxis in May 2020")
    createPlot(yellow2019, "Yellow taxis in May 2019")
    createPlot(pcpt_2020, "All taxis in May 2020")
    createPlot(pcpt_2019, "All taxis in May 2019")
    createPlot(yellow_pcpt, "Yellow taxis in May 2019 and 2020")
    createPlot(green_pcpt, "Green taxis in May 2019 and 2020")
    createPlot(all_pcpt, "All taxis in May 2019 and 2020")

    end = time.time()
    print("Time: "+ str(end - start))
    session.stop()
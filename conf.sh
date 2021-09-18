#!/bin/bash
sudo pip3 install --upgrade pip
pip3 install seaborn
pip3 install pandas
pip3 install matplotlib
pip3 install scipy
pip3 install boto3
aws s3 cp s3://taxi-data-new-york/TaxiDataAnalysis.py .
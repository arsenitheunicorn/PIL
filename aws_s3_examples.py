import boto3 # This is the main thing
import pandas as pd
import glob
import io
import logging
import os
from queue import Queue
from threading import Thread
from time import time


# 1!!! the example for getting files from two folders, 
# 	merging them by pandas and
# 	saving pandas csv files into S3 by pandas

dates_list = ["2020-10-01","2020-10-02","2020-10-03"]

for requested_date in dates_list:
    bucket_name = '{BUCKETNAMEHERE}'
    prefix_list = ["{folder_one}","{folder_n}"]
    cohorts_folder = "./data_202010/cohorts/" #local
    daily_folder = "./data_202010/daily/" #local

    # You can set s3 credentials by one of the following methods:
    # a) Use env params — you could google this
    # b) Use AWS service SSM parameter store (the next time talk)
    # c) Use local profiles which work through AWS CLI 
    # theese profiles usually are stored in {home}/.aws/credentials file.
    # This file may contain several profiles with parameters such as:
    # profile name, aws_key, aws_secret_key - read more in documentation 
    # AWS CLI config
    # 
    # d) Bum style: hardcode key/secret_key or use json which is 
    # added to .gitignore
    # DO NOT STORE YOUR CREDENTIALS IN YOUR CODE OR IN OPEN JSONS 
    # Here we use c - local profiles. This could work not well for 
    # a web app because for that aws profiles should be set on the 

    session = boto3.Session(profile_name="voodoo") 
    # client = session.client("s3")
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)
    client = session.client('s3')


    # This part is for getting all cohorts for the set "requested_date"
    bucket_files_list = []
    df_list=[]

    first_list = bucket.objects.filter(Prefix=(prefix_list[0]+requested_date+"_"))
    for obj in first_list:
        bucket_files_list.append(obj.key)
        df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()), index_col=None, header=0)
        df_list.append(df)
    df_cohorts = pd.concat(df_list, axis=0, ignore_index=True)
    print(len(bucket_files_list))
    print(bucket_files_list[0])
    df_cohorts["date"] = requested_date
    df_cohorts.to_csv(cohorts_folder + "df_cohorts_" + requested_date + ".csv")

    # This part is for getting all daily_metrics for the set "requested_date"
    bucket_files_list = []
    df_list=[]
    first_list = bucket.objects.filter(Prefix=(prefix_list[1]+requested_date+"_"))
    for obj in first_list:
        bucket_files_list.append(obj.key)
        df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()), index_col=None, header=0)
        df_list.append(df)
    df_daily = pd.concat(df_list, axis=0, ignore_index=True)
    print(len(bucket_files_list))
    print(bucket_files_list[0])
    df_daily.to_csv(daily_folder + "df_daily_" + requested_date + ".csv")


# 2!!! The other method — upload_file
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_file


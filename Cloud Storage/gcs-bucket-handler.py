# Umarah Qaseem

from google.cloud import storage
import google.cloud.storage
import json
import os
import sys
import pandas as pd
import io
from io import BytesIO
import numpy as np

#Constants
#OLD_JSON_FILE = 'service-account-key.json'
JSON_FILE = 'keyfile.json'
FILE = '2020_Olympics_Dataset.csv'
PROCESSED_FILE = 'Processed_Olymics_Dataset_class.csv'
BUCKET_NAME = 'umqbucket'


class BucketHandler:

    def __init__(self, service_key, bucket_name):
        """
            Constructor. Sets the variables of class
            sets storage client object and GCS Bucket object

            :param service key: name of authentication service key file (string)
            :param bucket name: name of bucket to work on (string)
            :return: nothing
        """
        self.service_key = service_key
        self.bucket_name = bucket_name
        self.storage_client = self.set_path(self.service_key)
        self.gcs_bucket = self.storage_client.get_bucket(self.bucket_name)
        return

    def set_path(self, json_file):
        """
            Returns the GCS Client Object through service key.

            :param json_file: name of authentication service key file (string)
            :return: GCS client object
        """
        path = os.path.join(os.getcwd() , json_file)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path
        storage_client = storage.Client(path)
        #print(storage_client)
        return storage_client

    def dir_bucket(self, bucket):
        """
            Displays all files in a bucket.

            :param bucket: name of bucket (string)
            :return: nothing
        """
        filename = [filename.name for filename in list(bucket.list_blobs(prefix='')) ]
        print("Files present on GCS are ", filename)

    def read_file_from_bucket(self, file):
        """
            Reads file from bucket and stores on local disk then loads in dataframe.

            :param file: file name to read (string)
            :param bucket: name of bucket (string)
            :return: a dataframe in which local file is loaded.
        """
        blop = self.gcs_bucket.blob(blob_name = file).download_as_string()
        with open ('local.csv', "wb") as f:
            f.write(blop)
        df = pd.read_csv("local.csv", encoding='latin-1')
        return df
        #workaround

    def process_file(self, data):
        """
            Process file, any condition can be written on it.
            All those records are kept which are for female with any medal.

            :param data: dataframe containing dataset
            :return: a dataframe with processed data.
        """
        female_gender_mask = data["Gender"] == 'Female'
        medal_notna_mask = data['Medal'].notna()
        mask = [female_gender_mask, medal_notna_mask]

        for m in mask:
            data = data[m]
        #print(data)
        return data

    def upload_data_to_bucket(self, data,filename):
        """
            Upload data from file to bucket

            :param data: dataframe containing data to upload (string)
            :param file: name you want to give to the file uploaded on bucket (string)
            :param bucket: name of bucket where data is to be uploaded (string)
            :return: returns nothing
        """
        data.to_csv(filename, index=False)
        completeLocalPath = os.path.join(os.getcwd(), filename)
        blob = self.gcs_bucket.blob(filename)
        blob.upload_from_filename(completeLocalPath)
        #workaround

def main():
    """
        main program
        get GCS client object,
        get the bucket, display the bucket,
        read data from bucket, process it and upload back to bucket.

        :return: nothing.
    """
    print("Started") #Checking logging in python

    handler = BucketHandler(JSON_FILE,BUCKET_NAME)
    handler.dir_bucket(handler.gcs_bucket)
    df = handler.read_file_from_bucket(FILE)
    # print("Unique Values: ", df["Medal"].unique())
    result_records = handler.process_file(df)
    handler.upload_data_to_bucket(result_records,PROCESSED_FILE)

if __name__ == "__main__":
    main()

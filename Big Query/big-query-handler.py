# UmQ
from google.cloud import bigquery
from google.cloud import storage
import google.cloud.storage
import json
import os
import sys
import pandas as pd
import pandas_gbq
import csv
import io
from io import BytesIO
import numpy as np

#Constants
PROJECT_NAME = "Your-project-name"
DATASET_NAME = "Olympics"
TABLE_NAME = "myTable"
TABLE_ID = "{}.{}.{}".format(PROJECT_NAME, DATASET_NAME, TABLE_NAME)
QUERY = (
"SELECT name FROM TABLE_ID "
"WHERE Medal = 'Gold' "
"LIMIT 100")

JSON_FILE = 'keyfile.json'
FILE_TO_UPLOAD = 'Processed_Olymics_Dataset.csv'
#JSON_FILE_TO_UPLOAD = "json_sample.json"

class BigqueryTable:
    def __init__(self, service_key):
        """
            Constructor. Sets the variables of class
            sets storage client object and GCS Bucket object

            :param service key: name of authentication service key file (string)
            :return: nothing
        """
        self.big_query_client = self.set_path(service_key)
        #Other initialization steps which you want here.
        return

    def set_path(self, json_file):
        """
            Returns the GCS Client Object through service key.

            :param json_file: name of authentication service key file (string)
            :return: Big Query client object
        """
        path = os.path.join(os.getcwd() , json_file)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path
        client = bigquery.Client()
        return client


    def create_table(self, table_id):
        """
            Creates empty Table using a schema given below in Big Query

            :param json_file: name of authentication service key file (string)
            :return: nothing
        """
        schema = [
            bigquery.SchemaField("first_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("last_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("dob", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("age", "INTEGER"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

    def upload_csv_to_newtable(self, table_id, file):
        """
            Uploads a csv file to Big Query by creating/appending/replacing a table

            :param table_id: name of table which will be created (string)
            :return: nothing
        """
        df=pd.read_csv(file)
        df.to_gbq(table_id, if_exists='replace')
        dataset_ref = self.big_query_client.dataset(DATASET_NAME)
        table_ref = dataset_ref.table(table_id)
        self.big_query_client.load_table_from_dataframe(df, table_ref).result()
        
    def insert_rows(self, rows, table):
        """
            insert rows in the table on big query
            :param rows: is a list of dictionaries. Dictionary keys map to column names in bigquery
            :param table: name of table which will be created (string)
            :return: nothing
        """
        try:
            errors = self.big_query_client.insert_rows_json(table, rows)
            if errors:
                print("Error: ", errors)

        except Exception as error:
            print(error)

    def query(self,query):
        """
            Runs a query that is recieve as a parameter

            :param query: The query to be run
            :return: nothing
        """
        query_job = client.query(query)
        rows = query_job.result()
        for row in rows:
            print(row.name)

    def load_json_to_table(self, file_uri, table_id):
        """uri is a json file path present on gcs"""
        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )

            load_job = self.big_query_client.load_table_from_uri(
                file_uri, table_id, job_config=job_config
            )  # Make an API request.

            load_job.result()  # Waits for the job to complete.

            destination_table = self.big_query_client.get_table(table_id)
            print(f"Loaded {destination_table.num_rows} rows.")
            return True

        except Exception as e:
            print("Exception occurs in load_json_to_table, ",e.args)
            return False

def main():
    print("Started")
    handler.upload_csv_to_newtable(TABLE_ID, FILE_TO_UPLOAD)
    
    #handler = BigqueryTable(JSON_FILE)
    #handler.create_table(TABLE_ID)
    #handler.query(QUERY)
    #df = pd.read_csv(FILE_TO_UPLOAD, encoding='latin-1')
    #rows = df.to_dict('records')
    #handler.insert_rows(rows,TABLE_ID_4)
    #handler.load_json_to_table(JSON_FILE_TO_UPLOAD,TABLE_ID_5)

if __name__ == "__main__":
    main()

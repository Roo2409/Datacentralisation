
from sqlalchemy import create_engine
import pandas as pd
from database_utils import data_connector
import tabula
import requests
import json
import boto3
import botocore
import os
import io
from io import BytesIO

class Dataextractor:
#     def __init__(self) -> None:
#         self.conn = data_connector.read_db_credsdata()

    def read_rds_table(self, table_name, engine):
        credentials = data_connector.read_db_credsdata()
        engine = data_connector.init_db_engine(credentials)
        table_name = data_connector.list_db_tables(engine)
        #print(table_name)
        data = pd.read_sql_table(table_name[0], engine)
        print(data.columns)
        return data
    def retrieve_pdf_data(self,link):
        link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        pdf = tabula.read_pdf(link , pages="all")
        df_pdf = pd.concat(pdf) 
        return df_pdf
    
    def list_number_of_stores(self, endpoint,headers):
        response = requests.get(endpoint, headers = headers)
        headers = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        content = response.text
        # print(content)
        result = json.loads(content)
        print(result)
        store_number = list(result.values())[1] #as index 1 represent no of stores and index 0 respresent statuscode
        return store_number
    
    def retrieve_stores_data(self):
        data=[]
        store_number=data_extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',{ 'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})
        for i in range(0,store_number):
            store_endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}'
            headers = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
            response = requests.get(store_endpoint,headers=headers)
            content = response.text
                    # print(content)
            result = json.loads(content)
            data.append(result)
        data_df= pd.DataFrame(data)
        print(data_df)

    
    def extract_from_s3(self):
        s3_client = boto3.client('s3')
        result = s3_client.get_object(Bucket = 'data-handling-public', Key= 'date_details.json' )
        json_data = result['Body'].read()
        df_json = pd.read_json(BytesIO(json_data))
        return df_json
        
    
        
  

      
            


data_extractor = Dataextractor()
credentials = data_connector.read_db_credsdata()
credentials = data_connector.read_db_credsdata()
engine = data_connector.init_db_engine(credentials)
table_name = data_connector.list_db_tables(engine)
read_table = data_extractor.read_rds_table(table_name[0], engine)
print(read_table)
link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
data_extractor.retrieve_pdf_data(link)
read_data= data_extractor.retrieve_stores_data()
print(read_data)
aws = data_extractor.extract_from_s3()
print(aws)
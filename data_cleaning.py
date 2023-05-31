
from data_extraction import data_extractor
from database_utils import data_connector
import pandas as pd

class Datacleaning:
     def clean_user_data(self):
        credentials = data_connector.read_db_credsdata()
        engine = data_connector.init_db_engine(credentials)
        table_name =data_connector.list_db_tables(engine)
        df = data_extractor.read_rds_table(table_name[1], engine)
        print(df.columns)
        df.info()
        error_list = ['I7G4DMDZOZ', 'NULL',
       'AJ1ENKS3QL', 'XGI7FM0VBJ', 'S0E37H52ON', 'XN9NGL5C0B',
       '50KUU3PQUF', 'EWE3U0DZIV', 'GMRBOMI0O1', 'YOTSVPRBQ7',
       '5EFAFD0JLI', 'PNRMPSYR1J', 'RQRB7RMTAD', '3518UD5CE8',
       '7ZNO5EBALT', 'T4WBZSW0XI']
        df = df[~(df.country.isin(error_list))] 
        df.drop_duplicates(inplace = True)
        df = df[~(df.company== 'NULL')]
        return df
     
     def clean_card_data(self):
         df_pdf = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
         df_pdf.info()
         df_pdf.isna().sum()
         # df_pdf['card_number'].value_counts()
         drop_null = df_pdf[df_pdf['card_number']=='NULL'].index
         df_pdf.drop(drop_null,inplace=True)
         df_pdf['card_number'].value_counts()
         # df_pdf['expiry_date'].value_counts()
         error_list = ['RF1ACW165R','XRPE6C4GS9','5VN8HOLMVE','Q7VGWP7LH9','2ANT8LW3I5','NWS3P2W38H','ACT9K6ECRJ','WDWMN9TU45']
         drop_errorlist = df_pdf[df_pdf['expiry_date']. isin(error_list)].index
         print(drop_errorlist)
         df_pdf.drop(drop_errorlist, inplace=True)
         df_pdf['expiry_date'].value_counts()
         return df_pdf
     
     def called_clean_store_data(self):
         credentials = data_connector.read_db_credsdata()
         engine = data_connector.init_db_engine(credentials)
         table_name =data_connector.list_db_tables(engine)
         store_data= data_extractor.read_rds_table(table_name[0], engine)
         print(store_data.columns)
         replace_cont = store_data.replace(['eeAmerica','eeEurope'],['America', 'Europe'], inplace=True)
         print(replace_cont)
         store_data['lat'].fillna('nonapplicable',inplace=True)
         store_data['latitude'].fillna('nonapplicable',inplace=True)
         store_data.drop(columns = 'lat' , inplace=True)
         store_data['country_code'].value_counts()
         delete_null =store_data[store_data['country_code']=='NULL'].index
         delete_null1= store_data.drop(delete_null,inplace=True)
         print(delete_null1)
         df2 = store_data[ (store_data['country_code'] != 'GB') & (store_data['country_code'] != 'US')& (store_data['country_code'] != 'DE')]
         print(df2)
         drop_rows = store_data.drop(labels = [63,172,231,333,381,414,447],axis=0, inplace = True)
         print(drop_rows)
         return store_data
            
         


     def clean_orders_data(self):
         credentials = data_connector.read_db_credsdata()
         engine = data_connector.init_db_engine(credentials)
         table_name =data_connector.list_db_tables(engine)
         order_df = data_extractor.read_rds_table(table_name[2], engine)
         order_df.drop(columns=['first_name', 'last_name', '1', 'level_0'],inplace=True)
         return order_df
     
    
cleaned_data = Datacleaning()
df = cleaned_data.clean_user_data()
data_connector.upload_to_db(df, 'Dim_Users_details')

df_card_data= cleaned_data.clean_card_data()
data_connector.upload_to_db(df_card_data,'Dim_cards_details')

clean_store = cleaned_data.called_clean_store_data()
data_connector.upload_to_db(clean_store,'Dim_stores_details')

order_table = cleaned_data.clean_orders_data()
data_connector.upload_to_db(order_table,'orders_table')




import yaml
from sqlalchemy import inspect
import pandas as pd
from sqlalchemy import create_engine


class Databaseconnector:
    def read_db_credsdata(self):
        with open("db_creds.yaml") as f:
            credentials = yaml.load(f, Loader=yaml.FullLoader)
            #line 9 indicates how to read a file
            #print(credentials)
            return credentials
            
    def init_db_engine(self, credentials):

        engine = create_engine(f"{credentials['RDS_DATABASE_TYPE']}+{credentials['DBAPI']}://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['DATABASE']}")
        engine.connect()
        return engine
   
    def list_db_tables(self, engine):
        inspector = inspect(engine)
        table_name= inspector.get_table_names()
        print(table_name)
        return table_name
    
    def upload_to_db(self, df , table_name):
        credentials = self.read_db_credsdata()
        conn = create_engine(f"{credentials['Local_DATABASE_TYPE']}+{credentials['Local_DBAPI']}://{credentials['Local_USER']}:{credentials['Local_PASSWORD']}@{credentials['Local_HOST']}:{credentials['Local_PORT']}/{credentials['Local_DATABASE']}")
        conn.connect()
        df.to_sql(name = table_name, con = conn, if_exists = 'replace')
        #return conn

        
    

        


data_connector = Databaseconnector()
credentials = data_connector.read_db_credsdata()
print(credentials)
engine = data_connector.init_db_engine(credentials)
print(engine)
table_name = data_connector.list_db_tables(engine)
print(table_name)


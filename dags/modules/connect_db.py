import sqlalchemy as sa
from dotenv import load_dotenv
import os

def connect_db():
    load_dotenv()


    username = os.getenv('REDSHIFT_USERNAME')
    password = os.getenv('REDSHIFT_PASSWORD')
    host = os.getenv('REDSHIFT_HOST')
    port = os.getenv('REDSHIFT_PORT')
    dbname = os.getenv('REDSHIFT_DBNAME')
    
    try:
        engine = sa.create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}")
        print('Connection successful')
    except Exception as e:
        print(f'Unable to connect to database {dbname} - {e}')

   
    
    return engine
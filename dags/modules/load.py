import pandas as pd
from modules import connect_db
from datetime import datetime
import sqlalchemy as sa
from dotenv import load_dotenv
import os


def load(exec_date, path):

    load_dotenv()


    username = os.getenv('REDSHIFT_USERNAME')
    password = os.getenv('REDSHIFT_PASSWORD')
    host = os.getenv('REDSHIFT_HOST')
    port = os.getenv('REDSHIFT_PORT')
    dbname = os.getenv('REDSHIFT_DBNAME')
    
    try:
        engine = sa.create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}")
    except Exception as e:
        print(f'Unable to connect to database {dbname} - {e}')



    date = datetime.strptime(exec_date, "%Y-%m-%d %H")
    csv_path = (
        f"{path}/raw_data/data.csv"
    )
    data=pd.read_csv(csv_path, sep=",")

    table='holtzy_repos'
    schema='fabriciositto_coderhouse'

    engine=connect_db()
    # #crear tabla
    # creation_query=f"""DROP TABLE IF EXISTS {schema}.{table};

    #                 CREATE TABLE {schema}.{table}(
    #                     id INT,
    #                     name VARCHAR(250),
    #                     owner VARCHAR(250),
    #                     description TEXT,
    #                     fork BOOLEAN,
    #                     created_at TIMESTAMP,
    #                     updated_at TIMESTAMP,
    #                     size INT,
    #                     language VARCHAR(250),
    #                     forks INT,
    #                     open_issues INT,
    #                     watchers INT,
    #                     extracted_timestamp TIMESTAMP,
    #                     comp_id VARCHAR(250) PRIMARY KEY UNIQUE
    #                 );"""
        
    # with engine.connect() as connection:
    #     connection.execute(creation_query)


    try:
        data.to_sql(
                    table,
                    con=engine,
                    schema=schema,
                    if_exists='append',
                    index=False
                )
    except Exception as e:
        print(f'The upload could not be completed - {e}')
    
    engine.dispose()
    
    
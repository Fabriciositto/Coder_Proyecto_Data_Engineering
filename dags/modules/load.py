import pandas as pd
from datetime import datetime
from modules import connect_db
import os


def load(exec_date, path):

    engine=connect_db



    date = datetime.strptime(exec_date, "%Y-%m-%d %H")
    csv_path = (
        f"{path}/raw_data/data.csv"
    )
    data=pd.read_csv(csv_path, sep=",")

    table='holtzy_repos'
    schema='fabriciositto_coderhouse'

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

    if engine is None:
        print("The engine could not be created. Exiting...")
    else:
        print("Engine is created and ready to use.")

    try:
        with engine.connect() as connection:
            data.to_sql(
                table,
                con=connection,  # use connection here
                schema=schema,
                if_exists='append',
                index=False
            )
        print("Upload successful")
    except Exception as e:
        print(f'The upload could not be completed - {e}')
    
    
    
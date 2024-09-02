import pandas as pd
from modules import connect_db


def load(data):

    data=pd.read_csv('../storage_files/transformation.csv')
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
    
    
import pandas as pd
import logging



def load(data,engine,table,schema):

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
        logging.info('Load successful')
    except Exception as e:
        logging.error(f'The upload could not be completed - {e}')
    
    
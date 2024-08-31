from modules import extraction, transformation, connect_db, load
import pandas as pd
import logging
import json

def main():


    #   Configurar el logger
    logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='app.log', 
                    filemode='a')


    
    user_name='holtzy'
    data_json=extraction(user_name)

    # # código auxiliar para guardado y lectura de json con los resultados de la consulta (para testear sin realizar tantas requests)
    # with open('./storage_files/mook.json', 'w') as file:
    #     json.dump(data_json, file)
    # 
    # with open('./storage_files/mook.json', 'r') as file:
    #     data_json = json.load(file)
    
    data=transformation(data_json)
    
    engine=connect_db()


    table=f'{user_name}_repos'
    schema='fabriciositto_coderhouse'

    load(data,engine,table,schema)

    engine.dispose()


if __name__ == "__main__":
    main()
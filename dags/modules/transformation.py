import pandas as pd
import json
from datetime import datetime

def transformation(exec_date, path):
    date = datetime.strptime(exec_date, "%Y-%m-%d %H")
    json_path = (
        f"{path}/raw_data/data.json"
    )
    with open(json_path, 'r') as file:
        data_json = json.load(file)

    #paso el json a df
    df=pd.DataFrame(data_json)


    # selecciono las propiedades de interes
    properties=['id','name','owner','description','fork',"created_at","updated_at",'size','language','forks','open_issues','watchers'] 

    #elimino las columnas que no vaya a utilizar
    df=df[properties]

    #creo una marca temporal del momento de la extracción
    df['extracted_timestamp']=pd.Timestamp.now()

    #extraigo el nombre del usuario
    df['owner']=df['owner'].map(lambda x: x['login'])

    #completo valores nulos
    df[['description','language']].fillna("")

    #corrijo formatos de datos
    df['created_at']=pd.to_datetime(df['created_at'])
    df['updated_at']=pd.to_datetime(df['updated_at'])

    #creo una columna compuesta que debería ser única en Redshift 
    df['comp_id']=df['id'].astype(str)+'_'+df['extracted_timestamp'].astype(str)

    df.drop_duplicates(inplace=True)

    csv_path = (
        f"{path}/raw_data/data.csv"
    )
    df.to_csv(csv_path, index=False, mode="a", header=False)
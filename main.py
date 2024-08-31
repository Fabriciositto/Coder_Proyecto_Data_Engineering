from Modules import extraction, transformation
import pandas as pd
import json

def main():

    data_json=extraction()
    

    # # código auxiliar para guardado y lectura de json con los resultados de la consulta
    # with open('mook.json', 'w') as file:
    #     json.dump(data_json, file)

    # with open('mook.json', 'r') as file:
    #     data_json = json.load(file)

    
    data=transformation(data_json)
    

    data.to_csv('data.csv',encoding="UTF-8")
    

if __name__ == "__main__":
    main()
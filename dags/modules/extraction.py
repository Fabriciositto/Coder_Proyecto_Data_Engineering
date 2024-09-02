import requests
import re
import json
from datetime import datetime

def extraction():
    
    
    user_name='holtzy'
    #armo el url para la consulta
    url=f'https://api.github.com/users/{user_name}/repos'
   
    #realizo primer consulta
    try:
        res=requests.get(url)
    except Exception as e:
        print(f'First request unsuccessful. Error :{e}')


    #guardo la data de la primer consulta
    data_json=res.json()

    # paginación: obtengo información de cuántas páginas tiene la consulta
    link=res.headers["Link"].split(',')
    for x in link:
        if 'rel="last"' in x:
            last=int(re.search(r'<(.*?)>',x).group(1)[-1])

    #paginación: extraigo la data de cada página
    for page in range(2,last+1):
        try:
            new_url=f'{url}?page={page}'
            res=requests.get(new_url)
            data_json.extend(res.json())
        except Exception as e:
            print(f'Request {page} unsuccessful. Error :{e}')
    

    with open('../storage_files/extraction.json', 'w') as file:
        json.dump(data_json, file)

    

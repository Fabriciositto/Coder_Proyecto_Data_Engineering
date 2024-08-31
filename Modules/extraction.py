import requests
import re
import logging


def extraction(user_name):
    
    

    #armo el url para la consulta
    url=f'https://api.github.com/users/{user_name}/repos'
   
    #realizo primer consulta
    try:
        res=requests.get(url)
        logging.info('First request succcessful')
    except Exception as e:
        logging.critical(f'First request unsuccessful. Error :{e}')


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
            logging.info(f'Page {page} requested successfuly')
            data_json.extend(res.json())
        except Exception as e:
            logging.critical(f'Request {page} unsuccessful. Error :{e}')
    
    
    return data_json

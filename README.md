# Consigna
Para finalizar tu segundo pre entregable, te proponemos que el script
de la entrega 1 adapté los datos leídos de la API y cargarlos en la tabla
creada en la pre-entrega anterior en Redshift.


## Objetivos generales
- El script de la entrega 1 deberá adaptar datos leídos de la API y
cargarlos en la tabla creada en la pre-entrega anterior en
Redshift de forma eficiente. En esta entrega se hace énfasis en la
limpieza de los datos crudos obtenidos de la API
## Objetivos específicos
- Generar ETLs a partir de información de APIs usando las librerías
requests, json, psycopg2/SqlAlchemy y pandas
- Solucionar una situación real de ETL donde puedan llegar a
aparecer duplicados, nulos y valores atípicos durante la ingesta-
Transformación- Carga de la data


## Rúbricas
1. Carga del DF en Pandas
El código utiliza sólo un dataframe para la carga de los datos.
2. Adaptaciones de los datos
Los datos fueron extraidos y cargados con sus correspondientes tipos de datos en relación a la tabla creada en Redshift.
3. Carga del DF en Redshift
Todas las columnas son cargadas en la tabla.
4. De-duplicación
Hay una clave primaria compuesta definida en la tabla o en el código Se logra establecer claves primarias compuestas, para que no haya 2 datos diferentes para otro dato ( 2 datos diferentes para una misma ciudad en un mismo día.).




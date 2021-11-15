##### Inicializar sesión de PySpark


```python
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import StructType
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf
from pyspark import SparkFiles
from pyspark.sql.types import *
```


```python
import pandas as pd
import requests
import io
```


```python
import os 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /usr/share/java/mariadb-java-client-2.5.3.jar pyspark-shell'
```


```python
#Configuración de la sesión
spark_context = SparkContext()
sql_context = SQLContext(spark_context)
spark = sql_context.sparkSession
```


    ---------------------------------------------------------------------------

    ValueError                                Traceback (most recent call last)

    <ipython-input-66-365590143f0c> in <module>
          1 #Configuración de la sesión
    ----> 2 spark_context = SparkContext()
          3 sql_context = SQLContext(spark_context)
          4 spark = sql_context.sparkSession


    /usr/local/spark/python/pyspark/context.py in __init__(self, master, appName, sparkHome, pyFiles, environment, batchSize, serializer, conf, gateway, jsc, profiler_cls)
        142                 " is not allowed as it is a security risk.")
        143 
    --> 144         SparkContext._ensure_initialized(self, gateway=gateway, conf=conf)
        145         try:
        146             self._do_init(master, appName, sparkHome, pyFiles, environment, batchSize, serializer,


    /usr/local/spark/python/pyspark/context.py in _ensure_initialized(cls, instance, gateway, conf)
        340 
        341                     # Raise error if there is already a running Spark context
    --> 342                     raise ValueError(
        343                         "Cannot run multiple SparkContexts at once; "
        344                         "existing SparkContext(app=%s, master=%s)"


    ValueError: Cannot run multiple SparkContexts at once; existing SparkContext(app=pyspark-shell, master=local[*]) created by __init__ at <ipython-input-5-365590143f0c>:2 


##### URLs para cargar los datos


```python
aeropuertos = 'https://raw.githubusercontent.com/MIAD-Modelo-Datos/Recursos/main/Infraestructura%20visible/Etapa%201/aeropuertos.csv'
```


```python
vuelos = 'https://raw.githubusercontent.com/MIAD-Modelo-Datos/Recursos/main/Infraestructura%20visible/Etapa%201/vuelos.csv'
```


```python
periodos = 'https://raw.githubusercontent.com/Federico-Higuera/FedericoHiguera/main/CuboOLAP/Periodos.csv'
```

##### Lectura de datos y almacenamiento en DataFrames.

## Extracción de datos


```python
df_vuelos = pd.read_csv(vuelos)
```


```python
df_aeropuertos = pd.read_csv(aeropuertos)
```


```python
df_periodos = pd.read_csv(periodos)
```

## Transformación de datos

### df_vuelos


```python
df_vuelos.shape[0] - df_vuelos.drop_duplicates().shape[0]
```




    13995



##### 13.995 Registros de vuelos totalmente duplicados


```python
df_vuelos.drop_duplicates(inplace = True)
```


```python
df_vuelos.shape
```




    (53604, 13)



##### Se eliminan registros duplicados con diferentes valores en carga y pasajeros


```python
df_vuelos.drop_duplicates(inplace = True, subset = df_vuelos.columns.tolist()[0:8])
```


```python
df_vuelos.shape
```




    (42963, 13)



##### Vuelos con el mismo origen y destino


```python
filtro = df_vuelos['origen'] != df_vuelos['destino']
```


```python
print( "Se eliminarán",len(filtro) - sum(filtro), "registron con el mismo origen y destino")
```

    Se eliminarán 1156 registron con el mismo origen y destino



```python
df_vuelos = df_vuelos.where(filtro).dropna(how = 'all')
```


```python
df_vuelos.shape
```




    (41807, 13)




```python
df_vuelos['id_vuelo'] = list(range(1, df_vuelos.shape[0]+1))
```


```python
df_vuelos.ano = df_vuelos.ano.astype(int)
df_vuelos.mes = df_vuelos.mes.astype(int)
```


```python
df_vuelos = df_vuelos[['id_vuelo', 'ano', 'mes', 'origen', 'destino', 'tipo_vuelo', 'vuelos', 'sillas', 'carga_ofrecida', 'pasajeros','carga_bordo']]
```


```python
df_vuelos
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id_vuelo</th>
      <th>ano</th>
      <th>mes</th>
      <th>origen</th>
      <th>destino</th>
      <th>tipo_vuelo</th>
      <th>vuelos</th>
      <th>sillas</th>
      <th>carga_ofrecida</th>
      <th>pasajeros</th>
      <th>carga_bordo</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2012</td>
      <td>1</td>
      <td>BOG</td>
      <td>CUC</td>
      <td>T</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>4.0</td>
      <td>100.00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>2013</td>
      <td>5</td>
      <td>UIB</td>
      <td>BOG</td>
      <td>R</td>
      <td>30.0</td>
      <td>1110.0</td>
      <td>24000.0</td>
      <td>873.0</td>
      <td>4222.00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>2013</td>
      <td>10</td>
      <td>IBE</td>
      <td>BOG</td>
      <td>R</td>
      <td>98.0</td>
      <td>3626.0</td>
      <td>56056.0</td>
      <td>2866.0</td>
      <td>2323.75</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>2012</td>
      <td>4</td>
      <td>FLA</td>
      <td>BOG</td>
      <td>T</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>4.0</td>
      <td>0.00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>2013</td>
      <td>7</td>
      <td>CUC</td>
      <td>AUC</td>
      <td>T</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>180.00</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>67593</th>
      <td>41803</td>
      <td>2013</td>
      <td>12</td>
      <td>VVC</td>
      <td>MVP</td>
      <td>R</td>
      <td>7.0</td>
      <td>126.0</td>
      <td>9800.0</td>
      <td>122.0</td>
      <td>4270.00</td>
    </tr>
    <tr>
      <th>67594</th>
      <td>41804</td>
      <td>2013</td>
      <td>3</td>
      <td>VVC</td>
      <td>NVA</td>
      <td>R</td>
      <td>1.0</td>
      <td>18.0</td>
      <td>1400.0</td>
      <td>0.0</td>
      <td>1240.00</td>
    </tr>
    <tr>
      <th>67595</th>
      <td>41805</td>
      <td>2013</td>
      <td>9</td>
      <td>VVC</td>
      <td>NVA</td>
      <td>R</td>
      <td>1.0</td>
      <td>18.0</td>
      <td>2800.0</td>
      <td>0.0</td>
      <td>2738.00</td>
    </tr>
    <tr>
      <th>67597</th>
      <td>41806</td>
      <td>2013</td>
      <td>1</td>
      <td>VVC</td>
      <td>RAV</td>
      <td>R</td>
      <td>1.0</td>
      <td>18.0</td>
      <td>1400.0</td>
      <td>6.0</td>
      <td>1063.00</td>
    </tr>
    <tr>
      <th>67598</th>
      <td>41807</td>
      <td>2013</td>
      <td>2</td>
      <td>VVC</td>
      <td>RAV</td>
      <td>R</td>
      <td>8.0</td>
      <td>144.0</td>
      <td>112000.0</td>
      <td>61.0</td>
      <td>14113.00</td>
    </tr>
  </tbody>
</table>
<p>41807 rows × 11 columns</p>
</div>



##### Se asigna el atributo 'periodo' a la tabla de hechos de 'vuelo' 


```python
df_vuelos['periodo'] = list(zip(df_vuelos.ano, df_vuelos.mes))
```


```python
df_periodos['Periodo'] = list(zip(df_periodos.Ano, df_periodos.Mes))
```


```python
df_vuelos = df_vuelos.merge(df_periodos.rename(columns = {'Periodo' : 'periodo'}), how = 'left', on = 'periodo')[['id_vuelo', 'Id_Periodo', 'origen', 'destino', 'tipo_vuelo', 'vuelos','sillas', 'carga_ofrecida', 'pasajeros', 'carga_bordo']]
```

### df_aeropuertos

##### Existen 80 aeropuertos duplicados, se eliminan los valores repetidos en la columna 'sigla'


```python
df_aeropuertos.shape[0] - df_aeropuertos.drop_duplicates(subset = ['sigla']).shape[0]
```




    80




```python
df_aeropuertos.drop_duplicates(subset = ['sigla'], inplace = True)
```


```python
df_aeropuertos.shape
```




    (212, 23)



##### Se eliminan aeropuertos que fueron construidos después de 2013


```python
df_aeropuertos['año'] = df_aeropuertos['fecha_construccion'].str.split("-", expand = True)[0].astype(int)
```


```python
filtro = df_aeropuertos['año'] <= 2013
```


```python
df_aeropuertos = df_aeropuertos.where(filtro).dropna(how = 'all')
```


```python
df_aeropuertos['id_aeropuerto'] = list(range(1, df_aeropuertos.shape[0]+1 ))
```


```python
df_aeropuertos = df_aeropuertos[['id_aeropuerto','sigla', 'iata', 'nombre', 'municipio', 'departamento','categoria','numero_vuelos_origen', 'latitud','longitud']]
```


```python
df_aeropuertos
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id_aeropuerto</th>
      <th>sigla</th>
      <th>iata</th>
      <th>nombre</th>
      <th>municipio</th>
      <th>departamento</th>
      <th>categoria</th>
      <th>numero_vuelos_origen</th>
      <th>latitud</th>
      <th>longitud</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>BOG</td>
      <td>BOG</td>
      <td>EL DORADO</td>
      <td>Bogotá, D.C.</td>
      <td>Bogotá, D.C.</td>
      <td>Internacional</td>
      <td>1804153.0</td>
      <td>4.7017</td>
      <td>-74.1469</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>TLU</td>
      <td>TLU</td>
      <td>TOLU</td>
      <td>Tolú Viejo</td>
      <td>Sucre</td>
      <td>Regional</td>
      <td>227253.0</td>
      <td>9.5095</td>
      <td>-75.5859</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3</td>
      <td>CUC</td>
      <td>CUC</td>
      <td>CAMILO DAZA</td>
      <td>San José de Cúcuta</td>
      <td>Norte de Santander</td>
      <td>Internacional</td>
      <td>91797.0</td>
      <td>7.9274</td>
      <td>-72.5116</td>
    </tr>
    <tr>
      <th>9</th>
      <td>4</td>
      <td>9MO</td>
      <td>NaN</td>
      <td>SAN NICOLAS</td>
      <td>Tauramena</td>
      <td>Casanare</td>
      <td>Aeródromo</td>
      <td>244452.0</td>
      <td>4.4703</td>
      <td>-72.4575</td>
    </tr>
    <tr>
      <th>10</th>
      <td>5</td>
      <td>TCO</td>
      <td>NaN</td>
      <td>LA FLORIDA</td>
      <td>San Andrés de Tumaco</td>
      <td>Nariño</td>
      <td>Nacional</td>
      <td>596754.0</td>
      <td>1.8144</td>
      <td>-78.7490</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>283</th>
      <td>158</td>
      <td>URR</td>
      <td>URR</td>
      <td>ALI PIEDRAHITA</td>
      <td>Urrao</td>
      <td>Antioquia</td>
      <td>Regional</td>
      <td>3524.0</td>
      <td>6.3273</td>
      <td>-76.1409</td>
    </tr>
    <tr>
      <th>284</th>
      <td>159</td>
      <td>VGP</td>
      <td>VGZ</td>
      <td>CANANGUCHAL</td>
      <td>Villagarzón</td>
      <td>Putumayo</td>
      <td>Regional</td>
      <td>5927.0</td>
      <td>0.9785</td>
      <td>-76.6064</td>
    </tr>
    <tr>
      <th>286</th>
      <td>160</td>
      <td>VVC</td>
      <td>VVC</td>
      <td>VANGUARDIA</td>
      <td>Villavicencio</td>
      <td>Meta</td>
      <td>Nacional</td>
      <td>117754.0</td>
      <td>4.1684</td>
      <td>-73.6147</td>
    </tr>
    <tr>
      <th>288</th>
      <td>161</td>
      <td>YAP</td>
      <td>NaN</td>
      <td>YAPIMA</td>
      <td>Mitú</td>
      <td>Vaupés</td>
      <td>Aeródromo</td>
      <td>2.0</td>
      <td>1.0825</td>
      <td>-69.4914</td>
    </tr>
    <tr>
      <th>291</th>
      <td>162</td>
      <td>ZPS</td>
      <td>AZT</td>
      <td>GUILLERMO GOMEZ ORTIZ</td>
      <td>Zapatoca</td>
      <td>Santander</td>
      <td>Aeródromo</td>
      <td>30.0</td>
      <td>6.7674</td>
      <td>-73.2846</td>
    </tr>
  </tbody>
</table>
<p>162 rows × 10 columns</p>
</div>



##### Se crea la dimensión 'trayecto'


```python
df_vuelos['trayecto'] = list(zip(df_vuelos.origen, df_vuelos.destino))
```


```python
df_trayectos = pd.DataFrame()
df_trayectos['trayecto'] = df_vuelos['trayecto'].unique()
```


```python
df_trayectos['Origen'] = pd.DataFrame(df_trayectos['trayecto'].tolist(), index=df_trayectos.index)[0]
df_trayectos
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>trayecto</th>
      <th>Origen</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>(BOG, CUC)</td>
      <td>BOG</td>
    </tr>
    <tr>
      <th>1</th>
      <td>(UIB, BOG)</td>
      <td>UIB</td>
    </tr>
    <tr>
      <th>2</th>
      <td>(IBE, BOG)</td>
      <td>IBE</td>
    </tr>
    <tr>
      <th>3</th>
      <td>(FLA, BOG)</td>
      <td>FLA</td>
    </tr>
    <tr>
      <th>4</th>
      <td>(CUC, AUC)</td>
      <td>CUC</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>1156</th>
      <td>(CZU, UIB)</td>
      <td>CZU</td>
    </tr>
    <tr>
      <th>1157</th>
      <td>(NQU, PZA)</td>
      <td>NQU</td>
    </tr>
    <tr>
      <th>1158</th>
      <td>(GYM, SNR)</td>
      <td>GYM</td>
    </tr>
    <tr>
      <th>1159</th>
      <td>(PZA, GYM)</td>
      <td>PZA</td>
    </tr>
    <tr>
      <th>1160</th>
      <td>(BMG, MVP)</td>
      <td>BMG</td>
    </tr>
  </tbody>
</table>
<p>1161 rows × 2 columns</p>
</div>




```python
df_trayectos['Origen'] = pd.DataFrame(df_trayectos['trayecto'].tolist(), index=df_trayectos.index)[0]
df_trayectos['Destino'] = pd.DataFrame(df_trayectos['trayecto'].tolist(), index=df_trayectos.index)[1]
```


```python
df_trayectos['Id_trayecto'] = list(range(1, df_trayectos.shape[0]+1))
```


```python
df_trayectos
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>trayecto</th>
      <th>Origen</th>
      <th>Destino</th>
      <th>Id_trayecto</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>(BOG, CUC)</td>
      <td>BOG</td>
      <td>CUC</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>(UIB, BOG)</td>
      <td>UIB</td>
      <td>BOG</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>(IBE, BOG)</td>
      <td>IBE</td>
      <td>BOG</td>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>(FLA, BOG)</td>
      <td>FLA</td>
      <td>BOG</td>
      <td>4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>(CUC, AUC)</td>
      <td>CUC</td>
      <td>AUC</td>
      <td>5</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>1156</th>
      <td>(CZU, UIB)</td>
      <td>CZU</td>
      <td>UIB</td>
      <td>1157</td>
    </tr>
    <tr>
      <th>1157</th>
      <td>(NQU, PZA)</td>
      <td>NQU</td>
      <td>PZA</td>
      <td>1158</td>
    </tr>
    <tr>
      <th>1158</th>
      <td>(GYM, SNR)</td>
      <td>GYM</td>
      <td>SNR</td>
      <td>1159</td>
    </tr>
    <tr>
      <th>1159</th>
      <td>(PZA, GYM)</td>
      <td>PZA</td>
      <td>GYM</td>
      <td>1160</td>
    </tr>
    <tr>
      <th>1160</th>
      <td>(BMG, MVP)</td>
      <td>BMG</td>
      <td>MVP</td>
      <td>1161</td>
    </tr>
  </tbody>
</table>
<p>1161 rows × 4 columns</p>
</div>




```python
##### Backups de Dataframes

#bu_vuelos = df_vuelos.copy()
#bu_aeropuertos = df_aeropuertos.copy()
#bu_trayectos = df_trayectos.copy()
#bu_periodos = df_periodos.copy()
```

##### Se realizan los 'Joins' correspondientes

Asignación de 'id_trayecto' a 'df_vuelos'


```python
#df1.merge(df2, left_on='lkey', right_on='rkey')
df_vuelos = df_vuelos.merge(df_trayectos, how = 'left', on = 'trayecto', sort = False)[['id_vuelo','Id_Periodo', 'Id_trayecto','tipo_vuelo', 'vuelos', 'sillas', 'carga_ofrecida', 'pasajeros', 'carga_bordo']].sort_values(by=['id_vuelo'])
```

Asignación de 'id_aer_origen' y 'id_aer_destino' a 'df_trayectos'


```python
print(df_aeropuertos.columns)
```

    Index(['id_aeropuerto', 'sigla', 'iata', 'nombre', 'municipio', 'departamento',
           'categoria', 'numero_vuelos_origen', 'latitud', 'longitud'],
          dtype='object')



```python
df_aeropuertos.rename(columns = {'sigla' : 'Origen'}, inplace = False)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id_aeropuerto</th>
      <th>Origen</th>
      <th>iata</th>
      <th>nombre</th>
      <th>municipio</th>
      <th>departamento</th>
      <th>categoria</th>
      <th>numero_vuelos_origen</th>
      <th>latitud</th>
      <th>longitud</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>BOG</td>
      <td>BOG</td>
      <td>EL DORADO</td>
      <td>Bogotá, D.C.</td>
      <td>Bogotá, D.C.</td>
      <td>Internacional</td>
      <td>1804153.0</td>
      <td>4.7017</td>
      <td>-74.1469</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>TLU</td>
      <td>TLU</td>
      <td>TOLU</td>
      <td>Tolú Viejo</td>
      <td>Sucre</td>
      <td>Regional</td>
      <td>227253.0</td>
      <td>9.5095</td>
      <td>-75.5859</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3</td>
      <td>CUC</td>
      <td>CUC</td>
      <td>CAMILO DAZA</td>
      <td>San José de Cúcuta</td>
      <td>Norte de Santander</td>
      <td>Internacional</td>
      <td>91797.0</td>
      <td>7.9274</td>
      <td>-72.5116</td>
    </tr>
    <tr>
      <th>9</th>
      <td>4</td>
      <td>9MO</td>
      <td>NaN</td>
      <td>SAN NICOLAS</td>
      <td>Tauramena</td>
      <td>Casanare</td>
      <td>Aeródromo</td>
      <td>244452.0</td>
      <td>4.4703</td>
      <td>-72.4575</td>
    </tr>
    <tr>
      <th>10</th>
      <td>5</td>
      <td>TCO</td>
      <td>NaN</td>
      <td>LA FLORIDA</td>
      <td>San Andrés de Tumaco</td>
      <td>Nariño</td>
      <td>Nacional</td>
      <td>596754.0</td>
      <td>1.8144</td>
      <td>-78.7490</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>283</th>
      <td>158</td>
      <td>URR</td>
      <td>URR</td>
      <td>ALI PIEDRAHITA</td>
      <td>Urrao</td>
      <td>Antioquia</td>
      <td>Regional</td>
      <td>3524.0</td>
      <td>6.3273</td>
      <td>-76.1409</td>
    </tr>
    <tr>
      <th>284</th>
      <td>159</td>
      <td>VGP</td>
      <td>VGZ</td>
      <td>CANANGUCHAL</td>
      <td>Villagarzón</td>
      <td>Putumayo</td>
      <td>Regional</td>
      <td>5927.0</td>
      <td>0.9785</td>
      <td>-76.6064</td>
    </tr>
    <tr>
      <th>286</th>
      <td>160</td>
      <td>VVC</td>
      <td>VVC</td>
      <td>VANGUARDIA</td>
      <td>Villavicencio</td>
      <td>Meta</td>
      <td>Nacional</td>
      <td>117754.0</td>
      <td>4.1684</td>
      <td>-73.6147</td>
    </tr>
    <tr>
      <th>288</th>
      <td>161</td>
      <td>YAP</td>
      <td>NaN</td>
      <td>YAPIMA</td>
      <td>Mitú</td>
      <td>Vaupés</td>
      <td>Aeródromo</td>
      <td>2.0</td>
      <td>1.0825</td>
      <td>-69.4914</td>
    </tr>
    <tr>
      <th>291</th>
      <td>162</td>
      <td>ZPS</td>
      <td>AZT</td>
      <td>GUILLERMO GOMEZ ORTIZ</td>
      <td>Zapatoca</td>
      <td>Santander</td>
      <td>Aeródromo</td>
      <td>30.0</td>
      <td>6.7674</td>
      <td>-73.2846</td>
    </tr>
  </tbody>
</table>
<p>162 rows × 10 columns</p>
</div>




```python
df_trayectos = df_trayectos.merge(df_aeropuertos.rename(columns = {'sigla' : 'Origen'}, inplace = False), how = 'left', on = 'Origen')[[ 'Id_trayecto', 'trayecto','id_aeropuerto','Origen','Destino']].rename(columns = {'id_aeropuerto': 'id_aer_origen'}).merge(df_aeropuertos.rename(columns = {'sigla' : 'Destino'}, inplace = False), how = 'left', on = 'Destino')[[ 'Id_trayecto', 'trayecto','id_aer_origen','Origen','id_aeropuerto','Destino']].rename(columns = {'id_aeropuerto': 'id_aer_destino'})
```

##### Resultado de las transformaciones


```python
df_vuelos.loc[56]
```




    id_vuelo           57
    Id_Periodo         41
    Id_trayecto        44
    tipo_vuelo          T
    vuelos            1.0
    sillas            0.0
    carga_ofrecida    0.0
    pasajeros         1.0
    carga_bordo       0.0
    Name: 56, dtype: object




```python
df_aeropuertos
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id_aeropuerto</th>
      <th>sigla</th>
      <th>iata</th>
      <th>nombre</th>
      <th>municipio</th>
      <th>departamento</th>
      <th>categoria</th>
      <th>numero_vuelos_origen</th>
      <th>latitud</th>
      <th>longitud</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>BOG</td>
      <td>BOG</td>
      <td>EL DORADO</td>
      <td>Bogotá, D.C.</td>
      <td>Bogotá, D.C.</td>
      <td>Internacional</td>
      <td>1804153.0</td>
      <td>4.7017</td>
      <td>-74.1469</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>TLU</td>
      <td>TLU</td>
      <td>TOLU</td>
      <td>Tolú Viejo</td>
      <td>Sucre</td>
      <td>Regional</td>
      <td>227253.0</td>
      <td>9.5095</td>
      <td>-75.5859</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3</td>
      <td>CUC</td>
      <td>CUC</td>
      <td>CAMILO DAZA</td>
      <td>San José de Cúcuta</td>
      <td>Norte de Santander</td>
      <td>Internacional</td>
      <td>91797.0</td>
      <td>7.9274</td>
      <td>-72.5116</td>
    </tr>
    <tr>
      <th>9</th>
      <td>4</td>
      <td>9MO</td>
      <td>NaN</td>
      <td>SAN NICOLAS</td>
      <td>Tauramena</td>
      <td>Casanare</td>
      <td>Aeródromo</td>
      <td>244452.0</td>
      <td>4.4703</td>
      <td>-72.4575</td>
    </tr>
    <tr>
      <th>10</th>
      <td>5</td>
      <td>TCO</td>
      <td>NaN</td>
      <td>LA FLORIDA</td>
      <td>San Andrés de Tumaco</td>
      <td>Nariño</td>
      <td>Nacional</td>
      <td>596754.0</td>
      <td>1.8144</td>
      <td>-78.7490</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>283</th>
      <td>158</td>
      <td>URR</td>
      <td>URR</td>
      <td>ALI PIEDRAHITA</td>
      <td>Urrao</td>
      <td>Antioquia</td>
      <td>Regional</td>
      <td>3524.0</td>
      <td>6.3273</td>
      <td>-76.1409</td>
    </tr>
    <tr>
      <th>284</th>
      <td>159</td>
      <td>VGP</td>
      <td>VGZ</td>
      <td>CANANGUCHAL</td>
      <td>Villagarzón</td>
      <td>Putumayo</td>
      <td>Regional</td>
      <td>5927.0</td>
      <td>0.9785</td>
      <td>-76.6064</td>
    </tr>
    <tr>
      <th>286</th>
      <td>160</td>
      <td>VVC</td>
      <td>VVC</td>
      <td>VANGUARDIA</td>
      <td>Villavicencio</td>
      <td>Meta</td>
      <td>Nacional</td>
      <td>117754.0</td>
      <td>4.1684</td>
      <td>-73.6147</td>
    </tr>
    <tr>
      <th>288</th>
      <td>161</td>
      <td>YAP</td>
      <td>NaN</td>
      <td>YAPIMA</td>
      <td>Mitú</td>
      <td>Vaupés</td>
      <td>Aeródromo</td>
      <td>2.0</td>
      <td>1.0825</td>
      <td>-69.4914</td>
    </tr>
    <tr>
      <th>291</th>
      <td>162</td>
      <td>ZPS</td>
      <td>AZT</td>
      <td>GUILLERMO GOMEZ ORTIZ</td>
      <td>Zapatoca</td>
      <td>Santander</td>
      <td>Aeródromo</td>
      <td>30.0</td>
      <td>6.7674</td>
      <td>-73.2846</td>
    </tr>
  </tbody>
</table>
<p>162 rows × 10 columns</p>
</div>



## Carga de Dataframes tratados a .CSV


```python
path = """C:/Users/FedericoHiguera/OneDrive/Maestria/"""
```


```python
df_vuelos.to_csv('Vuelos_ETL.csv')
```


```python
df_trayectos.to_csv('Trayectos_ETL.csv')
```


```python
df_aeropuertos.to_csv('Aeropuertos_ETL.csv')
```

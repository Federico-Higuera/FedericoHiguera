# Tarea: Mi primer proceso ETL

#### Se inicializa la sesión de PySpark y configura el controlador MariaDb


```python
import os 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /usr/share/java/mariadb-java-client-2.5.3.jar pyspark-shell'
```


```python
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import functions as f
from pyspark.sql.functions import concat,col
import mysql.connector
```


```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,lit
```


```python
import pandas as pd
```


```python
spark_context = SparkContext()
sql_context = SQLContext(spark_context)
spark = sql_context.sparkSession
```

### E. Extracción de los datos necesarios

#### 1. Aeropuertos.csv


```python
path = './InfraestructuraVisible/'
```


```python
def load_aeropuertos(spark):
    return spark.read.format("csv").option("header", "true").load(path+"aeropuertos.csv")
```


```python
df_aeropuertos = load_aeropuertos(spark).select('sigla','nombre', 'municipio','departamento','categoria','tipo')
```


```python
df_aeropuertos.show()
```

    +-----+--------------------+--------------------+------------------+-------------+----------+
    |sigla|              nombre|           municipio|      departamento|    categoria|      tipo|
    +-----+--------------------+--------------------+------------------+-------------+----------+
    |  BOG|           EL DORADO|        Bogotá, D.C.|      Bogotá, D.C.|Internacional| Aerocivil|
    |  9DG|        EL LAGO - CA|             Piedras|            Tolima|    Aeródromo|   Privado|
    |  TLU|                TOLU|          Tolú Viejo|             Sucre|     Regional| Aerocivil|
    |  9MK|      VILLA ISABELLA|            Trinidad|          Casanare|    Aeródromo|Fumigación|
    |  CUC|         CAMILO DAZA|  San José de Cúcuta|Norte de Santander|Internacional| Aerocivil|
    |  9BE|           LAS VEGAS|      Paz de Ariporo|          Casanare|    Aeródromo|   Privado|
    |  YPP|      GERMAN ALBERTO|               Yopal|          Casanare|    Aeródromo|Fumigación|
    |  VVU|        SANTA ISABEL|       Villavicencio|              Meta|    Aeródromo|Fumigación|
    |  7GD|           VELASQUEZ|       Puerto Boyacá|            Boyacá|    Aeródromo|   Privado|
    |  9MO|         SAN NICOLAS|           Tauramena|          Casanare|    Aeródromo|Fumigación|
    |  TCO|          LA FLORIDA|San Andrés de Tumaco|            Nariño|     Nacional| Aerocivil|
    |  ELB|          LAS FLORES|            El Banco|         Magdalena|    Aeródromo| Aerocivil|
    |  9AJ|    SAN LUIS DE PACA|                Mitú|            Vaupés|    Aeródromo|   Público|
    |  A03|              FORTUL|              Fortul|            Arauca|    Aeródromo|   Público|
    |  7NP|          EL CAIRANO|      Santa Catalina|           Bolívar|    Aeródromo|   Privado|
    |  9MN|   EL PEDRAL BONANZA|      Puerto Wilches|         Santander|    Aeródromo|Fumigación|
    |  NQU|       REYES MURILLO|               Nuquí|             Chocó|     Regional| Aerocivil|
    |  BMG|      BARRANCO MINAS|      Barranco Minas|           Guainía|    Aeródromo|   Público|
    |  9MG|          MADREVIEJA|             Nunchía|          Casanare|    Aeródromo|Fumigación|
    |  PIP|PAIPA JUAN JOSE R...|               Paipa|            Boyacá|    Aeródromo| Aerocivil|
    +-----+--------------------+--------------------+------------------+-------------+----------+
    only showing top 20 rows
    


#### 2. vuelos.csv


```python
def load_vuelos(spark):
    return spark.read.format("csv").option("header", "true").load(path+"vuelos.csv")
```


```python
df_vuelos = load_vuelos(spark).select('ano', 'mes', 'origen', 'destino', 'vuelos', 'sillas','carga_ofrecida', 'pasajeros','carga_bordo')
```


```python
df_vuelos.show()
```

    +----+---+------+-------+------+------+--------------+---------+------------------+
    | ano|mes|origen|destino|vuelos|sillas|carga_ofrecida|pasajeros|       carga_bordo|
    +----+---+------+-------+------+------+--------------+---------+------------------+
    |2012|  1|   BOG|    CUC|   1.0|   0.0|           0.0|      4.0|             100.0|
    |2013|  5|   UIB|    BOG|  30.0|1110.0|       24000.0|    873.0|            4222.0|
    |2013| 10|   IBE|    BOG|  98.0|3626.0|       56056.0|   2866.0|           2323.75|
    |2012|  4|   FLA|    BOG|   1.0|   0.0|           0.0|      4.0|               0.0|
    |2013|  7|   CUC|    AUC|   1.0|   0.0|           0.0|      nan|             180.0|
    |2012|  3|   CUC|    OCV|   6.0|   0.0|           0.0|      6.0|               0.0|
    |2013| 11|   7NS|    BOG|   nan|   nan|           0.0|    149.0|             880.0|
    |2010|  6|   AUC|    TME|   1.0|   0.0|           0.0|      2.0|              50.0|
    |2010|  4|   CLO|    BOG|   3.0|  96.0|        7078.5|      2.0|               0.0|
    |2013|  5|   BOG|    CLO|   1.0|   0.0|           0.0|     10.0|               0.0|
    |2013| 12|   MVP|    VVC|   9.0| 621.0|       18000.0|    569.0|               0.0|
    |2013|  4|   BOG|    IPI|  13.0| 416.0|       19500.0|      nan|               0.0|
    |2012|  6|   BOG|    FLA|   4.0|   nan|           0.0|     11.0|               0.0|
    |2011|  6|   NVA|    BOG|   2.0|   0.0|           0.0|     11.0|              40.0|
    |2010|  5|   BOG|    CTG|   1.0| 142.0|        7211.0|    107.0|              34.0|
    |2010| 11|   BOG|    VVC|   2.0|   0.0|           0.0|      0.0|1687.3999999999999|
    |2013|  5|   EYP|    AUC|   nan|   0.0|           0.0|     18.0|               0.0|
    |2013| 11|   PUU|    BOG|   1.0|  70.0|        2000.0|     29.0|               0.0|
    |2011| 10|   BOG|    CLO|   4.0|   0.0|           0.0|      8.0|               0.0|
    |2012| 10|   BOG|    ADZ|   1.0|   0.0|           0.0|      2.0|             100.0|
    +----+---+------+-------+------+------+--------------+---------+------------------+
    only showing top 20 rows
    


#### 3. Cobertura de Aerea de Centros Poblados por Categoria de Aeropuerto1.csv


```python
def load_CoberturaAerea(spark):
    return spark.read.format("csv").option("header", "true").load(path+"Cobertura de Aerea de Centros Poblados por Categoria de Aeropuerto1.csv")
```


```python
df_cobertura = load_CoberturaAerea(spark)#.select('Centro Poblado', 'Aeropuerto', 'Distancia(Km)', 'Cobertura')
```


```python
df_cobertura.show()
```

    +---------------+--------------------+----------------+---------+--------------------+----------------+--------------------+----------------+--------------------+----------------+--------------------+----------------+--------------+
    | Centro Poblado|          Aeropuerto|   Distancia(Km)|Cobertura|           Aerodromo|     D_Aerodromo|            Regional|      D_Regional|            Nacional|      D_Nacional|       Internacional| D_Internacional|Tipo_Cobertura|
    +---------------+--------------------+----------------+---------+--------------------+----------------+--------------------+----------------+--------------------+----------------+--------------------+----------------+--------------+
    |      ABASTICOS|           EL DORADO|34.4490837685403|     True|      VILLA GEORGINA|36.2106614000918|          SAN MARTIN|87.8515968551185|          VANGUARDIA|51.6883416180031|           EL DORADO|34.4490837685403| Internacional|
    |       ACAPULCO|          HORIZONTES|3.30402959375963|     True|          HORIZONTES|3.30402959375963|       COLONIZADORES| 141.02501304312|           YARIGUIES|74.4304171986738|           PALONEGRO|14.2111163795027| Internacional|
    |        ACEVEDO|            CONTADOR|22.6532566970602|     True|            LA JAGUA|43.6493625266892|            CONTADOR|22.6532566970602|GUSTAVO ARTUNDUAG...|43.3237402716814|        BENITO SALAS|143.390689536798|      Regional|
    |   BARRO BLANCO|        SAN BERNARDO|25.3697141388984|     True|        SAN BERNARDO|25.3697141388984|          LAS BRUJAS|71.8220061729186|        LOS GARZONES|144.989267076566|        RAFAEL NUÑEZ|152.243581933441|     Aeródromo|
    |       EL CERRO|      ANTONIO NARIÑO|38.0781434211722|     True|         EL GUAYABAL|71.3016456197922|        JUAN CASIANO|109.195513154648|      ANTONIO NARIÑO|38.0781434211722|ALFONSO BONILLA A...|240.676994865676|      Nacional|
    |    AGUA BONITA|      VILLA GEORGINA|7.73050896203633|     True|      VILLA GEORGINA|7.73050896203633|       SANTIAGO VILA|54.7579608123635|          VANGUARDIA|84.3176710618212|           EL DORADO|36.1004278601308| Internacional|
    |        EL PLAN|ALFONSO LOPEZ PUM...|23.2148253274297|     True|  SAN JUAN DEL CESAR|31.9749980192931|JORGE ISAACS (LA ...|189.831570369287|ALFONSO LOPEZ PUM...|23.2148253274297|   ALMIRANTE PADILLA|119.388680858664|      Nacional|
    |     AGUA LINDA|         CAMILO DAZA|16.4026289132759|     True|              CINERA|43.6742546493805|        AGUAS CLARAS|110.475454725398|           YARIGUIES|165.537513072767|         CAMILO DAZA|16.4026289132759| Internacional|
    |       AGUANICA| FLAMINIO S. CAMACHO|13.6553491468359|     True| FLAMINIO S. CAMACHO|13.6553491468359|      JOSE CELESTINO|97.9296964949602|          VANGUARDIA|97.5521278643301|           EL DORADO|28.1545846028419| Internacional|
    |        AIMARAL|   ALMIRANTE PADILLA|14.5714039199428|     True|          SHANGRI-LA|44.5168192871937|      PUERTO BOLIVAR|118.520239172684|ALFONSO LOPEZ PUM...|129.317355513779|   ALMIRANTE PADILLA|14.5714039199428| Internacional|
    |       EL LLANO|BUENAVENTURA-GERA...|9.92784300922365|     True|EDGAR CALDAS POTE...|61.7323490394532|FARFAN HERIBERTO ...|95.7008327445712|BUENAVENTURA-GERA...|9.92784300922365|ALFONSO BONILLA A...|72.4122624243147|      Nacional|
    |         ALASKA|    SAN ROQUE  - CA.|13.1830073258331|     True|    SAN ROQUE  - CA.|13.1830073258331|FARFAN HERIBERTO ...|23.0006265111197|BUENAVENTURA-GERA...|90.0260593587472|ALFONSO BONILLA A...|44.6935474996501|      Regional|
    |   LAS CAMELIAS|           MORICHITO| 14.895444916699|     True|           MORICHITO| 14.895444916699|      GUSTAVO VARGAS|46.7086926843324|        EL ALCARAVAN|99.6661947344119|           PALONEGRO|190.517179830657|     Aeródromo|
    |     LAS CANOAS|          NUEVA ROMA|34.4728908947269|     True|          NUEVA ROMA|34.4728908947269|          LAS BRUJAS|132.441246675237|ALFONSO LOPEZ PUM...|137.994259877552|   ERNESTO CORTISSOZ|79.4776232471418|     Aeródromo|
    |     ALEJANDRÍA|          NUEVA ROMA|25.8580598699215|     True|          NUEVA ROMA|25.8580598699215|          LAS BRUJAS|145.491742855807|ALFONSO LOPEZ PUM...|109.256012494505|   ERNESTO CORTISSOZ|131.095226009146|     Aeródromo|
    |        ALTAMAR| FLAMINIO S. CAMACHO|15.4248886590723|     True| FLAMINIO S. CAMACHO|15.4248886590723|       SANTIAGO VILA|106.325204614854|          VANGUARDIA|72.8907549069129|           EL DORADO|21.7622326343405| Internacional|
    |       ALTAMIRA|          HORIZONTES|13.7427968187756|     True|          HORIZONTES|13.7427968187756|       COLONIZADORES|129.290417558113|           YARIGUIES|87.1391858081435|           PALONEGRO|30.1736314786314| Internacional|
    |    PIEDECUESTA|      JOSE CELESTINO|13.5159849488022|     True|GANADERIA EL TRIUNFO|22.9270547733822|      JOSE CELESTINO|13.5159849488022|            LA NUBIA|56.3136993029632|            MATECAÑA| 92.269217233967|      Regional|
    |         AMALFI|              EL RIO|4.29901138791201|     True|              EL RIO|4.29901138791201|        REMEDIOS OTU|41.9482926846517|       OLAYA HERRERA|95.2190706770922|     JOSE M. CORDOVA|91.2632551082593|     Aeródromo|
    |ALTO DE CÓRDOBA|           EL DORADO|22.3578002087464|     True| FLAMINIO S. CAMACHO| 27.626832455803|      JOSE CELESTINO|   77.0921087369|             PERALES|100.715276424281|           EL DORADO|22.3578002087464| Internacional|
    +---------------+--------------------+----------------+---------+--------------------+----------------+--------------------+----------------+--------------------+----------------+--------------------+----------------+--------------+
    only showing top 20 rows
    


#### 4. Dimension_Mes.csv


```python
df_mes = spark.read.format("csv").option("header", "true").load(path+"Dimension_Mes.csv")
```


```python
df_mes = df_mes.limit(12)
```


```python
df_mes.show()
```

    +---+----------+---+----+
    |Mes|Nombre_Mes|  Q|Días|
    +---+----------+---+----+
    |  1|     Enero| Q1|  31|
    |  2|   Febrero| Q1|  28|
    |  3|     Marzo| Q1|  31|
    |  4|     Abril| Q2|  30|
    |  5|      Mayo| Q2|  31|
    |  6|     Junio| Q2|  30|
    |  7|     Julio| Q3|  31|
    |  8|    Agosto| Q3|  31|
    |  9|Septiembre| Q3|  30|
    | 10|   Octubre| Q4|  31|
    | 11| Noviembre| Q4|  30|
    | 12| Diciembre| Q4|  31|
    +---+----------+---+----+
    


### T. Transformación


```python
## Crear columna ID
##df = df.withColumn('ID', f.monotonically_increasing_id())
```


```python
w = Window().orderBy(lit('A'))
```

##### df_vuelos


```python
print('Se eliminarán', df_vuelos.count() - df_vuelos.dropDuplicates().count(),'registros duplicados en df_vuelos')
```

    Se eliminarán 2649 registros duplicados en df_vuelos



```python
df_vuelos = df_vuelos.dropDuplicates()
```


```python
df_vuelos.distinct().count()
```




    64950




```python
df_vuelos.show()
```

    +---------+----+---+------+-------+------+------+--------------+---------+------------------+
    |Id_vuelos| ano|mes|origen|destino|vuelos|sillas|carga_ofrecida|pasajeros|       carga_bordo|
    +---------+----+---+------+-------+------+------+--------------+---------+------------------+
    |        1|2013|  5|   BOG|    AUC|   1.0|   0.0|           0.0|      8.0|               0.0|
    |        2|2011|  8|   7GC|    EYP|   1.0|   0.0|           0.0|      2.0|              36.4|
    |        3|2010|  5|   VVC|    BMG|   nan|   nan|           0.0|      nan|             328.9|
    |        4|2013|  4|   NVA|    BOG|  59.0|   nan|       23600.0|   1468.0|            2068.0|
    |        5|2010|  7|   CLO|    TCO|   8.0| 256.0|        6000.0|    149.0|            1828.0|
    |        6|2011| 11|   PUU|    CLO|   1.0|   0.0|           0.0|      4.0|               0.0|
    |        7|2012| 11|   BOG|    RVE|   1.0|   0.0|           0.0|      nan|            2000.0|
    |        8|2013| 12|   NVA|    PTL|   2.0|   0.0|           0.0|      3.0|              50.0|
    |        9|2013|  3|   PZA|    EYP|   1.0|   0.0|           0.0|      2.0|               0.0|
    |       10|2013|  9|   CLO|    TCO|   4.0|   0.0|           0.0|      nan|             400.0|
    |       11|2013|  8|   BUN|    BUN|   1.0|   nan|           0.0|      4.0|               0.0|
    |       12|2011|  1|   MVP|    BOG|   4.0| 304.0|        4800.0|    153.0|3942.1200000000003|
    |       13|2010|  4|   PVA|    ADZ|  67.0|1273.0|        8375.0|    861.0|31063.760000000002|
    |       14|2010| 11|   CZU|    BOG|  24.0|1824.0|       57600.0|   1409.0|             562.0|
    |       15|2013|  8|   IPI|    CLO|   2.0|   nan|           0.0|      7.0|             291.2|
    |       16|2011|  6|   SVI|    NVA|   2.0|   0.0|           0.0|      4.0|             200.0|
    |       17|2011|  5|   CUC|    BOG|  30.0|   nan|        8129.0|      nan|            152.88|
    |       18|2011|  8|   BOG|    EYP|   8.0| 400.0|        9600.0|    216.0|             195.0|
    |       19|2011| 10|   9CU|    EYP|   1.0|   0.0|           0.0|      2.0|             291.2|
    |       20|2010|  5|   CLO|    UIB|  12.0| 384.0|       19800.0|    198.0|             902.0|
    +---------+----+---+------+-------+------+------+--------------+---------+------------------+
    only showing top 20 rows
    



```python
df_vuelos = df_vuelos.withColumn("Id_Vuelos", f.row_number().over(w)).select('Id_vuelos',
 'ano',
 'mes',
 'origen',
 'destino',
 'vuelos',
 'sillas',
 'carga_ofrecida',
 'pasajeros',
 'carga_bordo')
```


```python
df_vuelos.columns
```




    ['Id_vuelos',
     'ano',
     'mes',
     'origen',
     'destino',
     'vuelos',
     'sillas',
     'carga_ofrecida',
     'pasajeros',
     'carga_bordo']




```python
### No se permiten valores nulos o vacíos en las columnas 'ano', 'mes', 'origen', 'destino'.
### En el perfilamiento se observó que no existen valores faltantes para dichas columnas.
```


```python
df_vuelos.show()
```

    +---------+----+---+------+-------+------+------+--------------+---------+------------------+
    |Id_vuelos| ano|mes|origen|destino|vuelos|sillas|carga_ofrecida|pasajeros|       carga_bordo|
    +---------+----+---+------+-------+------+------+--------------+---------+------------------+
    |        1|2013|  5|   BOG|    AUC|   1.0|   0.0|           0.0|      8.0|               0.0|
    |        2|2011|  8|   7GC|    EYP|   1.0|   0.0|           0.0|      2.0|              36.4|
    |        3|2010|  5|   VVC|    BMG|   nan|   nan|           0.0|      nan|             328.9|
    |        4|2013|  4|   NVA|    BOG|  59.0|   nan|       23600.0|   1468.0|            2068.0|
    |        5|2010|  7|   CLO|    TCO|   8.0| 256.0|        6000.0|    149.0|            1828.0|
    |        6|2011| 11|   PUU|    CLO|   1.0|   0.0|           0.0|      4.0|               0.0|
    |        7|2012| 11|   BOG|    RVE|   1.0|   0.0|           0.0|      nan|            2000.0|
    |        8|2013| 12|   NVA|    PTL|   2.0|   0.0|           0.0|      3.0|              50.0|
    |        9|2013|  3|   PZA|    EYP|   1.0|   0.0|           0.0|      2.0|               0.0|
    |       10|2013|  9|   CLO|    TCO|   4.0|   0.0|           0.0|      nan|             400.0|
    |       11|2013|  8|   BUN|    BUN|   1.0|   nan|           0.0|      4.0|               0.0|
    |       12|2011|  1|   MVP|    BOG|   4.0| 304.0|        4800.0|    153.0|3942.1200000000003|
    |       13|2010|  4|   PVA|    ADZ|  67.0|1273.0|        8375.0|    861.0|31063.760000000002|
    |       14|2010| 11|   CZU|    BOG|  24.0|1824.0|       57600.0|   1409.0|             562.0|
    |       15|2013|  8|   IPI|    CLO|   2.0|   nan|           0.0|      7.0|             291.2|
    |       16|2011|  6|   SVI|    NVA|   2.0|   0.0|           0.0|      4.0|             200.0|
    |       17|2011|  5|   CUC|    BOG|  30.0|   nan|        8129.0|      nan|            152.88|
    |       18|2011|  8|   BOG|    EYP|   8.0| 400.0|        9600.0|    216.0|             195.0|
    |       19|2011| 10|   9CU|    EYP|   1.0|   0.0|           0.0|      2.0|             291.2|
    |       20|2010|  5|   CLO|    UIB|  12.0| 384.0|       19800.0|    198.0|             902.0|
    +---------+----+---+------+-------+------+------+--------------+---------+------------------+
    only showing top 20 rows
    


##### df_aeropuertos


```python
print('Se eliminarán', df_aeropuertos.count() - df_aeropuertos.dropDuplicates().count(),'registros duplicados en df_aeropuertos')
```

    Se eliminarán 79 registros duplicados en df_aeropuertos



```python
df_aeropuertos = df_aeropuertos.dropDuplicates()
```


```python
df_aeropuertos.distinct().count()
```




    213




```python
df_aeropuertos = df_aeropuertos.withColumn("Id_Aeropuerto", f.row_number().over(w))
```


```python
df_aeropuertos = df_aeropuertos.select('Id_Aeropuerto','sigla','nombre','municipio','departamento','categoria','tipo')
```


```python
df_aeropuertos.show()
```

    +-------------+-----+--------------------+---------------+---------------+-------------+----------+
    |Id_Aeropuerto|sigla|              nombre|      municipio|   departamento|    categoria|      tipo|
    +-------------+-----+--------------------+---------------+---------------+-------------+----------+
    |            1|  7GN|          LOS MANGOS|         Ayapel|        Córdoba|    Aeródromo|   Privado|
    |            2|  BEC|            BECERRIL|       Becerril|          Cesar|    Aeródromo|Fumigación|
    |            3|  7HE|GUACHARACAS (COLO...|        Beltrán|   Cundinamarca|    Aeródromo|   Privado|
    |            4|  9BP|     EL CONCHAL - CA|     El Cerrito|Valle del Cauca|    Aeródromo|   Privado|
    |            5|  9AD|         URACA - CA.|Puerto Colombia|      Atlántico|    Aeródromo|   Privado|
    |            6|  9DI|          SAN FELIPE|     San Felipe|        Guainía|    Aeródromo|   Público|
    |            7|  MTR|        LOS GARZONES|       Montería|        Córdoba|     Nacional| Aerocivil|
    |            8|  9BE|           LAS VEGAS| Paz de Ariporo|       Casanare|    Aeródromo|   Privado|
    |            9|  BNI|              BOLUGA|      Venadillo|         Tolima|    Aeródromo|Fumigación|
    |           10|  9LQ|        LA PONDEROSA|  Puerto Gaitán|           Meta|    Aeródromo|   Privado|
    |           11|  9AO|         SANTA CLARA|    Sabanalarga|       Casanare|    Aeródromo|Fumigación|
    |           12|  ARO|           ARBOLETES|      Arboletes|      Antioquia|    Aeródromo|   Público|
    |           13|  7HN|             ARMENIA| Paz de Ariporo|       Casanare|    Aeródromo|   Privado|
    |           14|  NQU|       REYES MURILLO|          Nuquí|          Chocó|     Regional| Aerocivil|
    |           15|  VGP|         CANANGUCHAL|    Villagarzón|       Putumayo|     Regional| Aerocivil|
    |           16|  9LH|    INGENIO PICHICHI|        Guacarí|Valle del Cauca|    Aeródromo|Fumigación|
    |           17|  AFI|              EL RIO|         Amalfi|      Antioquia|    Aeródromo| Aerocivil|
    |           18|  RCH|   ALMIRANTE PADILLA|       Riohacha|     La Guajira|Internacional| Aerocivil|
    |           19|  9MR|          LOS GANSOS|       Cumaribo|        Vichada|    Aeródromo|Fumigación|
    |           20|  9MM|           EL CARIBE| Paz de Ariporo|       Casanare|    Aeródromo|Fumigación|
    +-------------+-----+--------------------+---------------+---------------+-------------+----------+
    only showing top 20 rows
    



```python
## No se permiten valores nulos o filas vacías en las columnas 'Sigla' y 'Municipio'
## En el perfilamiento se observó que no hace falta registros en estas columnas.
```

#### Se realiza transformación para obtener el IdAeropuerto en df_vuelos


```python
df_vuelos = df_vuelos.join(df_aeropuertos.selectExpr('sigla as origen', 'Id_Aeropuerto'), how = 'left', on = 'origen' ).selectExpr('Id_vuelos','ano as Ano','mes as Mes','Id_Aeropuerto as IdAer_Orgn', 'destino','vuelos as Vuelos','sillas as Sillas','carga_ofrecida as CargaDisp','pasajeros as Pasajeros','carga_bordo as CargaBord')
```


```python
df_vuelos = df_vuelos.join(df_aeropuertos.selectExpr('sigla as destino', 'Id_Aeropuerto'), how = 'left', on = 'destino' ).selectExpr('Id_vuelos','Ano','Mes','IdAer_Orgn','Id_Aeropuerto as IdAer_Dstn','Vuelos','Sillas','CargaDisp','Pasajeros','CargaBord')
```


```python
df_vuelos= df_vuelos.selectExpr('Id_vuelos',
 'Ano',
 'Mes as IdMes',
 'IdAer_Orgn',
 'IdAer_Dstn',
 'Vuelos',
 'Sillas',
 'CargaDisp',
 'Pasajeros',
 'CargaBord')
```


```python
df_vuelos.show()
```

    +---------+----+-----+----------+----------+------+------+---------+---------+------------------+
    |Id_vuelos| Ano|IdMes|IdAer_Orgn|IdAer_Dstn|Vuelos|Sillas|CargaDisp|Pasajeros|         CargaBord|
    +---------+----+-----+----------+----------+------+------+---------+---------+------------------+
    |        1|2013|    5|       119|        95|   1.0|   0.0|      0.0|      8.0|               0.0|
    |        2|2011|    8|       146|       193|   1.0|   0.0|      0.0|      2.0|              36.4|
    |        3|2010|    5|        24|       122|   nan|   nan|      0.0|      nan|             328.9|
    |        4|2013|    4|       198|       119|  59.0|   nan|  23600.0|   1468.0|            2068.0|
    |        5|2010|    7|       183|        25|   8.0| 256.0|   6000.0|    149.0|            1828.0|
    |        6|2011|   11|        77|       183|   1.0|   0.0|      0.0|      4.0|               0.0|
    |        7|2012|   11|       119|        80|   1.0|   0.0|      0.0|      nan|            2000.0|
    |        8|2013|   12|       198|       118|   2.0|   0.0|      0.0|      3.0|              50.0|
    |        9|2013|    3|        91|       193|   1.0|   0.0|      0.0|      2.0|               0.0|
    |       10|2013|    9|       183|        25|   4.0|   0.0|      0.0|      nan|             400.0|
    |       11|2013|    8|        74|        74|   1.0|   nan|      0.0|      4.0|               0.0|
    |       12|2011|    1|       194|       119|   4.0| 304.0|   4800.0|    153.0|3942.1200000000003|
    |       13|2010|    4|       140|        32|  67.0|1273.0|   8375.0|    861.0|31063.760000000002|
    |       14|2010|   11|        78|       119|  24.0|1824.0|  57600.0|   1409.0|             562.0|
    |       15|2013|    8|        64|       183|   2.0|   nan|      0.0|      7.0|             291.2|
    |       16|2011|    6|        89|       198|   2.0|   0.0|      0.0|      4.0|             200.0|
    |       17|2011|    5|       178|       119|  30.0|   nan|   8129.0|      nan|            152.88|
    |       17|2011|    5|       124|       119|  30.0|   nan|   8129.0|      nan|            152.88|
    |       18|2011|    8|       119|       193|   8.0| 400.0|   9600.0|    216.0|             195.0|
    |       19|2011|   10|        79|       193|   1.0|   0.0|      0.0|      2.0|             291.2|
    +---------+----+-----+----------+----------+------+------+---------+---------+------------------+
    only showing top 20 rows
    


##### df_cobertura


```python
print(df_cobertura.count() - df_cobertura.dropDuplicates().count(),'registros duplicados fueron encontrados en df_cobertura')
```

    0 registros duplicados fueron encontrados en df_cobertura



```python
## No se permiten valores nulos o filas vacías en las columnas 'Centro Poblado'
```


```python
# Se agrega la columna Id_CentroPoblado y se cambia el nombre de las columnas 'Centro Poblado' y 'Distancia(Km)' para no generar errores
df_cobertura = df_cobertura.withColumn("Id_CentroPoblado", f.row_number().over(w))#.selectExpr('Id_CentroPoblado','`Centro Poblado` as CentroPoblado', 'Aeropuerto', '`Distancia(Km)` as Distancia', 'Cobertura', 'Aerodromo','D_Aerodromo', 'Regional', 'D_Regional', 'Nacional', 'D_Nacional', 'Internacional', 'D_Internacional', 'Tipo_Cobertura')
```


```python
df_cobertura.columns
```




    ['Id_CentroPoblado',
     'CentroPoblado',
     'Id_Nearest',
     'D_Nearest',
     'Cobertura',
     'Id_Aerodromo',
     'D_Aerodromo',
     'Id_Regional',
     'D_Regional',
     'Id_Nacional',
     'D_Nacional',
     'Id_Internacional',
     'D_Internacional',
     'Tipo_Cobertura']




```python
df_cobertura = df_cobertura.join(df_aeropuertos.selectExpr('nombre as Aeropuerto', 'Id_Aeropuerto'), how = 'left', on = 'Aeropuerto' ).selectExpr('Id_CentroPoblado',
 'CentroPoblado',
 'Id_Aeropuerto as Id_Nearest' ,
 'Distancia as Dist_Nearest',
 'Cobertura',
 'Aerodromo',
 'D_Aerodromo',
 'Regional',
 'D_Regional',
 'Nacional',
 'D_Nacional',
 'Internacional',
 'D_Internacional',
 'Tipo_Cobertura')
```


```python
df_cobertura = df_cobertura.join(df_aeropuertos.selectExpr('nombre as Aerodromo', 'Id_Aeropuerto'), how = 'left', on = 'Aerodromo' ).selectExpr('Id_CentroPoblado',
 'CentroPoblado',
 'Id_Nearest' ,
 'Dist_Nearest as D_Nearest',
 'Cobertura',
 'Id_Aeropuerto as Id_Aerodromo',
 'D_Aerodromo',
 'Regional',
 'D_Regional',
 'Nacional',
 'D_Nacional',
 'Internacional',
 'D_Internacional',
 'Tipo_Cobertura')
```


```python
df_cobertura = df_cobertura.join(df_aeropuertos.selectExpr('nombre as Regional', 'Id_Aeropuerto'), how = 'left', on = 'Regional' ).selectExpr('Id_CentroPoblado',
 'CentroPoblado',
 'Id_Nearest' ,
 'D_Nearest',
 'Cobertura',
 'Id_Aerodromo',
 'D_Aerodromo',
 'Id_Aeropuerto as Id_Regional',
 'D_Regional',
 'Nacional',
 'D_Nacional',
 'Internacional',
 'D_Internacional',
 'Tipo_Cobertura')
```


```python
df_cobertura = df_cobertura.join(df_aeropuertos.selectExpr('nombre as Nacional', 'Id_Aeropuerto'), how = 'left', on = 'Nacional' ).selectExpr('Id_CentroPoblado',
 'CentroPoblado',
 'Id_Nearest' ,
 'D_Nearest',
 'Cobertura',
 'Id_Aerodromo',
 'D_Aerodromo',
 'Id_Regional',
 'D_Regional',
 'Id_Aeropuerto as Id_Nacional',
 'D_Nacional',
 'Internacional',
 'D_Internacional',
 'Tipo_Cobertura')
```


```python
df_cobertura = df_cobertura.join(df_aeropuertos.selectExpr('nombre as Internacional', 'Id_Aeropuerto'), how = 'left', on = 'Internacional' ).selectExpr('Id_CentroPoblado',
 'CentroPoblado',
 'Id_Nearest' ,
 'D_Nearest',
 'Cobertura',
 'Id_Aerodromo',
 'D_Aerodromo',
 'Id_Regional',
 'D_Regional',
 'Id_Nacional',
 'D_Nacional',
 'Id_Aeropuerto as Id_Internacional',
 'D_Internacional',
 'Tipo_Cobertura')
```


```python
df_cobertura.show()
```

    +----------------+-------------+----------+----------------+---------+------------+----------------+-----------+----------------+-----------+----------------+----------------+----------------+--------------+
    |Id_CentroPoblado|CentroPoblado|Id_Nearest|       D_Nearest|Cobertura|Id_Aerodromo|     D_Aerodromo|Id_Regional|      D_Regional|Id_Nacional|      D_Nacional|Id_Internacional| D_Internacional|Tipo_Cobertura|
    +----------------+-------------+----------+----------------+---------+------------+----------------+-----------+----------------+-----------+----------------+----------------+----------------+--------------+
    |               1|    ABASTICOS|       119|34.4490837685403|     True|         209|36.2106614000918|        187|87.8515968551185|         24|51.6883416180031|             119|34.4490837685403| Internacional|
    |               2|     ACAPULCO|        46|3.30402959375963|     True|          46|3.30402959375963|         80| 141.02501304312|        159|74.4304171986738|            null|14.2111163795027| Internacional|
    |               3|      ACEVEDO|       118|22.6532566970602|     True|        null|43.6493625266892|        118|22.6532566970602|        169|43.3237402716814|             198|143.390689536798|      Regional|
    |               4| BARRO BLANCO|       180|25.3697141388984|     True|         180|25.3697141388984|         78|71.8220061729186|          7|144.989267076566|             100|152.243581933441|     Aeródromo|
    |               5|     EL CERRO|        56|38.0781434211722|     True|        null|71.3016456197922|        161|109.195513154648|         56|38.0781434211722|             183|240.676994865676|      Nacional|
    |               6|  AGUA BONITA|       209|7.73050896203633|     True|         209|7.73050896203633|       null|54.7579608123635|         24|84.3176710618212|             119|36.1004278601308| Internacional|
    |               7|      EL PLAN|       185|23.2148253274297|     True|        null|31.9749980192931|       null|189.831570369287|        185|23.2148253274297|              18|119.388680858664|      Nacional|
    |               8|   AGUA LINDA|       124|16.4026289132759|     True|        null|43.6742546493805|        181|110.475454725398|        159|165.537513072767|             124|16.4026289132759| Internacional|
    |               9|   AGUA LINDA|       124|16.4026289132759|     True|        null|43.6742546493805|         82|110.475454725398|        159|165.537513072767|             124|16.4026289132759| Internacional|
    |              10|     AGUANICA|        67|13.6553491468359|     True|          67|13.6553491468359|        121|97.9296964949602|         24|97.5521278643301|             119|28.1545846028419| Internacional|
    |              11|      AIMARAL|        18|14.5714039199428|     True|        null|44.5168192871937|       null|118.520239172684|        185|129.317355513779|              18|14.5714039199428| Internacional|
    |              12|     EL LLANO|        74|9.92784300922365|     True|        null|61.7323490394532|       null|95.7008327445712|         74|9.92784300922365|             183|72.4122624243147|      Nacional|
    |              13|       ALASKA|        48|13.1830073258331|     True|          48|13.1830073258331|       null|23.0006265111197|         74|90.0260593587472|             183|44.6935474996501|      Regional|
    |              14| LAS CAMELIAS|       186| 14.895444916699|     True|         186| 14.895444916699|        195|46.7086926843324|        193|99.6661947344119|            null|190.517179830657|     Aeródromo|
    |              15|   LAS CANOAS|        29|34.4728908947269|     True|          29|34.4728908947269|         78|132.441246675237|        185|137.994259877552|            null|79.4776232471418|     Aeródromo|
    |              16|   ALEJANDRÍA|        29|25.8580598699215|     True|          29|25.8580598699215|         78|145.491742855807|        185|109.256012494505|            null|131.095226009146|     Aeródromo|
    |              17|      ALTAMAR|        67|15.4248886590723|     True|          67|15.4248886590723|       null|106.325204614854|         24|72.8907549069129|             119|21.7622326343405| Internacional|
    |              18|     ALTAMIRA|        46|13.7427968187756|     True|          46|13.7427968187756|         80|129.290417558113|        159|87.1391858081435|            null|30.1736314786314| Internacional|
    |              19|  PIEDECUESTA|       121|13.5159849488022|     True|        null|22.9270547733822|        121|13.5159849488022|       null|56.3136993029632|            null| 92.269217233967|      Regional|
    |              20|       AMALFI|        17|4.29901138791201|     True|          17|4.29901138791201|        184|41.9482926846517|       null|95.2190706770922|            null|91.2632551082593|     Aeródromo|
    +----------------+-------------+----------+----------------+---------+------------+----------------+-----------+----------------+-----------+----------------+----------------+----------------+--------------+
    only showing top 20 rows
    


### L. Carga de información (exportación a .CSV)


```python
pathf = './InfraestructuraVisible/Transformed/'
```


```python
#Bloque 1: exporta la dimensión de Aeropuertos
df_aeropuertos.toPandas().to_csv(pathf+'Aeropuertos_F.csv')
```


```python
#Bloque 2: exporta la tabla de hechos de Vuelos
df_vuelos.toPandas().to_csv(pathf+'Vuelos_F.csv')
```


```python
#Bloque 3: exporta la dimensión Meses 
df_mes.toPandas().to_csv(pathf+'Meses_F.csv')
```


```python
#Bloque 4: exporta la dimensión Cobertura 
df_cobertura.select('Id_CentroPoblado',
 'CentroPoblado',
 'Id_Nearest',
 'D_Nearest',
 'Id_Aerodromo',
 'D_Aerodromo',
 'Id_Regional',
 'D_Regional',
 'Id_Nacional',
 'D_Nacional',
 'Id_Internacional',
 'D_Internacional',
 'Tipo_Cobertura').toPandas().to_csv(pathf+'Cobertura_F.csv')
```

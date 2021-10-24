Tarea: Perfilamiento de datos
=============================

1. Importación de librerias necesarias
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: ipython3

    from pyspark.sql import SparkSession
    from pyspark.sql import functions
    from pyspark.sql.types import StructType
    from pyspark import SparkContext, SparkConf, SQLContext
    from pyspark.sql.types import FloatType
    from pyspark.sql.functions import udf

.. code:: ipython3

    ## ***Importar pandas***

.. code:: ipython3

    import pandas as pd
    import numpy as np

.. code:: ipython3

    import os 
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /usr/share/java/mariadb-java-client-2.5.3.jar pyspark-shell'

.. code:: ipython3

    #Configuración de la sesión
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    spark = sql_context.sparkSession

2. Carga de archivos
^^^^^^^^^^^^^^^^^^^^

.. code:: ipython3

    ##PATH = "./Tarea: Perfilamiento de datos/"

.. code:: ipython3

    aeropuertos_df = spark.read.load("aeropuertos.csv",format="csv", sep=",", inferSchema="true", header="true")
    cobertura_df = spark.read.load("Cobertura de Aerea de Centros Poblados por Categoria de Aeropuerto1.csv",format="csv", sep=",", inferSchema="true", header="true")
    distancias_df = spark.read.load("Matriz de distancias entre aeropuertos y centros poblados de Colombia1.csv",format="csv", sep=",", inferSchema="true", header="true")
    vuelos_df = spark.read.load("vuelos.csv",format="csv", sep=",", inferSchema="true", header="true")

3. Perfilamiento de datos
~~~~~~~~~~~~~~~~~~~~~~~~~

3.1 Aeropuertos
^^^^^^^^^^^^^^^

.. code:: ipython3

    aeropuertos_df.count()




.. parsed-literal::

    292



.. code:: ipython3

    len(aeropuertos_df.columns)




.. parsed-literal::

    23



.. code:: ipython3

    aeropuertos_df.columns




.. parsed-literal::

    ['_c0',
     'sigla',
     'iata',
     'nombre',
     'municipio',
     'departamento',
     'categoria',
     'latitud',
     'longitud',
     'propietario',
     'explotador',
     'longitud_pista',
     'ancho_pista',
     'pbmo',
     'elevacion',
     'resolucion',
     'fecha_construccion',
     'fecha_vigencia',
     'clase',
     'tipo',
     'numero_vuelos_origen',
     'gcd_departamento',
     'gcd_municipio']



.. code:: ipython3

    #aeropuertos_df.select('< Columna >').show()
    aeropuertos_df.select('_c0').distinct().show()

.. code:: ipython3

    aeropuertos_df.schema.fields




.. parsed-literal::

    [StructField(_c0,IntegerType,true),
     StructField(sigla,StringType,true),
     StructField(iata,StringType,true),
     StructField(nombre,StringType,true),
     StructField(municipio,StringType,true),
     StructField(departamento,StringType,true),
     StructField(categoria,StringType,true),
     StructField(latitud,DoubleType,true),
     StructField(longitud,DoubleType,true),
     StructField(propietario,StringType,true),
     StructField(explotador,StringType,true),
     StructField(longitud_pista,DoubleType,true),
     StructField(ancho_pista,DoubleType,true),
     StructField(pbmo,StringType,true),
     StructField(elevacion,DoubleType,true),
     StructField(resolucion,StringType,true),
     StructField(fecha_construccion,StringType,true),
     StructField(fecha_vigencia,StringType,true),
     StructField(clase,StringType,true),
     StructField(tipo,StringType,true),
     StructField(numero_vuelos_origen,StringType,true),
     StructField(gcd_departamento,IntegerType,true),
     StructField(gcd_municipio,IntegerType,true)]



.. code:: ipython3

    # Nombre de la columna

.. code:: ipython3

    aeropuertos_df.schema.fieldNames()[0]




.. parsed-literal::

    '_c0'



.. code:: ipython3

    aeropuertos_df.schema[1].name




.. parsed-literal::

    'sigla'



.. code:: ipython3

    # Tipo de dato

.. code:: ipython3

    print(aeropuertos_df.schema[1].dataType)


.. parsed-literal::

    StringType


.. code:: ipython3

    # Número de datos faltantes.

.. code:: ipython3

    aeropuertos_df.where(aeropuertos_df['fecha_vigencia'] == 'nan').count()




.. parsed-literal::

    221



.. code:: ipython3

    # Obtener valores, media, mediana, min, max...

.. code:: ipython3

    aeropuertos_df.agg({'longitud' : 'mean'}).collect()[0][0]




.. parsed-literal::

    -73.24611404109585



.. code:: ipython3

    aeropuertos_df.select('ancho_pista').summary().collect()




.. parsed-literal::

    [Row(summary='count', ancho_pista='292'),
     Row(summary='mean', ancho_pista='17.633561643835616'),
     Row(summary='stddev', ancho_pista='10.356164406912008'),
     Row(summary='min', ancho_pista='10.0'),
     Row(summary='25%', ancho_pista='10.0'),
     Row(summary='50%', ancho_pista='15.0'),
     Row(summary='75%', ancho_pista='20.0'),
     Row(summary='max', ancho_pista='75.0')]



.. code:: ipython3

    StringType = aeropuertos_df.schema[1].dataType

.. code:: ipython3

    # Valores únicos

.. code:: ipython3

    aeropuertos_df.select('departamento').distinct().count()




.. parsed-literal::

    30



.. code:: ipython3

    for i in aeropuertos_df.schema.fields:
        
        name = i.name
        tipo = i.dataType
        falt = aeropuertos_df.where(aeropuertos_df[i.name] == 'nan').count()
        mini = aeropuertos_df.select(i.name).summary().collect()[3][1]
        maxi = aeropuertos_df.select(i.name).summary().collect()[7][1]
        P50  = aeropuertos_df.select(i.name).summary().collect()[5][1]
        mean = aeropuertos_df.select(i.name).summary().collect()[1][1]
        stdv = aeropuertos_df.select(i.name).summary().collect()[2][1]
        
        val_unic = aeropuertos_df.select(i.name).distinct().count()
        
        if i.dataType == StringType:
        
            print ('|',name,'|',tipo,'|','Categórico','|',falt,'|','-','|','-','|','-','|','-','|','-','|',val_unic,'|')
            
        else: 
            
            print ('|',name,'|',tipo,'|','Numérico','|',falt,'|',mini,'|',maxi,'|',P50,'|',mean,'|',stdv,'|',val_unic,'|')
        
        
        
        


.. parsed-literal::

    | _c0 | IntegerType | Numérico | 0 | 2 | 862 | 177 | 274.26027397260276 | 248.70325006946265 | 213 |
    | sigla | StringType | Categórico | 0 | - | - | - | - | - | 212 |
    | iata | StringType | Categórico | 209 | - | - | - | - | - | 62 |
    | nombre | StringType | Categórico | 0 | - | - | - | - | - | 206 |
    | municipio | StringType | Categórico | 0 | - | - | - | - | - | 122 |
    | departamento | StringType | Categórico | 0 | - | - | - | - | - | 30 |
    | categoria | StringType | Categórico | 0 | - | - | - | - | - | 4 |
    | latitud | DoubleType | Numérico | 0 | -0.7831 | 13.3572 | 5.2993 | 5.543335616438355 | 2.3826685454671885 | 212 |
    | longitud | DoubleType | Numérico | 0 | -81.7113 | -67.0776 | -73.0583 | -73.24611404109585 | 2.2960739088619917 | 212 |
    | propietario | StringType | Categórico | 3 | - | - | - | - | - | 121 |
    | explotador | StringType | Categórico | 0 | - | - | - | - | - | 103 |
    | longitud_pista | DoubleType | Numérico | 0 | 200.0 | 3800.0 | 800.0 | 962.445205479452 | 540.0191104015921 | 98 |
    | ancho_pista | DoubleType | Numérico | 0 | 10.0 | 75.0 | 15.0 | 17.633561643835616 | 10.356164406912008 | 22 |
    | pbmo | StringType | Categórico | 55 | - | - | - | - | - | 37 |
    | elevacion | DoubleType | Numérico | 0 | 0.0 | 9740.0 | 594.0 | 1073.3082191780823 | 1580.8586233906904 | 183 |
    | resolucion | StringType | Categórico | 6 | - | - | - | - | - | 166 |
    | fecha_construccion | StringType | Categórico | 0 | - | - | - | - | - | 194 |
    | fecha_vigencia | StringType | Categórico | 221 | - | - | - | - | - | 56 |
    | clase | StringType | Categórico | 0 | - | - | - | - | - | 15 |
    | tipo | StringType | Categórico | 0 | - | - | - | - | - | 4 |
    | numero_vuelos_origen | StringType | Categórico | 71 | - | - | - | - | - | 187 |
    | gcd_departamento | IntegerType | Numérico | 0 | 5 | 99 | 73 | 63.03767123287671 | 27.46134963485643 | 30 |
    | gcd_municipio | IntegerType | Numérico | 0 | 5031 | 99773 | 73275 | 63341.63698630137 | 27463.258042601276 | 122 |


3.2 Cobertura de Aerea de Centros Poblados por Categoria de Aeropuerto1
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: ipython3

    cobertura_df




.. parsed-literal::

    DataFrame[Centro Poblado: string, Aeropuerto: string, Distancia(Km): double, Cobertura: boolean, Aerodromo: string, D_Aerodromo: double, Regional: string, D_Regional: double, Nacional: string, D_Nacional: double, Internacional: string, D_Internacional: double, Tipo_Cobertura: string]



.. code:: ipython3

    cobertura_df.count()




.. parsed-literal::

    2417



.. code:: ipython3

    len(cobertura_df.columns)




.. parsed-literal::

    13



.. code:: ipython3

    cobertura_df.columns




.. parsed-literal::

    ['Centro Poblado',
     'Aeropuerto',
     'Distancia(Km)',
     'Cobertura',
     'Aerodromo',
     'D_Aerodromo',
     'Regional',
     'D_Regional',
     'Nacional',
     'D_Nacional',
     'Internacional',
     'D_Internacional',
     'Tipo_Cobertura']



.. code:: ipython3

    cobertura_df.select('Tipo_Cobertura').show()


.. parsed-literal::

    +--------------+
    |Tipo_Cobertura|
    +--------------+
    | Internacional|
    | Internacional|
    |      Regional|
    |     Aeródromo|
    |      Nacional|
    | Internacional|
    |      Nacional|
    | Internacional|
    | Internacional|
    | Internacional|
    |      Nacional|
    |      Regional|
    |     Aeródromo|
    |     Aeródromo|
    |     Aeródromo|
    | Internacional|
    | Internacional|
    |      Regional|
    |     Aeródromo|
    | Internacional|
    +--------------+
    only showing top 20 rows
    


.. code:: ipython3

    cobertura_df.select('Tipo_Cobertura').distinct().count()




.. parsed-literal::

    5



.. code:: ipython3

    #cobertura_df.select('<>').summary().collect()

.. code:: ipython3

    cobertura_df.schema[3].dataType




.. parsed-literal::

    BooleanType



.. code:: ipython3

    BoolType = cobertura_df.schema[3].dataType

.. code:: ipython3

    for i in cobertura_df.schema.fields:
        
        if i.dataType == StringType or i.dataType == BoolType:
        
            mini = '-'
            maxi = '-'
            P50  = '-'
            mean = '-'
            stdv = '-'    
        else:
            mini = cobertura_df.select(i.name).summary().collect()[3][1]
            maxi = cobertura_df.select(i.name).summary().collect()[7][1]
            P50  = cobertura_df.select(i.name).summary().collect()[5][1]
            mean = cobertura_df.select(i.name).summary().collect()[1][1]
            stdv = cobertura_df.select(i.name).summary().collect()[2][1]
    
        falt = cobertura_df.where(cobertura_df[i.name] == 'nan').count()
        val_unic = cobertura_df.select(i.name).distinct().count()
        name = i.name
        tipo = i.dataType
    
        print ('|',name,'|',tipo,'|',falt,'|',(mini),'|',maxi,'|',P50,'|',mean,'|',stdv,'|',val_unic,'|')
        
        
        
        


.. parsed-literal::

    | Centro Poblado | StringType | 0 | - | - | - | - | - | 2417 |
    | Aeropuerto | StringType | 0 | - | - | - | - | - | 135 |
    | Distancia(Km) | DoubleType | 0 | 0.0223630141858667 | 105.430351585437 | 20.0143385729883 | 23.35469915818823 | 16.00602068713784 | 2368 |
    | Cobertura | BooleanType | 0 | - | - | - | - | - | 2 |
    | Aerodromo | StringType | 0 | - | - | - | - | - | 155 |
    | D_Aerodromo | DoubleType | 0 | 0.0223630141858667 | 720.500617771555 | 32.0003044608048 | 40.77610106248731 | 50.619574902015096 | 2368 |
    | Regional | StringType | 0 | - | - | - | - | - | 32 |
    | D_Regional | DoubleType | 0 | 0.0230081279085742 | 686.988739337329 | 73.0073720277524 | 76.12477415752738 | 56.997530431276424 | 2368 |
    | Nacional | StringType | 0 | - | - | - | - | - | 18 |
    | D_Nacional | DoubleType | 0 | 0.282750026371487 | 578.530200301063 | 65.9205507029441 | 75.47305009913859 | 54.394503550965574 | 2368 |
    | Internacional | StringType | 0 | - | - | - | - | - | 12 |
    | D_Internacional | DoubleType | 0 | 1.68138609948784 | 716.678974118283 | 129.797021410875 | 145.173129540198 | 101.13328073575107 | 2368 |
    | Tipo_Cobertura | StringType | 0 | - | - | - | - | - | 5 |


3.3 Matriz de distancias entre aeropuertos y centros poblados de Colombia1.csv
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: ipython3

    distancias_df




.. parsed-literal::

    DataFrame[Unnamed: 0: string, LA ESCONDIDA: double, MORICHITO: double, CAROLINA DEL PRINCIPE: double, DUBAI: double, BARU - HIDROPUERTO: double, LA CAROLINA: string, SAN FELIPE DEL PAUTO: double, VELASQUEZ: string, LA UNION: double, LA ILUSION: double, LA VENTUROSA: double, GUAYABAL DEL CRAVO: double, LAS VIOLETAS- CA: double, LOS MANGOS: double, EL CONDOR: double, HOTEL SAN DIEGO: double, EL CAFUCHE: double, GUACHARACAS (COLOMBAIMA): double, GAVILAN DE LA PASCUA: double, DOROTEA B1: double, HORIZONTES: double, MACOLLA: double, MULETOS- CA: double, LA MAPORA: double, LLANO CAUCHO: double, ARMENIA: double, RANCHO COLIBRI - CA: double, EL CAIRANO: double, NUEVA ROMA: double, JAGUAR: double, OCELOTE: double, GETSEMANI: double, URACA - CA.: double, SAN ROQUE  - CA.: double, SAN LUIS DE PACA: double, CANANARI: double, MARAREY: double, SANTA CLARA: double, SAN PABLO: double, COROCITO: double, LAS FURIAS: double, EL NOGAL: double, SANTA CRUZ: double, CURUMANI: string, SAN MIGUEL: string, CANTADELICIAS: double, LAS VEGAS: double, LA FRANCIA: double, SAN FELIPE: string, JULIAN: double, LOS HALCONES: string, EL SOÑADOR: double, EL CONCHAL - CA: double, SEVILLA: double, ASA SAN MARTIN: double, LA FAZENDA: double, LA ESTRELLA-CA.: double, LAS AGUILAS -CA: double, CAMARUCOS: double, EL TOTUMO: double, VARSOVIA: double, LA HERMOSA: double, SAN ESTEBAN: double, SAN JOSE DEL ARIPORO: double, COLINERAS: double, LA CAIMANA: double, EMAUS: double, SANTA MARIA DEL CAFÉ: double, MIRAMAR DE GUANAPALO: double, LA SALVACION: double, LOMA GRANDE - CA: double, EL CAPRICHO: double, COROCORA: double, GRISMANIA: double, PALMAS DE TUMACO  -CA: double, INGENIO LA CARMELITA: double, EL LAGO - CA: double, HACIENDA LA JOYA: double, LA PASTORA: double, TALANQUERA: double, VILLA GEORGINA: double, INGENIO PICHICHI: double, SAN SEBASTIAN: double, EL COROZO: double, HATO VIEJO: double, GUADUALITO: string, LA REDENCION: double, LA PONDEROSA: string, LOS REMANSOS: double, SAN LUIS: string, LOS GAVANES (LA MATA): double, EL RODEO: string, EL PALMAR: double, LA GAITANA: double, SAN ISIDRO II: double, EL DELIRIO: double, LA MONICA: double, LA ABEJITA: double, CACHIMBALITO - CA: double, MADREVIEJA: double, SAN JUAN: double, LA FORTUNA: double, AGROFORESTAL MATA AZUL: double, VILLA ISABELLA: double, MIRADOR: double, EL CARIBE: double, EL PEDRAL BONANZA: double, SAN NICOLAS: double, LOS LOBOS: double, EL RASTRO: double, LOS GANSOS: double, BETANIA: double, AGUAS CLARAS: double, LAS NUBES: double, CAMPO ALEGRE: string, CAÑO COLORADO: double, FORTUL: double, AGUA BLANCA: string, ALCIDES FERNANDEZ: double, EL MONASTERIO: double, AGUACLARA: double, ACAPULCO: double, ARARACUARA: double, GUSTAVO ROJAS PINILLA: double, AEROFLANDES - C.A.: double, EL RIO: double, HACARITAMA: double, MARIA ANGELICA: double, EL DIAMANTE: double, ANTONIO ROLDAN BETANCOURT: double, ARBOLETES: double, EL TRONCAL: double, ATACO: double, SANTIAGO PEREZ QUIROZ: double, EL CORAJE: string, INGENIO RISARALDA: double, BANCO LARGO: double, GUAICARAMO: double, PIZARRO: double, BECERRIL: string, BERASTEGUI: double, BIZERTA: double, BARRANCO MINAS: double, BOLUGA: double, BUENOS AIRES: double, BUENOS AIRES -FADELCE: double, EL DORADO: double, BUENAVENTURA-GERARDO TOBAR LOPEZ: double, ALFONSO BONILLA ARAGON: double, CONDOTO MANDINGA: double, NAVAS PARDO: double, RAFAEL NUÑEZ: double, CAMILO DAZA: double, CAMILO DAZA No.2: double, LAS BRUJAS: double, YARIGUIES: double, EL JUNCAL: double, LAS FLORES: double, EL ALCARAVAN: double, GUSTAVO ARTUNDUAGA PAREDES: double, JUAN CASIANO: double, FLAMINIO S. CAMACHO: double, HATO COROZAL: double, PERALES: double, JAIME ORTIZ BETANCUR: double, MIRAFLORES: double, BARACOA: double, SAN BERNARDO: double, JOSE CELESTINO: double, EL PINDO: double, LOS GARZONES: double, FABIO A. LEON BENTLEY: double, REYES MURILLO: double, BENITO SALAS: double, EL MEDANO: double, REMEDIOS OTU: double, GERMAN OLANO: double, PAIPA JUAN JOSE RONDON: double, GUILLERMO LEON VALENCIA: double, ANTONIO NARIÑO: double, CONTADOR: double, GEMELOS DORADOS: double, TRES DE MAYO: double, EL EMBRUJO: double, PAZ DE ARIPORO: double, CRAVO NORTE: double, ALMIRANTE PADILLA: double, COLONIZADORES: double, SIMON BOLIVAR: double, SAN MARTIN: double, EDUARDO FALLA SOLANO: double, LA FLORIDA: double, TRINIDAD: double, TOLU: double, GUSTAVO VARGAS: double, TABLON DE TAMARA: double, EL CARAÑO: double, ALI PIEDRAHITA: double, CANANGUCHAL: double, ALFONSO LOPEZ PUMAREJO: double, VANGUARDIA: double, SANTA ISABEL: double, YAPIMA: double, TROMPILLOS: double, GERMAN ALBERTO: double, GUILLERMO GOMEZ ORTIZ: double]



.. code:: ipython3

    print('Las dimensiones de la tabla son ', len(distancias_df.columns),' columnas y', distancias_df.count(), 'observaciones.')


.. parsed-literal::

    Las dimensiones de la tabla son  207  columnas y 5512 observaciones.


.. code:: ipython3

    distancias_df.columns




.. parsed-literal::

    ['Unnamed: 0',
     'LA ESCONDIDA',
     'MORICHITO',
     'CAROLINA DEL PRINCIPE',
     'DUBAI',
     'BARU - HIDROPUERTO',
     'LA CAROLINA',
     'SAN FELIPE DEL PAUTO',
     'VELASQUEZ',
     'LA UNION',
     'LA ILUSION',
     'LA VENTUROSA',
     'GUAYABAL DEL CRAVO',
     'LAS VIOLETAS- CA',
     'LOS MANGOS',
     'EL CONDOR',
     'HOTEL SAN DIEGO',
     'EL CAFUCHE',
     'GUACHARACAS (COLOMBAIMA)',
     'GAVILAN DE LA PASCUA',
     'DOROTEA B1',
     'HORIZONTES',
     'MACOLLA',
     'MULETOS- CA',
     'LA MAPORA',
     'LLANO CAUCHO',
     'ARMENIA',
     'RANCHO COLIBRI - CA',
     'EL CAIRANO',
     'NUEVA ROMA',
     'JAGUAR',
     'OCELOTE',
     'GETSEMANI',
     'URACA - CA.',
     'SAN ROQUE  - CA.',
     'SAN LUIS DE PACA',
     'CANANARI',
     'MARAREY',
     'SANTA CLARA',
     'SAN PABLO',
     'COROCITO',
     'LAS FURIAS',
     'EL NOGAL',
     'SANTA CRUZ',
     'CURUMANI',
     'SAN MIGUEL',
     'CANTADELICIAS',
     'LAS VEGAS',
     'LA FRANCIA',
     'SAN FELIPE',
     'JULIAN',
     'LOS HALCONES',
     'EL SOÑADOR',
     'EL CONCHAL - CA',
     'SEVILLA',
     'ASA SAN MARTIN',
     'LA FAZENDA',
     'LA ESTRELLA-CA.',
     'LAS AGUILAS -CA',
     'CAMARUCOS',
     'EL TOTUMO',
     'VARSOVIA',
     'LA HERMOSA',
     'SAN ESTEBAN',
     'SAN JOSE DEL ARIPORO',
     'COLINERAS',
     'LA CAIMANA',
     'EMAUS',
     'SANTA MARIA DEL CAFÉ',
     'MIRAMAR DE GUANAPALO',
     'LA SALVACION',
     'LOMA GRANDE - CA',
     'EL CAPRICHO',
     'COROCORA',
     'GRISMANIA',
     'PALMAS DE TUMACO  -CA',
     'INGENIO LA CARMELITA',
     'EL LAGO - CA',
     'HACIENDA LA JOYA',
     'LA PASTORA',
     'TALANQUERA',
     'VILLA GEORGINA',
     'INGENIO PICHICHI',
     'SAN SEBASTIAN',
     'EL COROZO',
     'HATO VIEJO',
     'GUADUALITO',
     'LA REDENCION',
     'LA PONDEROSA',
     'LOS REMANSOS',
     'SAN LUIS',
     'LOS GAVANES (LA MATA)',
     'EL RODEO',
     'EL PALMAR',
     'LA GAITANA',
     'SAN ISIDRO II',
     'EL DELIRIO',
     'LA MONICA',
     'LA ABEJITA',
     'CACHIMBALITO - CA',
     'MADREVIEJA',
     'SAN JUAN',
     'LA FORTUNA',
     'AGROFORESTAL MATA AZUL',
     'VILLA ISABELLA',
     'MIRADOR',
     'EL CARIBE',
     'EL PEDRAL BONANZA',
     'SAN NICOLAS',
     'LOS LOBOS',
     'EL RASTRO',
     'LOS GANSOS',
     'BETANIA',
     'AGUAS CLARAS',
     'LAS NUBES',
     'CAMPO ALEGRE',
     'CAÑO COLORADO',
     'FORTUL',
     'AGUA BLANCA',
     'ALCIDES FERNANDEZ',
     'EL MONASTERIO',
     'AGUACLARA',
     'ACAPULCO',
     'ARARACUARA',
     'GUSTAVO ROJAS PINILLA',
     'AEROFLANDES - C.A.',
     'EL RIO',
     'HACARITAMA',
     'MARIA ANGELICA',
     'EL DIAMANTE',
     'ANTONIO ROLDAN BETANCOURT',
     'ARBOLETES',
     'EL TRONCAL',
     'ATACO',
     'SANTIAGO PEREZ QUIROZ',
     'EL CORAJE',
     'INGENIO RISARALDA',
     'BANCO LARGO',
     'GUAICARAMO',
     'PIZARRO',
     'BECERRIL',
     'BERASTEGUI',
     'BIZERTA',
     'BARRANCO MINAS',
     'BOLUGA',
     'BUENOS AIRES',
     'BUENOS AIRES -FADELCE',
     'EL DORADO',
     'BUENAVENTURA-GERARDO TOBAR LOPEZ',
     'ALFONSO BONILLA ARAGON',
     'CONDOTO MANDINGA',
     'NAVAS PARDO',
     'RAFAEL NUÑEZ',
     'CAMILO DAZA',
     'CAMILO DAZA No.2',
     'LAS BRUJAS',
     'YARIGUIES',
     'EL JUNCAL',
     'LAS FLORES',
     'EL ALCARAVAN',
     'GUSTAVO ARTUNDUAGA PAREDES',
     'JUAN CASIANO',
     'FLAMINIO S. CAMACHO',
     'HATO COROZAL',
     'PERALES',
     'JAIME ORTIZ BETANCUR',
     'MIRAFLORES',
     'BARACOA',
     'SAN BERNARDO',
     'JOSE CELESTINO',
     'EL PINDO',
     'LOS GARZONES',
     'FABIO A. LEON BENTLEY',
     'REYES MURILLO',
     'BENITO SALAS',
     'EL MEDANO',
     'REMEDIOS OTU',
     'GERMAN OLANO',
     'PAIPA JUAN JOSE RONDON',
     'GUILLERMO LEON VALENCIA',
     'ANTONIO NARIÑO',
     'CONTADOR',
     'GEMELOS DORADOS',
     'TRES DE MAYO',
     'EL EMBRUJO',
     'PAZ DE ARIPORO',
     'CRAVO NORTE',
     'ALMIRANTE PADILLA',
     'COLONIZADORES',
     'SIMON BOLIVAR',
     'SAN MARTIN',
     'EDUARDO FALLA SOLANO',
     'LA FLORIDA',
     'TRINIDAD',
     'TOLU',
     'GUSTAVO VARGAS',
     'TABLON DE TAMARA',
     'EL CARAÑO',
     'ALI PIEDRAHITA',
     'CANANGUCHAL',
     'ALFONSO LOPEZ PUMAREJO',
     'VANGUARDIA',
     'SANTA ISABEL',
     'YAPIMA',
     'TROMPILLOS',
     'GERMAN ALBERTO',
     'GUILLERMO GOMEZ ORTIZ']



.. code:: ipython3

     distancias_df.select('CURUMANI').summary().show()


.. parsed-literal::

    +-------+--------+
    |summary|CURUMANI|
    +-------+--------+
    |  count|       0|
    |   mean|    null|
    | stddev|    null|
    |    min|    null|
    |    25%|    null|
    |    50%|    null|
    |    75%|    null|
    |    max|    null|
    +-------+--------+
    



.. code:: ipython3

    for i in distancias_df.columns:
        try:
            if distancias_df.select(i).summary().collect()[0][1] == 0:
                print('|',i,'|') 
        except:
            print('|',i,'|')
            pass


.. parsed-literal::

    | URACA - CA. |
    | SAN ROQUE  - CA. |
    | LA ESTRELLA-CA. |
    | AEROFLANDES - C.A. |
    | CAMILO DAZA No.2 |
    | FLAMINIO S. CAMACHO |
    | FABIO A. LEON BENTLEY |


.. code:: ipython3

    distancias_df.where(distancias_df['GUSTAVO VARGAS'].isNull()).show()


.. parsed-literal::

    +--------------------+------------+---------+---------------------+-----+------------------+-----------+--------------------+---------+--------+----------+------------+------------------+----------------+----------+---------+---------------+----------+------------------------+--------------------+----------+----------+-------+-----------+---------+------------+-------+-------------------+----------+----------+------+-------+---------+-----------+----------------+----------------+--------+-------+-----------+---------+--------+----------+--------+----------+--------+----------+-------------+---------+----------+----------+------+------------+----------+---------------+-------+--------------+----------+---------------+---------------+---------+---------+--------+----------+-----------+--------------------+---------+----------+-----+--------------------+--------------------+------------+----------------+-----------+--------+---------+---------------------+--------------------+------------+----------------+----------+----------+--------------+----------------+-------------+---------+----------+----------+------------+------------+------------+--------+---------------------+--------+---------+----------+-------------+----------+---------+----------+-----------------+----------+--------+----------+----------------------+--------------+-------+---------+-----------------+-----------+---------+---------+----------+-------+------------+---------+------------+-------------+------+-----------+-----------------+-------------+---------+--------+----------+---------------------+------------------+------+----------+--------------+-----------+-------------------------+---------+----------+-----+---------------------+---------+-----------------+-----------+----------+-------+--------+----------+-------+--------------+------+------------+---------------------+---------+--------------------------------+----------------------+----------------+-----------+------------+-----------+----------------+----------+---------+---------+----------+------------+--------------------------+------------+-------------------+------------+-------+--------------------+----------+-------+------------+--------------+--------+------------+---------------------+-------------+------------+---------+------------+------------+----------------------+-----------------------+--------------+--------+---------------+------------+----------+--------------+-----------+-----------------+-------------+-------------+----------+--------------------+----------+--------+----+--------------+----------------+---------+--------------+-----------+----------------------+----------+------------+------+----------+--------------+---------------------+
    |          Unnamed: 0|LA ESCONDIDA|MORICHITO|CAROLINA DEL PRINCIPE|DUBAI|BARU - HIDROPUERTO|LA CAROLINA|SAN FELIPE DEL PAUTO|VELASQUEZ|LA UNION|LA ILUSION|LA VENTUROSA|GUAYABAL DEL CRAVO|LAS VIOLETAS- CA|LOS MANGOS|EL CONDOR|HOTEL SAN DIEGO|EL CAFUCHE|GUACHARACAS (COLOMBAIMA)|GAVILAN DE LA PASCUA|DOROTEA B1|HORIZONTES|MACOLLA|MULETOS- CA|LA MAPORA|LLANO CAUCHO|ARMENIA|RANCHO COLIBRI - CA|EL CAIRANO|NUEVA ROMA|JAGUAR|OCELOTE|GETSEMANI|URACA - CA.|SAN ROQUE  - CA.|SAN LUIS DE PACA|CANANARI|MARAREY|SANTA CLARA|SAN PABLO|COROCITO|LAS FURIAS|EL NOGAL|SANTA CRUZ|CURUMANI|SAN MIGUEL|CANTADELICIAS|LAS VEGAS|LA FRANCIA|SAN FELIPE|JULIAN|LOS HALCONES|EL SOÑADOR|EL CONCHAL - CA|SEVILLA|ASA SAN MARTIN|LA FAZENDA|LA ESTRELLA-CA.|LAS AGUILAS -CA|CAMARUCOS|EL TOTUMO|VARSOVIA|LA HERMOSA|SAN ESTEBAN|SAN JOSE DEL ARIPORO|COLINERAS|LA CAIMANA|EMAUS|SANTA MARIA DEL CAFÉ|MIRAMAR DE GUANAPALO|LA SALVACION|LOMA GRANDE - CA|EL CAPRICHO|COROCORA|GRISMANIA|PALMAS DE TUMACO  -CA|INGENIO LA CARMELITA|EL LAGO - CA|HACIENDA LA JOYA|LA PASTORA|TALANQUERA|VILLA GEORGINA|INGENIO PICHICHI|SAN SEBASTIAN|EL COROZO|HATO VIEJO|GUADUALITO|LA REDENCION|LA PONDEROSA|LOS REMANSOS|SAN LUIS|LOS GAVANES (LA MATA)|EL RODEO|EL PALMAR|LA GAITANA|SAN ISIDRO II|EL DELIRIO|LA MONICA|LA ABEJITA|CACHIMBALITO - CA|MADREVIEJA|SAN JUAN|LA FORTUNA|AGROFORESTAL MATA AZUL|VILLA ISABELLA|MIRADOR|EL CARIBE|EL PEDRAL BONANZA|SAN NICOLAS|LOS LOBOS|EL RASTRO|LOS GANSOS|BETANIA|AGUAS CLARAS|LAS NUBES|CAMPO ALEGRE|CAÑO COLORADO|FORTUL|AGUA BLANCA|ALCIDES FERNANDEZ|EL MONASTERIO|AGUACLARA|ACAPULCO|ARARACUARA|GUSTAVO ROJAS PINILLA|AEROFLANDES - C.A.|EL RIO|HACARITAMA|MARIA ANGELICA|EL DIAMANTE|ANTONIO ROLDAN BETANCOURT|ARBOLETES|EL TRONCAL|ATACO|SANTIAGO PEREZ QUIROZ|EL CORAJE|INGENIO RISARALDA|BANCO LARGO|GUAICARAMO|PIZARRO|BECERRIL|BERASTEGUI|BIZERTA|BARRANCO MINAS|BOLUGA|BUENOS AIRES|BUENOS AIRES -FADELCE|EL DORADO|BUENAVENTURA-GERARDO TOBAR LOPEZ|ALFONSO BONILLA ARAGON|CONDOTO MANDINGA|NAVAS PARDO|RAFAEL NUÑEZ|CAMILO DAZA|CAMILO DAZA No.2|LAS BRUJAS|YARIGUIES|EL JUNCAL|LAS FLORES|EL ALCARAVAN|GUSTAVO ARTUNDUAGA PAREDES|JUAN CASIANO|FLAMINIO S. CAMACHO|HATO COROZAL|PERALES|JAIME ORTIZ BETANCUR|MIRAFLORES|BARACOA|SAN BERNARDO|JOSE CELESTINO|EL PINDO|LOS GARZONES|FABIO A. LEON BENTLEY|REYES MURILLO|BENITO SALAS|EL MEDANO|REMEDIOS OTU|GERMAN OLANO|PAIPA JUAN JOSE RONDON|GUILLERMO LEON VALENCIA|ANTONIO NARIÑO|CONTADOR|GEMELOS DORADOS|TRES DE MAYO|EL EMBRUJO|PAZ DE ARIPORO|CRAVO NORTE|ALMIRANTE PADILLA|COLONIZADORES|SIMON BOLIVAR|SAN MARTIN|EDUARDO FALLA SOLANO|LA FLORIDA|TRINIDAD|TOLU|GUSTAVO VARGAS|TABLON DE TAMARA|EL CARAÑO|ALI PIEDRAHITA|CANANGUCHAL|ALFONSO LOPEZ PUMAREJO|VANGUARDIA|SANTA ISABEL|YAPIMA|TROMPILLOS|GERMAN ALBERTO|GUILLERMO GOMEZ ORTIZ|
    +--------------------+------------+---------+---------------------+-----+------------------+-----------+--------------------+---------+--------+----------+------------+------------------+----------------+----------+---------+---------------+----------+------------------------+--------------------+----------+----------+-------+-----------+---------+------------+-------+-------------------+----------+----------+------+-------+---------+-----------+----------------+----------------+--------+-------+-----------+---------+--------+----------+--------+----------+--------+----------+-------------+---------+----------+----------+------+------------+----------+---------------+-------+--------------+----------+---------------+---------------+---------+---------+--------+----------+-----------+--------------------+---------+----------+-----+--------------------+--------------------+------------+----------------+-----------+--------+---------+---------------------+--------------------+------------+----------------+----------+----------+--------------+----------------+-------------+---------+----------+----------+------------+------------+------------+--------+---------------------+--------+---------+----------+-------------+----------+---------+----------+-----------------+----------+--------+----------+----------------------+--------------+-------+---------+-----------------+-----------+---------+---------+----------+-------+------------+---------+------------+-------------+------+-----------+-----------------+-------------+---------+--------+----------+---------------------+------------------+------+----------+--------------+-----------+-------------------------+---------+----------+-----+---------------------+---------+-----------------+-----------+----------+-------+--------+----------+-------+--------------+------+------------+---------------------+---------+--------------------------------+----------------------+----------------+-----------+------------+-----------+----------------+----------+---------+---------+----------+------------+--------------------------+------------+-------------------+------------+-------+--------------------+----------+-------+------------+--------------+--------+------------+---------------------+-------------+------------+---------+------------+------------+----------------------+-----------------------+--------------+--------+---------------+------------+----------+--------------+-----------+-----------------+-------------+-------------+----------+--------------------+----------+--------+----+--------------+----------------+---------+--------------+-----------+----------------------+----------+------------+------+----------+--------------+---------------------+
    |        CERROPETRONA|        null|     null|                 null| null|              null|       null|                null|     null|    null|      null|        null|              null|            null|      null|     null|           null|      null|                    null|                null|      null|      null|   null|       null|     null|        null|   null|               null|      null|      null|  null|   null|     null|       null|            null|            null|    null|   null|       null|     null|    null|      null|    null|      null|    null|      null|         null|     null|      null|      null|  null|        null|      null|           null|   null|          null|      null|           null|           null|     null|     null|    null|      null|       null|                null|     null|      null| null|                null|                null|        null|            null|       null|    null|     null|                 null|                null|        null|            null|      null|      null|          null|            null|         null|     null|      null|      null|        null|        null|        null|    null|                 null|    null|     null|      null|         null|      null|     null|      null|             null|      null|    null|      null|                  null|          null|   null|     null|             null|       null|     null|     null|      null|   null|        null|     null|        null|         null|  null|       null|             null|         null|     null|    null|      null|                 null|              null|  null|      null|          null|       null|                     null|     null|      null| null|                 null|     null|             null|       null|      null|   null|    null|      null|   null|          null|  null|        null|                 null|     null|                            null|                  null|            null|       null|        null|       null|            null|      null|     null|     null|      null|        null|                      null|        null|               null|        null|   null|                null|      null|   null|        null|          null|    null|        null|                 null|         null|        null|     null|        null|        null|                  null|                   null|          null|    null|           null|        null|      null|          null|       null|             null|         null|         null|      null|                null|      null|    null|null|          null|            null|     null|          null|       null|                  null|      null|        null|  null|      null|          null|                 null|
    |        LOS BARRILES|        null|     null|                 null| null|              null|       null|                null|     null|    null|      null|        null|              null|            null|      null|     null|           null|      null|                    null|                null|      null|      null|   null|       null|     null|        null|   null|               null|      null|      null|  null|   null|     null|       null|            null|            null|    null|   null|       null|     null|    null|      null|    null|      null|    null|      null|         null|     null|      null|      null|  null|        null|      null|           null|   null|          null|      null|           null|           null|     null|     null|    null|      null|       null|                null|     null|      null| null|                null|                null|        null|            null|       null|    null|     null|                 null|                null|        null|            null|      null|      null|          null|            null|         null|     null|      null|      null|        null|        null|        null|    null|                 null|    null|     null|      null|         null|      null|     null|      null|             null|      null|    null|      null|                  null|          null|   null|     null|             null|       null|     null|     null|      null|   null|        null|     null|        null|         null|  null|       null|             null|         null|     null|    null|      null|                 null|              null|  null|      null|          null|       null|                     null|     null|      null| null|                 null|     null|             null|       null|      null|   null|    null|      null|   null|          null|  null|        null|                 null|     null|                            null|                  null|            null|       null|        null|       null|            null|      null|     null|     null|      null|        null|                      null|        null|               null|        null|   null|                null|      null|   null|        null|          null|    null|        null|                 null|         null|        null|     null|        null|        null|                  null|                   null|          null|    null|           null|        null|      null|          null|       null|             null|         null|         null|      null|                null|      null|    null|null|          null|            null|     null|          null|       null|                  null|      null|        null|  null|      null|          null|                 null|
    |            MOSCOVIA|        null|     null|                 null| null|              null|       null|                null|     null|    null|      null|        null|              null|            null|      null|     null|           null|      null|                    null|                null|      null|      null|   null|       null|     null|        null|   null|               null|      null|      null|  null|   null|     null|       null|            null|            null|    null|   null|       null|     null|    null|      null|    null|      null|    null|      null|         null|     null|      null|      null|  null|        null|      null|           null|   null|          null|      null|           null|           null|     null|     null|    null|      null|       null|                null|     null|      null| null|                null|                null|        null|            null|       null|    null|     null|                 null|                null|        null|            null|      null|      null|          null|            null|         null|     null|      null|      null|        null|        null|        null|    null|                 null|    null|     null|      null|         null|      null|     null|      null|             null|      null|    null|      null|                  null|          null|   null|     null|             null|       null|     null|     null|      null|   null|        null|     null|        null|         null|  null|       null|             null|         null|     null|    null|      null|                 null|              null|  null|      null|          null|       null|                     null|     null|      null| null|                 null|     null|             null|       null|      null|   null|    null|      null|   null|          null|  null|        null|                 null|     null|                            null|                  null|            null|       null|        null|       null|            null|      null|     null|     null|      null|        null|                      null|        null|               null|        null|   null|                null|      null|   null|        null|          null|    null|        null|                 null|         null|        null|     null|        null|        null|                  null|                   null|          null|    null|           null|        null|      null|          null|       null|             null|         null|         null|      null|                null|      null|    null|null|          null|            null|     null|          null|       null|                  null|      null|        null|  null|      null|          null|                 null|
    |   BANCO DE ARENAS 2|        null|     null|                 null| null|              null|       null|                null|     null|    null|      null|        null|              null|            null|      null|     null|           null|      null|                    null|                null|      null|      null|   null|       null|     null|        null|   null|               null|      null|      null|  null|   null|     null|       null|            null|            null|    null|   null|       null|     null|    null|      null|    null|      null|    null|      null|         null|     null|      null|      null|  null|        null|      null|           null|   null|          null|      null|           null|           null|     null|     null|    null|      null|       null|                null|     null|      null| null|                null|                null|        null|            null|       null|    null|     null|                 null|                null|        null|            null|      null|      null|          null|            null|         null|     null|      null|      null|        null|        null|        null|    null|                 null|    null|     null|      null|         null|      null|     null|      null|             null|      null|    null|      null|                  null|          null|   null|     null|             null|       null|     null|     null|      null|   null|        null|     null|        null|         null|  null|       null|             null|         null|     null|    null|      null|                 null|              null|  null|      null|          null|       null|                     null|     null|      null| null|                 null|     null|             null|       null|      null|   null|    null|      null|   null|          null|  null|        null|                 null|     null|                            null|                  null|            null|       null|        null|       null|            null|      null|     null|     null|      null|        null|                      null|        null|               null|        null|   null|                null|      null|   null|        null|          null|    null|        null|                 null|         null|        null|     null|        null|        null|                  null|                   null|          null|    null|           null|        null|      null|          null|       null|             null|         null|         null|      null|                null|      null|    null|null|          null|            null|     null|          null|       null|                  null|      null|        null|  null|      null|          null|                 null|
    |BRISAS DEL CHICAM...|        null|     null|                 null| null|              null|       null|                null|     null|    null|      null|        null|              null|            null|      null|     null|           null|      null|                    null|                null|      null|      null|   null|       null|     null|        null|   null|               null|      null|      null|  null|   null|     null|       null|            null|            null|    null|   null|       null|     null|    null|      null|    null|      null|    null|      null|         null|     null|      null|      null|  null|        null|      null|           null|   null|          null|      null|           null|           null|     null|     null|    null|      null|       null|                null|     null|      null| null|                null|                null|        null|            null|       null|    null|     null|                 null|                null|        null|            null|      null|      null|          null|            null|         null|     null|      null|      null|        null|        null|        null|    null|                 null|    null|     null|      null|         null|      null|     null|      null|             null|      null|    null|      null|                  null|          null|   null|     null|             null|       null|     null|     null|      null|   null|        null|     null|        null|         null|  null|       null|             null|         null|     null|    null|      null|                 null|              null|  null|      null|          null|       null|                     null|     null|      null| null|                 null|     null|             null|       null|      null|   null|    null|      null|   null|          null|  null|        null|                 null|     null|                            null|                  null|            null|       null|        null|       null|            null|      null|     null|     null|      null|        null|                      null|        null|               null|        null|   null|                null|      null|   null|        null|          null|    null|        null|                 null|         null|        null|     null|        null|        null|                  null|                   null|          null|    null|           null|        null|      null|          null|       null|             null|         null|         null|      null|                null|      null|    null|null|          null|            null|     null|          null|       null|                  null|      null|        null|  null|      null|          null|                 null|
    |           MIRALINDO|        null|     null|                 null| null|              null|       null|                null|     null|    null|      null|        null|              null|            null|      null|     null|           null|      null|                    null|                null|      null|      null|   null|       null|     null|        null|   null|               null|      null|      null|  null|   null|     null|       null|            null|            null|    null|   null|       null|     null|    null|      null|    null|      null|    null|      null|         null|     null|      null|      null|  null|        null|      null|           null|   null|          null|      null|           null|           null|     null|     null|    null|      null|       null|                null|     null|      null| null|                null|                null|        null|            null|       null|    null|     null|                 null|                null|        null|            null|      null|      null|          null|            null|         null|     null|      null|      null|        null|        null|        null|    null|                 null|    null|     null|      null|         null|      null|     null|      null|             null|      null|    null|      null|                  null|          null|   null|     null|             null|       null|     null|     null|      null|   null|        null|     null|        null|         null|  null|       null|             null|         null|     null|    null|      null|                 null|              null|  null|      null|          null|       null|                     null|     null|      null| null|                 null|     null|             null|       null|      null|   null|    null|      null|   null|          null|  null|        null|                 null|     null|                            null|                  null|            null|       null|        null|       null|            null|      null|     null|     null|      null|        null|                      null|        null|               null|        null|   null|                null|      null|   null|        null|          null|    null|        null|                 null|         null|        null|     null|        null|        null|                  null|                   null|          null|    null|           null|        null|      null|          null|       null|             null|         null|         null|      null|                null|      null|    null|null|          null|            null|     null|          null|       null|                  null|      null|        null|  null|      null|          null|                 null|
    |CABECERA RÍO SAN ...|        null|     null|                 null| null|              null|       null|                null|     null|    null|      null|        null|              null|            null|      null|     null|           null|      null|                    null|                null|      null|      null|   null|       null|     null|        null|   null|               null|      null|      null|  null|   null|     null|       null|            null|            null|    null|   null|       null|     null|    null|      null|    null|      null|    null|      null|         null|     null|      null|      null|  null|        null|      null|           null|   null|          null|      null|           null|           null|     null|     null|    null|      null|       null|                null|     null|      null| null|                null|                null|        null|            null|       null|    null|     null|                 null|                null|        null|            null|      null|      null|          null|            null|         null|     null|      null|      null|        null|        null|        null|    null|                 null|    null|     null|      null|         null|      null|     null|      null|             null|      null|    null|      null|                  null|          null|   null|     null|             null|       null|     null|     null|      null|   null|        null|     null|        null|         null|  null|       null|             null|         null|     null|    null|      null|                 null|              null|  null|      null|          null|       null|                     null|     null|      null| null|                 null|     null|             null|       null|      null|   null|    null|      null|   null|          null|  null|        null|                 null|     null|                            null|                  null|            null|       null|        null|       null|            null|      null|     null|     null|      null|        null|                      null|        null|               null|        null|   null|                null|      null|   null|        null|          null|    null|        null|                 null|         null|        null|     null|        null|        null|                  null|                   null|          null|    null|           null|        null|      null|          null|       null|             null|         null|         null|      null|                null|      null|    null|null|          null|            null|     null|          null|       null|                  null|      null|        null|  null|      null|          null|                 null|
    |       SAN ANTONIO 2|        null|     null|                 null| null|              null|       null|                null|     null|    null|      null|        null|              null|            null|      null|     null|           null|      null|                    null|                null|      null|      null|   null|       null|     null|        null|   null|               null|      null|      null|  null|   null|     null|       null|            null|            null|    null|   null|       null|     null|    null|      null|    null|      null|    null|      null|         null|     null|      null|      null|  null|        null|      null|           null|   null|          null|      null|           null|           null|     null|     null|    null|      null|       null|                null|     null|      null| null|                null|                null|        null|            null|       null|    null|     null|                 null|                null|        null|            null|      null|      null|          null|            null|         null|     null|      null|      null|        null|        null|        null|    null|                 null|    null|     null|      null|         null|      null|     null|      null|             null|      null|    null|      null|                  null|          null|   null|     null|             null|       null|     null|     null|      null|   null|        null|     null|        null|         null|  null|       null|             null|         null|     null|    null|      null|                 null|              null|  null|      null|          null|       null|                     null|     null|      null| null|                 null|     null|             null|       null|      null|   null|    null|      null|   null|          null|  null|        null|                 null|     null|                            null|                  null|            null|       null|        null|       null|            null|      null|     null|     null|      null|        null|                      null|        null|               null|        null|   null|                null|      null|   null|        null|          null|    null|        null|                 null|         null|        null|     null|        null|        null|                  null|                   null|          null|    null|           null|        null|      null|          null|       null|             null|         null|         null|      null|                null|      null|    null|null|          null|            null|     null|          null|       null|                  null|      null|        null|  null|      null|          null|                 null|
    |            JIGUALES|        null|     null|                 null| null|              null|       null|                null|     null|    null|      null|        null|              null|            null|      null|     null|           null|      null|                    null|                null|      null|      null|   null|       null|     null|        null|   null|               null|      null|      null|  null|   null|     null|       null|            null|            null|    null|   null|       null|     null|    null|      null|    null|      null|    null|      null|         null|     null|      null|      null|  null|        null|      null|           null|   null|          null|      null|           null|           null|     null|     null|    null|      null|       null|                null|     null|      null| null|                null|                null|        null|            null|       null|    null|     null|                 null|                null|        null|            null|      null|      null|          null|            null|         null|     null|      null|      null|        null|        null|        null|    null|                 null|    null|     null|      null|         null|      null|     null|      null|             null|      null|    null|      null|                  null|          null|   null|     null|             null|       null|     null|     null|      null|   null|        null|     null|        null|         null|  null|       null|             null|         null|     null|    null|      null|                 null|              null|  null|      null|          null|       null|                     null|     null|      null| null|                 null|     null|             null|       null|      null|   null|    null|      null|   null|          null|  null|        null|                 null|     null|                            null|                  null|            null|       null|        null|       null|            null|      null|     null|     null|      null|        null|                      null|        null|               null|        null|   null|                null|      null|   null|        null|          null|    null|        null|                 null|         null|        null|     null|        null|        null|                  null|                   null|          null|    null|           null|        null|      null|          null|       null|             null|         null|         null|      null|                null|      null|    null|null|          null|            null|     null|          null|       null|                  null|      null|        null|  null|      null|          null|                 null|
    |          MAPIRIPANA|        null|     null|                 null| null|              null|       null|                null|     null|    null|      null|        null|              null|            null|      null|     null|           null|      null|                    null|                null|      null|      null|   null|       null|     null|        null|   null|               null|      null|      null|  null|   null|     null|       null|            null|            null|    null|   null|       null|     null|    null|      null|    null|      null|    null|      null|         null|     null|      null|      null|  null|        null|      null|           null|   null|          null|      null|           null|           null|     null|     null|    null|      null|       null|                null|     null|      null| null|                null|                null|        null|            null|       null|    null|     null|                 null|                null|        null|            null|      null|      null|          null|            null|         null|     null|      null|      null|        null|        null|        null|    null|                 null|    null|     null|      null|         null|      null|     null|      null|             null|      null|    null|      null|                  null|          null|   null|     null|             null|       null|     null|     null|      null|   null|        null|     null|        null|         null|  null|       null|             null|         null|     null|    null|      null|                 null|              null|  null|      null|          null|       null|                     null|     null|      null| null|                 null|     null|             null|       null|      null|   null|    null|      null|   null|          null|  null|        null|                 null|     null|                            null|                  null|            null|       null|        null|       null|            null|      null|     null|     null|      null|        null|                      null|        null|               null|        null|   null|                null|      null|   null|        null|          null|    null|        null|                 null|         null|        null|     null|        null|        null|                  null|                   null|          null|    null|           null|        null|      null|          null|       null|             null|         null|         null|      null|                null|      null|    null|null|          null|            null|     null|          null|       null|                  null|      null|        null|  null|      null|          null|                 null|
    |      PUERTO ZANCUDO|        null|     null|                 null| null|              null|       null|                null|     null|    null|      null|        null|              null|            null|      null|     null|           null|      null|                    null|                null|      null|      null|   null|       null|     null|        null|   null|               null|      null|      null|  null|   null|     null|       null|            null|            null|    null|   null|       null|     null|    null|      null|    null|      null|    null|      null|         null|     null|      null|      null|  null|        null|      null|           null|   null|          null|      null|           null|           null|     null|     null|    null|      null|       null|                null|     null|      null| null|                null|                null|        null|            null|       null|    null|     null|                 null|                null|        null|            null|      null|      null|          null|            null|         null|     null|      null|      null|        null|        null|        null|    null|                 null|    null|     null|      null|         null|      null|     null|      null|             null|      null|    null|      null|                  null|          null|   null|     null|             null|       null|     null|     null|      null|   null|        null|     null|        null|         null|  null|       null|             null|         null|     null|    null|      null|                 null|              null|  null|      null|          null|       null|                     null|     null|      null| null|                 null|     null|             null|       null|      null|   null|    null|      null|   null|          null|  null|        null|                 null|     null|                            null|                  null|            null|       null|        null|       null|            null|      null|     null|     null|      null|        null|                      null|        null|               null|        null|   null|                null|      null|   null|        null|          null|    null|        null|                 null|         null|        null|     null|        null|        null|                  null|                   null|          null|    null|           null|        null|      null|          null|       null|             null|         null|         null|      null|                null|      null|    null|null|          null|            null|     null|          null|       null|                  null|      null|        null|  null|      null|          null|                 null|
    |   VILLA ALEJANDRA 2|        null|     null|                 null| null|              null|       null|                null|     null|    null|      null|        null|              null|            null|      null|     null|           null|      null|                    null|                null|      null|      null|   null|       null|     null|        null|   null|               null|      null|      null|  null|   null|     null|       null|            null|            null|    null|   null|       null|     null|    null|      null|    null|      null|    null|      null|         null|     null|      null|      null|  null|        null|      null|           null|   null|          null|      null|           null|           null|     null|     null|    null|      null|       null|                null|     null|      null| null|                null|                null|        null|            null|       null|    null|     null|                 null|                null|        null|            null|      null|      null|          null|            null|         null|     null|      null|      null|        null|        null|        null|    null|                 null|    null|     null|      null|         null|      null|     null|      null|             null|      null|    null|      null|                  null|          null|   null|     null|             null|       null|     null|     null|      null|   null|        null|     null|        null|         null|  null|       null|             null|         null|     null|    null|      null|                 null|              null|  null|      null|          null|       null|                     null|     null|      null| null|                 null|     null|             null|       null|      null|   null|    null|      null|   null|          null|  null|        null|                 null|     null|                            null|                  null|            null|       null|        null|       null|            null|      null|     null|     null|      null|        null|                      null|        null|               null|        null|   null|                null|      null|   null|        null|          null|    null|        null|                 null|         null|        null|     null|        null|        null|                  null|                   null|          null|    null|           null|        null|      null|          null|       null|             null|         null|         null|      null|                null|      null|    null|null|          null|            null|     null|          null|       null|                  null|      null|        null|  null|      null|          null|                 null|
    |         LA CATALINA|        null|     null|                 null| null|              null|       null|                null|     null|    null|      null|        null|              null|            null|      null|     null|           null|      null|                    null|                null|      null|      null|   null|       null|     null|        null|   null|               null|      null|      null|  null|   null|     null|       null|            null|            null|    null|   null|       null|     null|    null|      null|    null|      null|    null|      null|         null|     null|      null|      null|  null|        null|      null|           null|   null|          null|      null|           null|           null|     null|     null|    null|      null|       null|                null|     null|      null| null|                null|                null|        null|            null|       null|    null|     null|                 null|                null|        null|            null|      null|      null|          null|            null|         null|     null|      null|      null|        null|        null|        null|    null|                 null|    null|     null|      null|         null|      null|     null|      null|             null|      null|    null|      null|                  null|          null|   null|     null|             null|       null|     null|     null|      null|   null|        null|     null|        null|         null|  null|       null|             null|         null|     null|    null|      null|                 null|              null|  null|      null|          null|       null|                     null|     null|      null| null|                 null|     null|             null|       null|      null|   null|    null|      null|   null|          null|  null|        null|                 null|     null|                            null|                  null|            null|       null|        null|       null|            null|      null|     null|     null|      null|        null|                      null|        null|               null|        null|   null|                null|      null|   null|        null|          null|    null|        null|                 null|         null|        null|     null|        null|        null|                  null|                   null|          null|    null|           null|        null|      null|          null|       null|             null|         null|         null|      null|                null|      null|    null|null|          null|            null|     null|          null|       null|                  null|      null|        null|  null|      null|          null|                 null|
    +--------------------+------------+---------+---------------------+-----+------------------+-----------+--------------------+---------+--------+----------+------------+------------------+----------------+----------+---------+---------------+----------+------------------------+--------------------+----------+----------+-------+-----------+---------+------------+-------+-------------------+----------+----------+------+-------+---------+-----------+----------------+----------------+--------+-------+-----------+---------+--------+----------+--------+----------+--------+----------+-------------+---------+----------+----------+------+------------+----------+---------------+-------+--------------+----------+---------------+---------------+---------+---------+--------+----------+-----------+--------------------+---------+----------+-----+--------------------+--------------------+------------+----------------+-----------+--------+---------+---------------------+--------------------+------------+----------------+----------+----------+--------------+----------------+-------------+---------+----------+----------+------------+------------+------------+--------+---------------------+--------+---------+----------+-------------+----------+---------+----------+-----------------+----------+--------+----------+----------------------+--------------+-------+---------+-----------------+-----------+---------+---------+----------+-------+------------+---------+------------+-------------+------+-----------+-----------------+-------------+---------+--------+----------+---------------------+------------------+------+----------+--------------+-----------+-------------------------+---------+----------+-----+---------------------+---------+-----------------+-----------+----------+-------+--------+----------+-------+--------------+------+------------+---------------------+---------+--------------------------------+----------------------+----------------+-----------+------------+-----------+----------------+----------+---------+---------+----------+------------+--------------------------+------------+-------------------+------------+-------+--------------------+----------+-------+------------+--------------+--------+------------+---------------------+-------------+------------+---------+------------+------------+----------------------+-----------------------+--------------+--------+---------------+------------+----------+--------------+-----------+-----------------+-------------+-------------+----------+--------------------+----------+--------+----+--------------+----------------+---------+--------------+-----------+----------------------+----------+------------+------+----------+--------------+---------------------+
    


.. code:: ipython3

    distancias_df.schema.fields[0].name




.. parsed-literal::

    'Unnamed: 0'



.. code:: ipython3

    distancias_df.select(i.name)

.. code:: ipython3

    distancias_df[1]




.. parsed-literal::

    Column<'LA ESCONDIDA'>



.. code:: ipython3

    for i in distancias_df.schema.fields:
        
        try:
            if i.dataType == StringType or i.dataType == BoolType:
                mini = '-'
                maxi = '-'
                P50  = '-'
                mean = '-'
                stdv = '-'    
            else:
                mini = distancias_df.select(i.name).summary().collect()[3][1]
                maxi = distancias_df.select(i.name).summary().collect()[7][1]
                P50  = distancias_df.select(i.name).summary().collect()[5][1]
                mean = distancias_df.select(i.name).summary().collect()[1][1]
                stdv = distancias_df.select(i.name).summary().collect()[2][1]
    
                falt = distancias_df.where(distancias_df[i.name].isNull()).count()
                val_unic = distancias_df.select(i.name).distinct().count()
                name = i.name
                tipo = i.dataType
    
            
                print ('|',name,'|',tipo,'|',falt,'|',(mini),'|',maxi,'|',P50,'|',mean,'|',stdv,'|',val_unic,'|')   
    
        except:
                pass # doing nothing on exception


.. parsed-literal::

    | LA ESCONDIDA | DoubleType | 13 | 13.9878032846239 | 1482.76577236659 | 587.248079001968 | 563.1865118293824 | 184.7729700721747 | 5436 |
    | MORICHITO | DoubleType | 13 | 0.637913491210542 | 1337.85772011372 | 507.76584692204 | 493.16959386605635 | 197.25665850061256 | 5436 |
    | CAROLINA DEL PRINCIPE | DoubleType | 13 | 0.863587276926074 | 1352.12210998824 | 297.753469031898 | 352.10803715808976 | 194.2537672295603 | 5436 |
    | DUBAI | DoubleType | 13 | 3.90080329328121 | 1499.73276132309 | 372.459104074522 | 407.9596075596445 | 249.99496908179114 | 5436 |
    | BARU - HIDROPUERTO | DoubleType | 13 | 4.81187338683297 | 1722.27685098134 | 567.229272269016 | 551.6536357445464 | 308.37028143947316 | 5436 |
    | SAN FELIPE DEL PAUTO | DoubleType | 13 | 4.99493221774517 | 1465.17562754941 | 608.221424261798 | 576.6770039278679 | 188.36752739171436 | 5436 |
    | LA UNION | DoubleType | 13 | 19.6409805662825 | 1427.16052230908 | 437.669187831879 | 470.05020061507736 | 203.05052780064517 | 5436 |
    | LA ILUSION | DoubleType | 13 | 20.2335428916897 | 1386.03474172782 | 472.77915899346 | 466.96557055823035 | 187.78739204860898 | 5436 |
    | LA VENTUROSA | DoubleType | 13 | 2.66127428082872 | 1427.79819841127 | 569.549447317449 | 540.4425891217076 | 188.89184541136356 | 5436 |
    | GUAYABAL DEL CRAVO | DoubleType | 13 | 13.0553030914762 | 1441.52672959629 | 548.578103095741 | 526.8437038228395 | 185.759476531733 | 5436 |
    | LAS VIOLETAS- CA | DoubleType | 13 | 0.717056246771218 | 1351.95639233729 | 390.38936550104 | 420.58232976983203 | 201.55483744317033 | 5436 |
    | LOS MANGOS | DoubleType | 13 | 3.7377092763782 | 1265.17189482211 | 353.199947454186 | 381.4087297457844 | 216.3669608757809 | 5436 |
    | EL CONDOR | DoubleType | 13 | 1.53024414735335 | 1242.90511137569 | 343.438706131764 | 367.7957064741362 | 208.29488604168645 | 5436 |
    | HOTEL SAN DIEGO | DoubleType | 13 | 33.3822674470509 | 1497.89225130826 | 534.299873304967 | 542.7739943775978 | 187.05856453227585 | 5436 |
    | EL CAFUCHE | DoubleType | 13 | 47.3860305555576 | 1534.2164658165 | 684.761625510865 | 655.7039890516367 | 189.30419835896564 | 5436 |
    | GUACHARACAS (COLOMBAIMA) | DoubleType | 13 | 3.84874433647238 | 1201.59756673658 | 331.631118743858 | 351.9347562047135 | 200.026051161826 | 5436 |
    | GAVILAN DE LA PASCUA | DoubleType | 13 | 41.2453058239915 | 1515.71720100468 | 658.003217662179 | 624.6274036775318 | 187.4961578368779 | 5436 |
    | DOROTEA B1 | DoubleType | 13 | 7.70794313521254 | 1432.47081369383 | 586.780950277856 | 561.288020122435 | 191.5487757423849 | 5436 |
    | HORIZONTES | DoubleType | 13 | 0.734235544069704 | 1294.05535007822 | 383.194946445583 | 412.35401543869506 | 215.0972667622844 | 5436 |
    | MACOLLA | DoubleType | 13 | 16.4243091410413 | 1399.24825231622 | 504.786465107031 | 487.773354444636 | 186.9757471154329 | 5436 |
    | MULETOS- CA | DoubleType | 13 | 11.5217437066695 | 1407.81817420024 | 495.886686702077 | 486.21349865086444 | 186.79402269039562 | 5436 |
    | LA MAPORA | DoubleType | 13 | 2.0825520029921 | 1340.68028293151 | 498.393243101213 | 478.895483581522 | 193.63081941921342 | 5436 |
    | LLANO CAUCHO | DoubleType | 13 | 7.10121493267809 | 1537.06661613266 | 636.576005868728 | 611.1043577426207 | 183.8058565269708 | 5436 |
    | ARMENIA | DoubleType | 13 | 22.1312954295832 | 1488.60551213101 | 651.554296939336 | 636.4789924936846 | 194.46524638610245 | 5436 |
    | RANCHO COLIBRI - CA | DoubleType | 13 | 2.3766794074978 | 1182.76063028782 | 364.082271298851 | 368.89869762760054 | 219.805417698415 | 5436 |
    | EL CAIRANO | DoubleType | 13 | 2.45897609824242 | 1759.76803160376 | 610.402150448402 | 591.7075296625916 | 320.09309976961003 | 5436 |
    | NUEVA ROMA | DoubleType | 13 | 1.59995239652603 | 1648.69593164714 | 532.809636318644 | 526.8268062898895 | 311.87137990448196 | 5436 |
    | JAGUAR | DoubleType | 13 | 36.9370826301453 | 1446.07474890557 | 509.649724502005 | 508.9120426120314 | 186.30969454330733 | 5436 |
    | OCELOTE | DoubleType | 13 | 30.1059012305884 | 1474.17698805308 | 549.305058588251 | 538.9506349445389 | 184.88436982592108 | 5436 |
    | GETSEMANI | DoubleType | 13 | 25.7746546343852 | 1479.69157701588 | 657.481283103787 | 650.8297036329521 | 199.2534531810731 | 5436 |
    | SAN LUIS DE PACA | DoubleType | 13 | 23.978699220525 | 1846.22715677683 | 791.82408038545 | 822.4883974149101 | 211.4805750553212 | 5436 |
    | CANANARI | DoubleType | 13 | 36.8115066358267 | 1833.28582503919 | 761.323623164623 | 806.3561361465956 | 220.45359107026627 | 5436 |
    | MARAREY | DoubleType | 13 | 5.4871673122225 | 1476.19586010205 | 594.957635865861 | 567.4773452474193 | 185.7061480257511 | 5436 |
    | SANTA CLARA | DoubleType | 13 | 4.61815442357054 | 1372.66512573534 | 441.220488321977 | 448.83932624123065 | 189.67477173274992 | 5436 |
    | SAN PABLO | DoubleType | 13 | 2.82155824400994 | 1610.20606420073 | 453.431925387643 | 462.44777927383245 | 275.63058589598654 | 5436 |
    | COROCITO | DoubleType | 13 | 46.8556155236493 | 1452.64523922929 | 495.095814958997 | 505.51747133309436 | 188.10689764038722 | 5436 |
    | LAS FURIAS | DoubleType | 13 | 15.2017860210727 | 1347.24845467754 | 493.070058942921 | 471.03430073677305 | 191.12997598055674 | 5436 |
    | EL NOGAL | DoubleType | 13 | 9.30304978054294 | 1370.01987348016 | 410.434349970492 | 435.14982159286717 | 195.9658564651607 | 5436 |
    | SANTA CRUZ | DoubleType | 13 | 8.43080029731266 | 1366.2880351522099 | 414.7751150069 | 435.25199933172036 | 193.83596181134752 | 5436 |
    | CANTADELICIAS | DoubleType | 13 | 0.503575435359665 | 1317.51603469378 | 359.66382486529 | 411.105719270025 | 225.95971252491242 | 5436 |
    | LAS VEGAS | DoubleType | 13 | 10.9281521030008 | 1376.02759452582 | 548.217804011549 | 535.9319280138151 | 198.4165628983922 | 5436 |
    | LA FRANCIA | DoubleType | 13 | 17.4188766814117 | 1589.33904016133 | 511.277183434932 | 524.1010500049143 | 299.5362117306392 | 5436 |
    | JULIAN | DoubleType | 13 | 13.2063856302867 | 1368.82951261989 | 425.460286971767 | 440.9590899573874 | 191.536156091813 | 5436 |
    | EL SOÑADOR | DoubleType | 13 | 4.77769918753263 | 1333.48223910591 | 466.447738841368 | 449.0582443601733 | 190.17133468603785 | 5436 |
    | EL CONCHAL - CA | DoubleType | 13 | 2.13221120940084 | 1205.11261886517 | 351.34905307654 | 403.7613345422857 | 243.52117941026796 | 5436 |
    | SEVILLA | DoubleType | 13 | 9.32340860555465 | 1344.35929761492 | 514.436825655174 | 499.65670801093506 | 197.31813897105894 | 5436 |
    | ASA SAN MARTIN | DoubleType | 13 | 3.59962570035499 | 1759.04816471375 | 693.933972428521 | 678.8526454444055 | 331.2100379793943 | 5436 |
    | LA FAZENDA | DoubleType | 13 | 12.4816111688119 | 1422.31613607913 | 491.111943144119 | 490.55931187848086 | 186.9781883064763 | 5436 |
    | LAS AGUILAS -CA | DoubleType | 13 | 1.88499510230244 | 1221.40611493305 | 355.559350564868 | 417.208604498181 | 251.11923969946406 | 5436 |
    | CAMARUCOS | DoubleType | 13 | 34.1433986433268 | 1440.25394250796 | 606.730821103079 | 592.0772397520469 | 195.90795539938128 | 5436 |
    | EL TOTUMO | DoubleType | 13 | 0.643107589871348 | 1386.14402444327 | 555.139319380931 | 541.4557274079501 | 197.3233676998047 | 5436 |
    | VARSOVIA | DoubleType | 13 | 8.7516897997555 | 1245.09512736795 | 350.290286044288 | 375.0815351346116 | 216.90558579870202 | 5436 |
    | LA HERMOSA | DoubleType | 13 | 1.01413120554447 | 1481.62130670266 | 638.079408329374 | 613.2145635487842 | 191.52193151522448 | 5436 |
    | SAN ESTEBAN | DoubleType | 13 | 24.3972712642987 | 1453.6375357049 | 605.476354023443 | 578.9036444635184 | 190.6955346143159 | 5436 |
    | SAN JOSE DEL ARIPORO | DoubleType | 13 | 28.1497746031489 | 1487.61396597058 | 659.537287979882 | 652.2424969462771 | 197.79666703140867 | 5436 |
    | COLINERAS | DoubleType | 13 | 23.5690496909271 | 1453.74993850049 | 625.811852246096 | 618.7933223363254 | 198.42791768050563 | 5436 |
    | LA CAIMANA | DoubleType | 13 | 26.9200446324512 | 1414.82562090981 | 586.68695191478 | 577.7575830288527 | 198.62190638301882 | 5436 |
    | EMAUS | DoubleType | 13 | 7.41011801055802 | 1456.19399724785 | 595.089870205035 | 564.2866600279534 | 187.96241279007305 | 5436 |
    | SANTA MARIA DEL CAFÉ | DoubleType | 13 | 22.6721241321352 | 1395.2813608822 | 570.153357273379 | 562.0281234910115 | 199.75812625632148 | 5436 |
    | MIRAMAR DE GUANAPALO | DoubleType | 13 | 0.811394737776313 | 1466.64903887088 | 600.323340282669 | 569.7984821591697 | 187.18387810184214 | 5436 |
    | LA SALVACION | DoubleType | 13 | 39.9848550703036 | 1434.5867414468 | 604.468820378923 | 594.3402267320672 | 197.63131138918158 | 5436 |
    | LOMA GRANDE - CA | DoubleType | 13 | 3.90990836341005 | 1503.15018811411 | 372.359912614394 | 408.1219598658303 | 249.41828341590127 | 5436 |
    | EL CAPRICHO | DoubleType | 13 | 25.2460732374319 | 1436.69098328801 | 497.297431701511 | 499.3864740657707 | 186.92656011800486 | 5436 |
    | COROCORA | DoubleType | 13 | 12.6923347001453 | 1446.80298103336 | 595.592402460856 | 567.746266725762 | 190.03700899506026 | 5436 |
    | GRISMANIA | DoubleType | 13 | 7.67633621893673 | 1347.52107293508 | 464.354762987302 | 449.89352801387 | 189.01539051384708 | 5436 |
    | PALMAS DE TUMACO  -CA | DoubleType | 13 | 4.75298008937078 | 1459.60747256192 | 663.829116077037 | 668.780650933518 | 308.40454960345375 | 5436 |
    | INGENIO LA CARMELITA | DoubleType | 13 | 2.04128915383929 | 1174.75272749041 | 354.42940421325 | 387.42440747001075 | 232.28928625247806 | 5436 |
    | EL LAGO - CA | DoubleType | 13 | 2.89025643932482 | 1211.37852566521 | 336.563529652891 | 355.99152855392816 | 203.86366304798227 | 5436 |
    | HACIENDA LA JOYA | DoubleType | 13 | 7.60376251221047 | 1629.38777446456 | 494.922351901204 | 492.4010561396061 | 302.3466529168867 | 5436 |
    | LA PASTORA | DoubleType | 13 | 13.049364123546 | 1352.58377866123 | 560.464962067189 | 568.6845549716567 | 211.91752982603705 | 5436 |
    | TALANQUERA | DoubleType | 13 | 2.14573814533637 | 1376.36410056922 | 424.813008172518 | 443.609297577948 | 192.56606791848324 | 5436 |
    | VILLA GEORGINA | DoubleType | 13 | 4.2065503972006 | 1266.87191024273 | 345.103894442067 | 374.88370474808323 | 201.73488186968956 | 5436 |
    | INGENIO PICHICHI | DoubleType | 13 | 1.68784108453001 | 1235.52143386964 | 351.806005810207 | 406.76645639565237 | 245.8172359008484 | 5436 |
    | SAN SEBASTIAN | DoubleType | 13 | 2.59522728758268 | 1378.88653482757 | 441.125240761332 | 451.4830850097242 | 189.93476043214216 | 5436 |
    | EL COROZO | DoubleType | 13 | 20.3619706731483 | 1356.7622139986 | 568.287011193974 | 581.3979403624734 | 213.76758715551333 | 5436 |
    | HATO VIEJO | DoubleType | 13 | 20.4437446653206 | 1364.95251959351 | 575.484743819442 | 588.0509110422814 | 213.21013337686622 | 5436 |
    | LA REDENCION | DoubleType | 13 | 14.8871616901248 | 1354.71746391355 | 513.226098875721 | 493.1917459063605 | 193.76419761480796 | 5436 |
    | LOS REMANSOS | DoubleType | 13 | 4.64276041178121 | 1288.57563633622 | 315.08184234803 | 358.38677572713465 | 196.074965212994 | 5436 |
    | LOS GAVANES (LA MATA) | DoubleType | 13 | 8.17218780090496 | 1358.65163973098 | 573.771369200772 | 590.4651193067372 | 215.35629043108293 | 5436 |
    | EL PALMAR | DoubleType | 13 | 9.37452676946263 | 1365.32991408043 | 511.602567659551 | 487.71874147519003 | 190.8767422993911 | 5436 |
    | LA GAITANA | DoubleType | 13 | 7.2496410386664 | 1369.4828933895 | 423.329584969789 | 440.2077198024909 | 192.07481468627 | 5436 |
    | SAN ISIDRO II | DoubleType | 13 | 5.95623532504294 | 1352.56883209935 | 403.547118063541 | 425.2148697607693 | 195.0170045473143 | 5436 |
    | EL DELIRIO | DoubleType | 13 | 17.6511399488301 | 1350.18661251442 | 564.550617824893 | 580.3160339316381 | 215.1829734176278 | 5436 |
    | LA MONICA | DoubleType | 13 | 19.2523334116661 | 1364.32598286002 | 456.599947082322 | 451.63249597473384 | 188.5826701684632 | 5436 |
    | LA ABEJITA | DoubleType | 13 | 5.77874663215657 | 1360.76482767733 | 481.298531453527 | 462.9426395792218 | 188.6363879269675 | 5436 |
    | CACHIMBALITO - CA | DoubleType | 13 | 4.89680455102638 | 1257.29066878557 | 377.96550872113 | 435.377967949372 | 260.3959295495037 | 5436 |
    | MADREVIEJA | DoubleType | 13 | 13.5030122391931 | 1356.5759882138 | 505.790071285999 | 483.1513269130648 | 191.57556034331142 | 5436 |
    | SAN JUAN | DoubleType | 13 | 7.5759661040505 | 1389.21881495566 | 414.849574922029 | 445.2729955944623 | 199.46919068619007 | 5436 |
    | LA FORTUNA | DoubleType | 13 | 8.54042096177242 | 1383.50502961958 | 429.869343378213 | 448.51763340794616 | 192.36707861494637 | 5436 |
    | AGROFORESTAL MATA AZUL | DoubleType | 13 | 30.2818131389982 | 1481.78550269596 | 629.669814664629 | 598.9674372247875 | 189.05555496459294 | 5436 |
    | VILLA ISABELLA | DoubleType | 13 | 5.94135487024418 | 1420.08887239574 | 564.533130650452 | 536.121643906709 | 189.4806058789288 | 5436 |
    | MIRADOR | DoubleType | 13 | 7.27423037204168 | 1388.73568362267 | 538.300789662292 | 512.5995827908118 | 191.0089445774103 | 5436 |
    | EL CARIBE | DoubleType | 13 | 6.42376720922239 | 1377.3079776353 | 537.991768551164 | 518.718401290981 | 194.3822176932187 | 5436 |
    | EL PEDRAL BONANZA | DoubleType | 13 | 3.30971260937571 | 1342.89945533222 | 339.860994436205 | 386.6102733880967 | 216.20312329207516 | 5436 |
    | SAN NICOLAS | DoubleType | 13 | 12.6805370601837 | 1392.54299173768 | 470.342346737935 | 468.5140047011063 | 187.9294091449751 | 5436 |
    | LOS LOBOS | DoubleType | 13 | 2.54123219958269 | 1369.99172359703 | 443.189698521197 | 448.53590332140806 | 189.4048994808483 | 5436 |
    | EL RASTRO | DoubleType | 13 | 20.4307443659041 | 1503.72917213402 | 617.288513860009 | 589.5216669752398 | 184.89549994845925 | 5436 |
    | LOS GANSOS | DoubleType | 13 | 12.689874293318 | 1494.03861711467 | 604.320823758448 | 578.1128792620913 | 184.86948187069245 | 5436 |
    | BETANIA | DoubleType | 13 | 13.4544088380417 | 1374.82144658696 | 527.404415990802 | 504.8186320558927 | 192.15539875117227 | 5436 |
    | AGUAS CLARAS | DoubleType | 13 | 0.907046045006214 | 1442.320978156 | 386.796204763419 | 448.44542474814193 | 255.09826088830917 | 5436 |
    | LAS NUBES | DoubleType | 13 | 33.4674354482407 | 1460.92778443279 | 531.984675808918 | 525.0065305596455 | 185.42714574876376 | 5436 |
    | CAÑO COLORADO | DoubleType | 13 | 29.6713904769729 | 1882.72631478206 | 810.638419346554 | 852.5749687009878 | 219.2309536559663 | 5436 |
    | FORTUL | DoubleType | 13 | 2.0683407077015 | 1280.81355743872 | 488.911383090127 | 500.19448822012015 | 212.85883654397526 | 5436 |
    | ALCIDES FERNANDEZ | DoubleType | 13 | 1.7510863829413 | 1629.2077165182 | 488.958728995976 | 498.456747397676 | 228.1296397420581 | 5436 |
    | EL MONASTERIO | DoubleType | 13 | 7.09460149789186 | 1362.45825668465 | 402.261177897002 | 428.99078400751534 | 197.59412323573247 | 5436 |
    | AGUACLARA | DoubleType | 13 | 2.2231196417175 | 1325.49642346153 | 415.46719776134 | 420.3500118508215 | 190.6748838402353 | 5436 |
    | ACAPULCO | DoubleType | 13 | 5.73533652593695 | 1367.92472815594 | 515.207396227175 | 490.92950891648206 | 190.96812420343102 | 5436 |
    | ARARACUARA | DoubleType | 13 | 2.37778081926826 | 1842.93497658724 | 757.529527250187 | 822.0303597051303 | 252.445663561806 | 5436 |
    | GUSTAVO ROJAS PINILLA | DoubleType | 13 | 1.26551227159588 | 1238.63908025113 | 393.499013930595 | 387.7569330242926 | 192.13496544237304 | 5436 |
    | EL RIO | DoubleType | 13 | 4.29901138791201 | 1357.67232688846 | 290.248208735947 | 355.4261964366832 | 198.12670874172585 | 5436 |
    | HACARITAMA | DoubleType | 13 | 1.65766589525993 | 1441.75224208882 | 374.760262641797 | 435.2257712287655 | 252.48899066031038 | 5436 |
    | MARIA ANGELICA | DoubleType | 13 | 5.27726971637841 | 1347.41592675751 | 469.677315688928 | 452.9184476493185 | 189.15368116396402 | 5436 |
    | EL DIAMANTE | DoubleType | 13 | 3.68771260170693 | 1388.83766151065 | 424.216185428653 | 447.8589153447279 | 195.5348538170447 | 5436 |
    | ANTONIO ROLDAN BETANCOURT | DoubleType | 13 | 0.921760240971251 | 1532.57686853419 | 410.630921634691 | 433.2020823207785 | 215.60079385149353 | 5436 |
    | ARBOLETES | DoubleType | 13 | 1.28321019974693 | 1618.53269333707 | 455.822505065534 | 472.860212111156 | 255.1093894011855 | 5436 |
    | EL TRONCAL | DoubleType | 13 | 0.428162469090855 | 1301.05307665287 | 518.409233137402 | 536.8243169487238 | 217.38457151622214 | 5436 |
    | ATACO | DoubleType | 13 | 1.06099209861001 | 1272.53664414741 | 350.58486134607 | 396.2509437483866 | 229.99152776859597 | 5436 |
    | SANTIAGO PEREZ QUIROZ | DoubleType | 13 | 1.39614798574324 | 1358.61310417478 | 576.305607200248 | 594.966158631844 | 216.39212076359652 | 5436 |
    | INGENIO RISARALDA | DoubleType | 13 | 1.8180291850273 | 1208.875809409 | 352.297918020422 | 352.8151776569907 | 207.56145455698763 | 5436 |
    | BANCO LARGO | DoubleType | 13 | 17.5606938014158 | 1347.81147039085 | 497.034789976658 | 474.90753751361643 | 191.654834936399 | 5436 |
    | GUAICARAMO | DoubleType | 13 | 12.3353305559193 | 1355.86853273604 | 426.526248584476 | 435.99395345131535 | 190.6020939934564 | 5436 |
    | PIZARRO | DoubleType | 13 | 0.725147687879642 | 1310.41041444757 | 410.392622992153 | 421.69355077831545 | 209.54333544379108 | 5436 |
    | BERASTEGUI | DoubleType | 13 | 0.228512709547622 | 1588.07315241977 | 435.39621136361 | 448.6267512336969 | 270.44465738320787 | 5436 |
    | BIZERTA | DoubleType | 13 | 18.9056957428825 | 1356.56692771365 | 503.888404255892 | 480.8661452204175 | 191.20075513266715 | 5436 |
    | BARRANCO MINAS | DoubleType | 13 | 0.538729901384086 | 1680.21853249156 | 740.138115893362 | 721.6146982273368 | 183.95352617088122 | 5436 |
    | BOLUGA | DoubleType | 13 | 2.14860571615246 | 1200.08960866102 | 331.690119960773 | 352.12857336251284 | 201.47718400440826 | 5436 |
    | BUENOS AIRES | DoubleType | 13 | 0.3432082049401 | 1874.03767553781 | 794.527085702123 | 842.438863746908 | 228.75758491353793 | 5436 |
    | BUENOS AIRES -FADELCE | DoubleType | 13 | 2.43896662465263 | 1527.05225528844 | 444.158178789587 | 475.2085655291234 | 280.85452463902817 | 5436 |
    | EL DORADO | DoubleType | 13 | 7.21322529297458 | 1249.09062259818 | 349.751758489748 | 368.3072138284899 | 197.0824703208898 | 5436 |
    | BUENAVENTURA-GERARDO TOBAR LOPEZ | DoubleType | 13 | 0.662733461056776 | 1186.77154047038 | 373.889722723743 | 426.5686089543788 | 243.95344363233505 | 5436 |
    | ALFONSO BONILLA ARAGON | DoubleType | 13 | 3.44008896549045 | 1223.86303433877 | 349.909452737065 | 412.06288789164114 | 248.93743081115025 | 5436 |
    | CONDOTO MANDINGA | DoubleType | 13 | 3.02249392479381 | 1273.79027070854 | 381.000317501691 | 379.7395120080421 | 206.9435986406516 | 5436 |
    | NAVAS PARDO | DoubleType | 13 | 15.6139149541047 | 1254.81940536828 | 349.099440391347 | 388.85858177438655 | 228.1076936006567 | 5436 |
    | RAFAEL NUÑEZ | DoubleType | 13 | 4.82620548631854 | 1741.02933569753 | 587.620840292497 | 569.4879410523645 | 312.97432870690847 | 5436 |
    | CAMILO DAZA | DoubleType | 13 | 2.4841571487614 | 1378.29663792433 | 418.862287191849 | 478.72668728231815 | 242.17890228775093 | 5436 |
    | LAS BRUJAS | DoubleType | 13 | 1.03817489468778 | 1617.07552441423 | 469.495941932307 | 471.6790404355871 | 291.5836312655121 | 5436 |
    | YARIGUIES | DoubleType | 13 | 0.684334361746066 | 1319.74677328624 | 334.365201825201 | 380.8200960966999 | 210.02846110486917 | 5436 |
    | EL JUNCAL | DoubleType | 13 | 9.16504624113739 | 1347.67962717214 | 373.304779789717 | 442.1513387637097 | 245.6978057544423 | 5436 |
    | LAS FLORES | DoubleType | 13 | 0.259020968033453 | 1646.36845602249 | 512.596535515411 | 507.3235582300888 | 307.3160815717452 | 5436 |
    | EL ALCARAVAN | DoubleType | 13 | 1.69640207639908 | 1332.23062281592 | 471.275983830927 | 452.5741490953918 | 190.69731024935436 | 5436 |
    | GUSTAVO ARTUNDUAGA PAREDES | DoubleType | 13 | 0.841219607568959 | 1458.96265151794 | 490.608312118723 | 539.5967115471473 | 278.0215386347244 | 5436 |
    | JUAN CASIANO | DoubleType | 13 | 1.15369028931094 | 1306.81082865222 | 518.117901202366 | 541.5877663801399 | 283.6437158410045 | 5436 |
    | HATO COROZAL | DoubleType | 13 | 0.472471482574954 | 1324.31747417864 | 505.673536958452 | 495.7947925423329 | 200.93206456368816 | 5436 |
    | PERALES | DoubleType | 13 | 4.69259702577186 | 1209.03151867716 | 344.454282474631 | 358.6310377018356 | 208.82092834216738 | 5436 |
    | JAIME ORTIZ BETANCUR | DoubleType | 13 | 3.20532461655199 | 1517.75057565643 | 402.874026296782 | 425.92302700715516 | 211.77356790290037 | 5436 |
    | MIRAFLORES | DoubleType | 13 | 0.0223630141858667 | 1693.94616102078 | 635.7517303035 | 681.8313673337492 | 218.49977024658017 | 5436 |
    | BARACOA | DoubleType | 13 | 2.21925096408352 | 1594.93563897439 | 461.887608589826 | 467.0679062513052 | 292.1135714767782 | 5436 |
    | SAN BERNARDO | DoubleType | 13 | 1.79588575103807 | 1577.28537549451 | 459.614049210627 | 469.612131318682 | 291.50831173312145 | 5436 |
    | JOSE CELESTINO | DoubleType | 13 | 1.56339369952542 | 1181.82554890677 | 324.513164528898 | 341.6813371343356 | 193.23829251331375 | 5436 |
    | EL PINDO | DoubleType | 13 | 2.69646381856604 | 1484.72052719062 | 356.459598382101 | 396.59893761985364 | 233.04459123928896 | 5436 |
    | LOS GARZONES | DoubleType | 13 | 1.56533270049002 | 1588.34061129083 | 431.408774998866 | 448.8045849155896 | 265.88547904805716 | 5436 |
    | REYES MURILLO | DoubleType | 13 | 0.0230081279085742 | 1369.56034301726 | 410.707508834332 | 412.6517669068546 | 194.6223138492614 | 5436 |
    | BENITO SALAS | DoubleType | 13 | 2.11548660605974 | 1338.77214089956 | 365.992757248582 | 435.27999685180924 | 242.90152940918114 | 5436 |
    | EL MEDANO | DoubleType | 13 | 20.1988728994995 | 1428.4171963461 | 536.689362459318 | 515.7510744798602 | 186.14257036395324 | 5436 |
    | REMEDIOS OTU | DoubleType | 13 | 0.20587786172667 | 1354.48042351288 | 295.31823634933 | 359.9696140634622 | 202.44901771967795 | 5436 |
    | GERMAN OLANO | DoubleType | 13 | 0.642689171521501 | 1717.81119682881 | 915.946790114954 | 903.1309148942377 | 199.1897131393525 | 5436 |
    | PAIPA JUAN JOSE RONDON | DoubleType | 13 | 2.19393029435305 | 1240.15973320883 | 405.550202597294 | 399.7387382981383 | 194.0765507117889 | 5436 |
    | GUILLERMO LEON VALENCIA | DoubleType | 13 | 2.75865279019864 | 1323.33859726331 | 442.844561801208 | 485.7565913626262 | 279.8185536373664 | 5436 |
    | ANTONIO NARIÑO | DoubleType | 13 | 0.436506514066723 | 1406.59619047003 | 573.587168069755 | 590.7538116867831 | 311.6333782597644 | 5436 |
    | CONTADOR | DoubleType | 13 | 7.41741514461964 | 1407.48723394569 | 477.500083148486 | 520.0967409523615 | 284.08352291530554 | 5436 |
    | GEMELOS DORADOS | DoubleType | 13 | 1.45039604956152 | 1341.90310750432 | 342.644714755811 | 388.290774519644 | 216.70842479675056 | 5436 |
    | TRES DE MAYO | DoubleType | 13 | 0.39414837664737 | 1529.22843265866 | 628.155380200189 | 648.2492072234122 | 310.8928442995453 | 5436 |
    | EL EMBRUJO | DoubleType | 13 | 0.282750026371487 | 2323.72600799546 | 1142.60293119234 | 1126.7797671180156 | 254.84203165203732 | 5436 |
    | PAZ DE ARIPORO | DoubleType | 13 | 0.900626206460384 | 1332.76126472844 | 500.924367078619 | 486.00359507853767 | 196.71382289356217 | 5436 |
    | CRAVO NORTE | DoubleType | 13 | 1.60902341259149 | 1452.73582966354 | 637.907738910425 | 634.2791486738178 | 201.94056542339092 | 5436 |
    | ALMIRANTE PADILLA | DoubleType | 13 | 1.68138609948784 | 1779.56086192519 | 725.51777969897 | 711.0798721584671 | 331.89089169349154 | 5436 |
    | COLONIZADORES | DoubleType | 13 | 1.82184258408062 | 1262.99329175626 | 480.806433299809 | 497.06837940548013 | 216.40618167151607 | 5436 |
    | SIMON BOLIVAR | DoubleType | 13 | 6.49897029044744 | 1768.83759927058 | 655.459435194174 | 634.4612163451272 | 331.4322107156113 | 5436 |
    | SAN MARTIN | DoubleType | 13 | 2.40695521645425 | 1367.8584875068 | 400.323083262779 | 430.22565810240985 | 202.8394247969532 | 5436 |
    | EDUARDO FALLA SOLANO | DoubleType | 13 | 3.60611199871139 | 1444.87140851893 | 439.56612208093 | 500.6810915446923 | 248.66343441482664 | 5436 |
    | LA FLORIDA | DoubleType | 13 | 1.83880207329003 | 1429.34663916297 | 640.848963840676 | 646.0495080493916 | 301.890839755885 | 5436 |
    | TRINIDAD | DoubleType | 13 | 2.58110350525908 | 1383.71634003745 | 532.54750485097 | 507.79760839275264 | 191.07850288819043 | 5436 |
    | TOLU | DoubleType | 13 | 1.84642844426804 | 1647.73007499846 | 495.344629684788 | 490.16158133579665 | 292.9630555514664 | 5436 |
    | GUSTAVO VARGAS | DoubleType | 13 | 0.244011887084875 | 1304.29659033382 | 496.472372713844 | 497.63021035916654 | 206.24998693095245 | 5436 |
    | TABLON DE TAMARA | DoubleType | 13 | 1.00833921400835 | 1325.43281735695 | 487.671726039861 | 469.5491031263493 | 194.61645790554184 | 5436 |
    | EL CARAÑO | DoubleType | 13 | 1.69234685360565 | 1327.80184485172 | 374.040083046287 | 376.4097985269837 | 195.0499716312532 | 5436 |
    | ALI PIEDRAHITA | DoubleType | 13 | 1.34974142950493 | 1357.91063291103 | 334.832100001826 | 362.456459132157 | 189.99106090137445 | 5436 |
    | CANANGUCHAL | DoubleType | 13 | 2.03383960324055 | 1475.8380802763 | 584.576267389837 | 605.6921640179442 | 308.3389203706994 | 5436 |
    | ALFONSO LOPEZ PUMAREJO | DoubleType | 13 | 3.02628237225327 | 1668.04166197616 | 601.614000734181 | 598.9041906147997 | 317.9231791279039 | 5436 |
    | VANGUARDIA | DoubleType | 13 | 4.40538942005912 | 1332.31688043039 | 385.701080879896 | 410.7759490644487 | 197.37067462972522 | 5436 |
    | SANTA ISABEL | DoubleType | 13 | 8.59454170158226 | 1376.53847431215 | 417.006494386968 | 440.18585304971805 | 195.14042633569272 | 5436 |
    | YAPIMA | DoubleType | 13 | 52.4097880369872 | 1892.50141331557 | 855.284341652796 | 873.8010751549161 | 203.8414573042617 | 5436 |
    | TROMPILLOS | DoubleType | 13 | 4.83219600459724 | 1365.84297856013 | 493.17635640187 | 471.845948408957 | 188.80427157261988 | 5436 |
    | GERMAN ALBERTO | DoubleType | 13 | 8.78327401343268 | 1347.48836316605 | 479.338145920787 | 459.62185831468696 | 189.66294694178418 | 5436 |
    | GUILLERMO GOMEZ ORTIZ | DoubleType | 13 | 5.53513892219753 | 1274.70955269607 | 371.81982377715 | 397.78157926449876 | 208.06888174091162 | 5436 |


.. code:: ipython3

    for i in distancias_df.schema.fields:
        
        if(i.dataType != distancias_df.schema[3].dataType):
            print('|', i.name, '|')


.. parsed-literal::

    | Unnamed: 0 |
    | LA CAROLINA |
    | VELASQUEZ |
    | CURUMANI |
    | SAN MIGUEL |
    | SAN FELIPE |
    | LOS HALCONES |
    | GUADUALITO |
    | LA PONDEROSA |
    | SAN LUIS |
    | EL RODEO |
    | CAMPO ALEGRE |
    | AGUA BLANCA |
    | EL CORAJE |
    | BECERRIL |



3.4 vuelos.csv
^^^^^^^^^^^^^^

.. code:: ipython3

    vuelos_df.show()


.. parsed-literal::

    +----+---+------+-------+-----------+----------+-------+--------------------+------+------+--------------+---------+------------------+
    | ano|mes|origen|destino|tipo_equipo|tipo_vuelo|trafico|             empresa|vuelos|sillas|carga_ofrecida|pasajeros|       carga_bordo|
    +----+---+------+-------+-----------+----------+-------+--------------------+------+------+--------------+---------+------------------+
    |2012|  1|   BOG|    CUC|       BE20|         T|      N|             RIO SUR|   1.0|   0.0|           0.0|      4.0|             100.0|
    |2013|  5|   UIB|    BOG|       DH8D|         R|      N|               AIRES|  30.0|1110.0|       24000.0|    873.0|            4222.0|
    |2013| 10|   IBE|    BOG|       DH8D|         R|      N|               AIRES|  98.0|3626.0|       56056.0|   2866.0|           2323.75|
    |2012|  4|   FLA|    BOG|       JS32|         T|      N|         SARPA S.A.S|   1.0|   0.0|           0.0|      4.0|               0.0|
    |2013|  7|   CUC|    AUC|       C402|         T|      N|AEROCHARTER ANDIN...|   1.0|   0.0|           0.0|      nan|             180.0|
    |2012|  3|   CUC|    OCV|       C303|         T|      N|AEROCHARTER ANDIN...|   6.0|   0.0|           0.0|      6.0|               0.0|
    |2013| 11|   7NS|    BOG|       B190|         T|      N|         SEARCA S.A.|   nan|   nan|           0.0|    149.0|             880.0|
    |2010|  6|   AUC|    TME|       C206|         T|      N|                SAER|   1.0|   0.0|           0.0|      2.0|              50.0|
    |2010|  4|   CLO|    BOG|       D328|         R|      N|SERVICIO AEREO A ...|   3.0|  96.0|        7078.5|      2.0|               0.0|
    |2013|  5|   BOG|    CLO|       JS32|         T|      N|         SARPA S.A.S|   1.0|   0.0|           0.0|     10.0|               0.0|
    |2013| 12|   MVP|    VVC|       AT72|         R|      N|SERVICIO AEREO A ...|   9.0| 621.0|       18000.0|    569.0|               0.0|
    |2013|  4|   BOG|    IPI|       D328|         R|      N|SERVICIO AEREO A ...|  13.0| 416.0|       19500.0|      nan|               0.0|
    |2012|  6|   BOG|    FLA|       JS32|         T|      N|         SARPA S.A.S|   4.0|   nan|           0.0|     11.0|               0.0|
    |2011|  6|   NVA|    BOG|       BE20|         T|      N|         SEARCA S.A.|   2.0|   0.0|           0.0|     11.0|              40.0|
    |2010|  5|   BOG|    CTG|        M83|         R|      N|                 SAM|   1.0| 142.0|        7211.0|    107.0|              34.0|
    |2010| 11|   BOG|    VVC|       AN32|         T|      N| AER CARIBE LIMITADA|   2.0|   0.0|           0.0|      0.0|1687.3999999999999|
    |2013|  5|   EYP|    AUC|       JS32|         T|    nan|         SARPA S.A.S|   nan|   0.0|           0.0|     18.0|               0.0|
    |2013| 11|   PUU|    BOG|       AT72|         R|      N|SERVICIO AEREO A ...|   1.0|  70.0|        2000.0|     29.0|               0.0|
    |2011| 10|   BOG|    CLO|       C560|         T|      N|     CENTRAL CHARTER|   4.0|   0.0|           0.0|      8.0|               0.0|
    |2012| 10|   BOG|    ADZ|       BE9L|         T|      N|             RIO SUR|   1.0|   0.0|           0.0|      2.0|             100.0|
    +----+---+------+-------+-----------+----------+-------+--------------------+------+------+--------------+---------+------------------+
    only showing top 20 rows
    


.. code:: ipython3

    vuelos_df.columns




.. parsed-literal::

    ['ano',
     'mes',
     'origen',
     'destino',
     'tipo_equipo',
     'tipo_vuelo',
     'trafico',
     'empresa',
     'vuelos',
     'sillas',
     'carga_ofrecida',
     'pasajeros',
     'carga_bordo']



.. code:: ipython3

    print('Las dimensiones de la tabla son ', len(vuelos_df.columns),' columnas y', vuelos_df.count(), 'observaciones.')


.. parsed-literal::

    Las dimensiones de la tabla son  13  columnas y 67599 observaciones.


.. code:: ipython3

    vuelos_df.select('carga_bordo').distinct().show()


.. parsed-literal::

    +------------------+
    |       carga_bordo|
    +------------------+
    |             38.61|
    |             558.0|
    |           29811.0|
    |           17884.0|
    |             70.07|
    |            3980.0|
    |            4800.0|
    |             496.0|
    |           21606.0|
    |            3597.0|
    |            1072.5|
    |             769.0|
    |             65.78|
    |              2.86|
    |           39252.0|
    |             934.0|
    |            2862.0|
    |1181.1799999999998|
    |           3636.36|
    |           18114.0|
    +------------------+
    only showing top 20 rows
    


.. code:: ipython3

    vuelos_df.where(vuelos_df['origen'] == 'NVA')




.. parsed-literal::

    DataFrame[ano: int, mes: int, origen: string, destino: string, tipo_equipo: string, tipo_vuelo: string, trafico: string, empresa: string, vuelos: string, sillas: string, carga_ofrecida: double, pasajeros: string, carga_bordo: double]



.. code:: ipython3

    for i in vuelos_df.schema.fields:
        
        if i.dataType == StringType or i.dataType == BoolType:
        
            mini = '-'
            maxi = '-'
            P50  = '-'
            mean = '-'
            stdv = '-'    
        else:
            mini = vuelos_df.select(i.name).summary().collect()[3][1]
            maxi = vuelos_df.select(i.name).summary().collect()[7][1]
            P50  = vuelos_df.select(i.name).summary().collect()[5][1]
            mean = vuelos_df.select(i.name).summary().collect()[1][1]
            stdv = vuelos_df.select(i.name).summary().collect()[2][1]
    
        falt = vuelos_df.where(vuelos_df[i.name] == 'nan').count()
        val_unic = vuelos_df.select(i.name).distinct().count()
        name = i.name
        tipo = i.dataType
    
        print ('|',name,'|',tipo,'|',falt,'|',(mini),'|',maxi,'|',P50,'|',mean,'|',stdv,'|',val_unic,'|')
           


.. parsed-literal::

    | ano | IntegerType | 0 | 2010 | 2013 | 2012 | 2011.5928637997604 | 1.1162151375421105 | 4 |
    | mes | IntegerType | 0 | 1 | 12 | 7 | 6.674359088152191 | 3.442955191548653 | 12 |
    | origen | StringType | 0 | - | - | - | - | - | 111 |
    | destino | StringType | 0 | - | - | - | - | - | 110 |
    | tipo_equipo | StringType | 0 | - | - | - | - | - | 94 |
    | tipo_vuelo | StringType | 0 | - | - | - | - | - | 4 |
    | trafico | StringType | 894 | - | - | - | - | - | 2 |
    | empresa | StringType | 0 | - | - | - | - | - | 71 |
    | vuelos | StringType | 1582 | - | - | - | - | - | 611 |
    | sillas | StringType | 4017 | - | - | - | - | - | 5810 |
    | carga_ofrecida | DoubleType | 0 | 0.0 | 7361385.0 | 0.0 | 46758.48664891492 | 230667.33670267835 | 9649 |
    | pasajeros | StringType | 3483 | - | - | - | - | - | 7894 |
    | carga_bordo | DoubleType | 0 | 0.0 | 1062015.0 | 110.0 | 5325.677899968939 | 23415.901919555745 | 10289 |


4. Completitud
~~~~~~~~~~~~~~

4.1. ‘aeropuertos.csv’
^^^^^^^^^^^^^^^^^^^^^^

.. code:: ipython3

    # Completitud por columnas

.. code:: ipython3

    aeropuertos_df.columns




.. parsed-literal::

    ['_c0',
     'sigla',
     'iata',
     'nombre',
     'municipio',
     'departamento',
     'categoria',
     'latitud',
     'longitud',
     'propietario',
     'explotador',
     'longitud_pista',
     'ancho_pista',
     'pbmo',
     'elevacion',
     'resolucion',
     'fecha_construccion',
     'fecha_vigencia',
     'clase',
     'tipo',
     'numero_vuelos_origen',
     'gcd_departamento',
     'gcd_municipio']



.. code:: ipython3

    NTF = aeropuertos_df.count()
    for i in aeropuertos_df.columns:
        NVI = aeropuertos_df.select(i).where(aeropuertos_df[i] == 'nan').count()
        complet = (1  - (NVI/NTF)) * 100
        print('|',i,'|', round(float(complet),1),'|')


.. parsed-literal::

    | _c0 | 100.0 |
    | sigla | 100.0 |
    | iata | 28.4 |
    | nombre | 100.0 |
    | municipio | 100.0 |
    | departamento | 100.0 |
    | categoria | 100.0 |
    | latitud | 100.0 |
    | longitud | 100.0 |
    | propietario | 99.0 |
    | explotador | 100.0 |
    | longitud_pista | 100.0 |
    | ancho_pista | 100.0 |
    | pbmo | 81.2 |
    | elevacion | 100.0 |
    | resolucion | 97.9 |
    | fecha_construccion | 100.0 |
    | fecha_vigencia | 24.3 |
    | clase | 100.0 |
    | tipo | 100.0 |
    | numero_vuelos_origen | 75.7 |
    | gcd_departamento | 100.0 |
    | gcd_municipio | 100.0 |


.. code:: ipython3

    aeropuertos_df.select('iata').where(aeropuertos_df['iata'] == 'nan').count()




.. parsed-literal::

    209



.. code:: ipython3

    # Copletitud de tabla

.. code:: ipython3

    aeropuertos_df.count()




.. parsed-literal::

    292



.. code:: ipython3

    aeropuertos_df.collect()[291]['iata'] == 'nan'

.. code:: ipython3

    NFI = 0
    for i in range(0, aeropuertos_df.count()-1):
        faltantes = 0
        for j in aeropuertos_df.columns:
            if aeropuertos_df.collect()[i][j] == 'nan':
                faltantes = faltantes + 1
                print('Falta el dato de', j, 'en la fila', i)
        
        if faltantes > 0:
            NFI = NFI +1
            print('Fila',i)
            print( NFI, 'Faltantes')
    
    print(NFI, 'Filas incompletas')
    Ctabla = (1 - (NFI/aeropuertos_df.count())) * 100
    print('%Ctabla = '  )


.. parsed-literal::

    Falta el dato de pbmo en la fila 0
    Falta el dato de fecha_vigencia en la fila 0
    Fila 0
    1 Faltantes
    Falta el dato de iata en la fila 1
    Falta el dato de fecha_vigencia en la fila 1
    Fila 1
    2 Faltantes
    Falta el dato de fecha_vigencia en la fila 2
    Fila 2
    3 Faltantes
    Falta el dato de iata en la fila 3
    Falta el dato de fecha_vigencia en la fila 3
    Fila 3
    4 Faltantes
    Falta el dato de pbmo en la fila 4
    Falta el dato de fecha_vigencia en la fila 4
    Fila 4
    5 Faltantes
    Falta el dato de iata en la fila 5
    Falta el dato de fecha_vigencia en la fila 5
    Fila 5
    6 Faltantes
    Falta el dato de iata en la fila 6
    Fila 6
    7 Faltantes
    Falta el dato de iata en la fila 7
    Falta el dato de fecha_vigencia en la fila 7
    Fila 7
    8 Faltantes
    Falta el dato de iata en la fila 8
    Falta el dato de fecha_vigencia en la fila 8
    Fila 8
    9 Faltantes
    Falta el dato de iata en la fila 9
    Falta el dato de fecha_vigencia en la fila 9
    Fila 9
    10 Faltantes
    Falta el dato de iata en la fila 10
    Falta el dato de pbmo en la fila 10
    Falta el dato de fecha_vigencia en la fila 10
    Fila 10
    11 Faltantes
    Falta el dato de fecha_vigencia en la fila 11
    Fila 11
    12 Faltantes
    Falta el dato de iata en la fila 12
    Falta el dato de fecha_vigencia en la fila 12
    Fila 12
    13 Faltantes
    Falta el dato de iata en la fila 13
    Falta el dato de fecha_vigencia en la fila 13
    Fila 13
    14 Faltantes
    Falta el dato de iata en la fila 14
    Falta el dato de fecha_vigencia en la fila 14
    Fila 14
    15 Faltantes
    Falta el dato de iata en la fila 15
    Falta el dato de propietario en la fila 15
    Falta el dato de resolucion en la fila 15
    Fila 15
    16 Faltantes
    Falta el dato de pbmo en la fila 16
    Falta el dato de fecha_vigencia en la fila 16
    Fila 16
    17 Faltantes
    Falta el dato de iata en la fila 17
    Falta el dato de pbmo en la fila 17
    Falta el dato de fecha_vigencia en la fila 17
    Fila 17
    18 Faltantes
    Falta el dato de iata en la fila 18
    Falta el dato de fecha_vigencia en la fila 18
    Fila 18
    19 Faltantes
    Falta el dato de fecha_vigencia en la fila 19
    Fila 19
    20 Faltantes
    Falta el dato de pbmo en la fila 20
    Falta el dato de fecha_vigencia en la fila 20
    Fila 20
    21 Faltantes
    Falta el dato de iata en la fila 21
    Fila 21
    22 Faltantes
    Falta el dato de iata en la fila 22
    Fila 22
    23 Faltantes
    Falta el dato de iata en la fila 23
    Falta el dato de fecha_vigencia en la fila 23
    Fila 23
    24 Faltantes
    Falta el dato de iata en la fila 24
    Fila 24
    25 Faltantes
    Falta el dato de iata en la fila 25
    Fila 25
    26 Faltantes
    Falta el dato de iata en la fila 26
    Fila 26
    27 Faltantes
    Falta el dato de iata en la fila 27
    Falta el dato de fecha_vigencia en la fila 27
    Fila 27
    28 Faltantes
    Falta el dato de iata en la fila 28
    Falta el dato de fecha_vigencia en la fila 28
    Fila 28
    29 Faltantes
    Falta el dato de iata en la fila 29
    Falta el dato de fecha_vigencia en la fila 29
    Fila 29
    30 Faltantes
    Falta el dato de iata en la fila 30
    Fila 30
    31 Faltantes
    Falta el dato de fecha_vigencia en la fila 31
    Fila 31
    32 Faltantes
    Falta el dato de iata en la fila 32
    Fila 32
    33 Faltantes
    Falta el dato de iata en la fila 33
    Fila 33
    34 Faltantes
    Falta el dato de fecha_vigencia en la fila 34
    Fila 34
    35 Faltantes
    Falta el dato de iata en la fila 35
    Falta el dato de fecha_vigencia en la fila 35
    Fila 35
    36 Faltantes
    Falta el dato de iata en la fila 36
    Falta el dato de fecha_vigencia en la fila 36
    Fila 36
    37 Faltantes
    Falta el dato de iata en la fila 37
    Falta el dato de fecha_vigencia en la fila 37
    Fila 37
    38 Faltantes
    Falta el dato de pbmo en la fila 38
    Falta el dato de fecha_vigencia en la fila 38
    Fila 38
    39 Faltantes
    Falta el dato de iata en la fila 39
    Fila 39
    40 Faltantes
    Falta el dato de fecha_vigencia en la fila 40
    Fila 40
    41 Faltantes
    Falta el dato de iata en la fila 41
    Falta el dato de fecha_vigencia en la fila 41
    Fila 41
    42 Faltantes
    Falta el dato de iata en la fila 42
    Fila 42
    43 Faltantes
    Falta el dato de iata en la fila 43
    Falta el dato de resolucion en la fila 43
    Falta el dato de fecha_vigencia en la fila 43
    Fila 43
    44 Faltantes
    Falta el dato de iata en la fila 44
    Fila 44
    45 Faltantes
    Falta el dato de fecha_vigencia en la fila 45
    Fila 45
    46 Faltantes
    Falta el dato de iata en la fila 46
    Fila 46
    47 Faltantes
    Falta el dato de iata en la fila 47
    Falta el dato de fecha_vigencia en la fila 47
    Fila 47
    48 Faltantes
    Falta el dato de iata en la fila 48
    Falta el dato de fecha_vigencia en la fila 48
    Fila 48
    49 Faltantes
    Falta el dato de fecha_vigencia en la fila 49
    Fila 49
    50 Faltantes
    Falta el dato de iata en la fila 50
    Falta el dato de fecha_vigencia en la fila 50
    Fila 50
    51 Faltantes
    Falta el dato de iata en la fila 51
    Falta el dato de fecha_vigencia en la fila 51
    Fila 51
    52 Faltantes
    Falta el dato de iata en la fila 52
    Fila 52
    53 Faltantes
    Falta el dato de pbmo en la fila 53
    Falta el dato de fecha_vigencia en la fila 53
    Fila 53
    54 Faltantes
    Falta el dato de iata en la fila 54
    Falta el dato de fecha_vigencia en la fila 54
    Fila 54
    55 Faltantes
    Falta el dato de iata en la fila 55
    Fila 55
    56 Faltantes
    Falta el dato de iata en la fila 56
    Falta el dato de fecha_vigencia en la fila 56
    Fila 56
    57 Faltantes
    Falta el dato de iata en la fila 57
    Falta el dato de fecha_vigencia en la fila 57
    Fila 57
    58 Faltantes
    Falta el dato de pbmo en la fila 58
    Falta el dato de fecha_vigencia en la fila 58
    Fila 58
    59 Faltantes
    Falta el dato de iata en la fila 59
    Falta el dato de fecha_vigencia en la fila 59
    Fila 59
    60 Faltantes
    Falta el dato de iata en la fila 60
    Fila 60
    61 Faltantes
    Falta el dato de pbmo en la fila 61
    Falta el dato de fecha_vigencia en la fila 61
    Fila 61
    62 Faltantes
    Falta el dato de iata en la fila 62
    Fila 62
    63 Faltantes
    Falta el dato de iata en la fila 63
    Fila 63
    64 Faltantes
    Falta el dato de iata en la fila 64
    Falta el dato de fecha_vigencia en la fila 64
    Fila 64
    65 Faltantes
    Falta el dato de iata en la fila 65
    Falta el dato de resolucion en la fila 65
    Fila 65
    66 Faltantes
    Falta el dato de iata en la fila 66
    Falta el dato de fecha_vigencia en la fila 66
    Fila 66
    67 Faltantes
    Falta el dato de iata en la fila 67
    Falta el dato de fecha_vigencia en la fila 67
    Fila 67
    68 Faltantes
    Falta el dato de iata en la fila 68
    Falta el dato de fecha_vigencia en la fila 68
    Fila 68
    69 Faltantes
    Falta el dato de fecha_vigencia en la fila 69
    Fila 69
    70 Faltantes
    Falta el dato de fecha_vigencia en la fila 70
    Fila 70
    71 Faltantes
    Falta el dato de iata en la fila 71
    Falta el dato de fecha_vigencia en la fila 71
    Fila 71
    72 Faltantes
    Falta el dato de iata en la fila 72
    Fila 72
    73 Faltantes
    Falta el dato de pbmo en la fila 73
    Falta el dato de fecha_vigencia en la fila 73
    Fila 73
    74 Faltantes
    Falta el dato de iata en la fila 74
    Falta el dato de fecha_vigencia en la fila 74
    Fila 74
    75 Faltantes
    Falta el dato de iata en la fila 75
    Falta el dato de fecha_vigencia en la fila 75
    Fila 75
    76 Faltantes
    Falta el dato de iata en la fila 76
    Fila 76
    77 Faltantes
    Falta el dato de iata en la fila 77
    Falta el dato de fecha_vigencia en la fila 77
    Fila 77
    78 Faltantes
    Falta el dato de fecha_vigencia en la fila 78
    Fila 78
    79 Faltantes
    Falta el dato de iata en la fila 79
    Fila 79
    80 Faltantes
    Falta el dato de iata en la fila 80
    Falta el dato de fecha_vigencia en la fila 80
    Falta el dato de numero_vuelos_origen en la fila 80
    Fila 80
    81 Faltantes
    Falta el dato de iata en la fila 81
    Falta el dato de fecha_vigencia en la fila 81
    Falta el dato de numero_vuelos_origen en la fila 81
    Fila 81
    82 Faltantes
    Falta el dato de iata en la fila 82
    Falta el dato de fecha_vigencia en la fila 82
    Falta el dato de numero_vuelos_origen en la fila 82
    Fila 82
    83 Faltantes
    Falta el dato de iata en la fila 83
    Falta el dato de numero_vuelos_origen en la fila 83
    Fila 83
    84 Faltantes
    Falta el dato de iata en la fila 84
    Falta el dato de fecha_vigencia en la fila 84
    Falta el dato de numero_vuelos_origen en la fila 84
    Fila 84
    85 Faltantes
    Falta el dato de iata en la fila 85
    Falta el dato de fecha_vigencia en la fila 85
    Fila 85
    86 Faltantes
    Falta el dato de iata en la fila 86
    Falta el dato de fecha_vigencia en la fila 86
    Fila 86
    87 Faltantes
    Falta el dato de iata en la fila 87
    Falta el dato de fecha_vigencia en la fila 87
    Fila 87
    88 Faltantes
    Falta el dato de iata en la fila 88
    Fila 88
    89 Faltantes
    Falta el dato de iata en la fila 89
    Falta el dato de fecha_vigencia en la fila 89
    Fila 89
    90 Faltantes
    Falta el dato de iata en la fila 90
    Fila 90
    91 Faltantes
    Falta el dato de iata en la fila 91
    Falta el dato de fecha_vigencia en la fila 91
    Falta el dato de numero_vuelos_origen en la fila 91
    Fila 91
    92 Faltantes
    Falta el dato de iata en la fila 92
    Falta el dato de fecha_vigencia en la fila 92
    Falta el dato de numero_vuelos_origen en la fila 92
    Fila 92
    93 Faltantes
    Falta el dato de iata en la fila 93
    Falta el dato de numero_vuelos_origen en la fila 93
    Fila 93
    94 Faltantes
    Falta el dato de iata en la fila 94
    Fila 94
    95 Faltantes
    Falta el dato de iata en la fila 95
    Fila 95
    96 Faltantes
    Falta el dato de iata en la fila 96
    Falta el dato de fecha_vigencia en la fila 96
    Fila 96
    97 Faltantes
    Falta el dato de iata en la fila 97
    Falta el dato de fecha_vigencia en la fila 97
    Fila 97
    98 Faltantes
    Falta el dato de iata en la fila 98
    Falta el dato de pbmo en la fila 98
    Falta el dato de fecha_vigencia en la fila 98
    Fila 98
    99 Faltantes
    Falta el dato de iata en la fila 99
    Falta el dato de fecha_vigencia en la fila 99
    Falta el dato de numero_vuelos_origen en la fila 99
    Fila 99
    100 Faltantes
    Falta el dato de iata en la fila 100
    Falta el dato de fecha_vigencia en la fila 100
    Fila 100
    101 Faltantes
    Falta el dato de iata en la fila 101
    Falta el dato de fecha_vigencia en la fila 101
    Falta el dato de numero_vuelos_origen en la fila 101
    Fila 101
    102 Faltantes
    Falta el dato de iata en la fila 102
    Falta el dato de fecha_vigencia en la fila 102
    Falta el dato de numero_vuelos_origen en la fila 102
    Fila 102
    103 Faltantes
    Falta el dato de iata en la fila 103
    Falta el dato de fecha_vigencia en la fila 103
    Fila 103
    104 Faltantes
    Falta el dato de iata en la fila 104
    Falta el dato de fecha_vigencia en la fila 104
    Falta el dato de numero_vuelos_origen en la fila 104
    Fila 104
    105 Faltantes
    Falta el dato de iata en la fila 105
    Falta el dato de numero_vuelos_origen en la fila 105
    Fila 105
    106 Faltantes
    Falta el dato de iata en la fila 106
    Falta el dato de fecha_vigencia en la fila 106
    Falta el dato de numero_vuelos_origen en la fila 106
    Fila 106
    107 Faltantes
    Falta el dato de iata en la fila 107
    Falta el dato de fecha_vigencia en la fila 107
    Fila 107
    108 Faltantes
    Falta el dato de iata en la fila 108
    Falta el dato de pbmo en la fila 108
    Fila 108
    109 Faltantes
    Falta el dato de iata en la fila 109
    Falta el dato de pbmo en la fila 109
    Falta el dato de fecha_vigencia en la fila 109
    Fila 109
    110 Faltantes
    Falta el dato de iata en la fila 110
    Falta el dato de fecha_vigencia en la fila 110
    Fila 110
    111 Faltantes
    Falta el dato de iata en la fila 111
    Falta el dato de numero_vuelos_origen en la fila 111
    Fila 111
    112 Faltantes
    Falta el dato de iata en la fila 112
    Falta el dato de fecha_vigencia en la fila 112
    Falta el dato de numero_vuelos_origen en la fila 112
    Fila 112
    113 Faltantes
    Falta el dato de iata en la fila 113
    Falta el dato de fecha_vigencia en la fila 113
    Fila 113
    114 Faltantes
    Falta el dato de iata en la fila 114
    Falta el dato de fecha_vigencia en la fila 114
    Fila 114
    115 Faltantes
    Falta el dato de iata en la fila 115
    Fila 115
    116 Faltantes
    Falta el dato de iata en la fila 116
    Falta el dato de fecha_vigencia en la fila 116
    Fila 116
    117 Faltantes
    Falta el dato de iata en la fila 117
    Falta el dato de fecha_vigencia en la fila 117
    Fila 117
    118 Faltantes
    Falta el dato de iata en la fila 118
    Falta el dato de fecha_vigencia en la fila 118
    Fila 118
    119 Faltantes
    Falta el dato de iata en la fila 119
    Falta el dato de numero_vuelos_origen en la fila 119
    Fila 119
    120 Faltantes
    Falta el dato de iata en la fila 120
    Falta el dato de fecha_vigencia en la fila 120
    Falta el dato de numero_vuelos_origen en la fila 120
    Fila 120
    121 Faltantes
    Falta el dato de iata en la fila 121
    Falta el dato de fecha_vigencia en la fila 121
    Falta el dato de numero_vuelos_origen en la fila 121
    Fila 121
    122 Faltantes
    Falta el dato de iata en la fila 122
    Falta el dato de fecha_vigencia en la fila 122
    Falta el dato de numero_vuelos_origen en la fila 122
    Fila 122
    123 Faltantes
    Falta el dato de iata en la fila 123
    Fila 123
    124 Faltantes
    Falta el dato de iata en la fila 124
    Falta el dato de fecha_vigencia en la fila 124
    Falta el dato de numero_vuelos_origen en la fila 124
    Fila 124
    125 Faltantes
    Falta el dato de iata en la fila 125
    Falta el dato de fecha_vigencia en la fila 125
    Fila 125
    126 Faltantes
    Falta el dato de iata en la fila 126
    Falta el dato de pbmo en la fila 126
    Falta el dato de fecha_vigencia en la fila 126
    Fila 126
    127 Faltantes
    Falta el dato de iata en la fila 127
    Falta el dato de fecha_vigencia en la fila 127
    Fila 127
    128 Faltantes
    Falta el dato de iata en la fila 128
    Falta el dato de numero_vuelos_origen en la fila 128
    Fila 128
    129 Faltantes
    Falta el dato de iata en la fila 129
    Fila 129
    130 Faltantes
    Falta el dato de iata en la fila 130
    Falta el dato de numero_vuelos_origen en la fila 130
    Fila 130
    131 Faltantes
    Falta el dato de iata en la fila 131
    Falta el dato de numero_vuelos_origen en la fila 131
    Fila 131
    132 Faltantes
    Falta el dato de iata en la fila 132
    Falta el dato de fecha_vigencia en la fila 132
    Falta el dato de numero_vuelos_origen en la fila 132
    Fila 132
    133 Faltantes
    Falta el dato de iata en la fila 133
    Falta el dato de fecha_vigencia en la fila 133
    Falta el dato de numero_vuelos_origen en la fila 133
    Fila 133
    134 Faltantes
    Falta el dato de iata en la fila 134
    Fila 134
    135 Faltantes
    Falta el dato de iata en la fila 135
    Falta el dato de fecha_vigencia en la fila 135
    Falta el dato de numero_vuelos_origen en la fila 135
    Fila 135
    136 Faltantes
    Falta el dato de iata en la fila 136
    Falta el dato de fecha_vigencia en la fila 136
    Falta el dato de numero_vuelos_origen en la fila 136
    Fila 136
    137 Faltantes
    Falta el dato de iata en la fila 137
    Fila 137
    138 Faltantes
    Falta el dato de iata en la fila 138
    Fila 138
    139 Faltantes
    Falta el dato de iata en la fila 139
    Fila 139
    140 Faltantes
    Falta el dato de iata en la fila 140
    Fila 140
    141 Faltantes
    Falta el dato de iata en la fila 141
    Fila 141
    142 Faltantes
    Falta el dato de iata en la fila 142
    Fila 142
    143 Faltantes
    Falta el dato de iata en la fila 143
    Fila 143
    144 Faltantes
    Falta el dato de iata en la fila 144
    Fila 144
    145 Faltantes
    Falta el dato de iata en la fila 145
    Falta el dato de fecha_vigencia en la fila 145
    Fila 145
    146 Faltantes
    Falta el dato de iata en la fila 146
    Falta el dato de fecha_vigencia en la fila 146
    Falta el dato de numero_vuelos_origen en la fila 146
    Fila 146
    147 Faltantes
    Falta el dato de iata en la fila 147
    Falta el dato de fecha_vigencia en la fila 147
    Fila 147
    148 Faltantes
    Falta el dato de iata en la fila 148
    Falta el dato de fecha_vigencia en la fila 148
    Fila 148
    149 Faltantes
    Falta el dato de iata en la fila 149
    Falta el dato de fecha_vigencia en la fila 149
    Falta el dato de numero_vuelos_origen en la fila 149
    Fila 149
    150 Faltantes
    Falta el dato de iata en la fila 150
    Falta el dato de fecha_vigencia en la fila 150
    Falta el dato de numero_vuelos_origen en la fila 150
    Fila 150
    151 Faltantes
    Falta el dato de iata en la fila 151
    Fila 151
    152 Faltantes
    Falta el dato de iata en la fila 152
    Falta el dato de fecha_vigencia en la fila 152
    Fila 152
    153 Faltantes
    Falta el dato de iata en la fila 153
    Falta el dato de fecha_vigencia en la fila 153
    Fila 153
    154 Faltantes
    Falta el dato de iata en la fila 154
    Fila 154
    155 Faltantes
    Falta el dato de iata en la fila 155
    Falta el dato de numero_vuelos_origen en la fila 155
    Fila 155
    156 Faltantes
    Falta el dato de iata en la fila 156
    Falta el dato de fecha_vigencia en la fila 156
    Falta el dato de numero_vuelos_origen en la fila 156
    Fila 156
    157 Faltantes
    Falta el dato de iata en la fila 157
    Falta el dato de numero_vuelos_origen en la fila 157
    Fila 157
    158 Faltantes
    Falta el dato de iata en la fila 158
    Falta el dato de fecha_vigencia en la fila 158
    Fila 158
    159 Faltantes
    Falta el dato de iata en la fila 159
    Falta el dato de fecha_vigencia en la fila 159
    Falta el dato de numero_vuelos_origen en la fila 159
    Fila 159
    160 Faltantes
    Falta el dato de iata en la fila 160
    Falta el dato de fecha_vigencia en la fila 160
    Falta el dato de numero_vuelos_origen en la fila 160
    Fila 160
    161 Faltantes
    Falta el dato de iata en la fila 161
    Falta el dato de resolucion en la fila 161
    Falta el dato de fecha_vigencia en la fila 161
    Falta el dato de numero_vuelos_origen en la fila 161
    Fila 161
    162 Faltantes
    Falta el dato de iata en la fila 162
    Falta el dato de numero_vuelos_origen en la fila 162
    Fila 162
    163 Faltantes
    Falta el dato de iata en la fila 163
    Falta el dato de fecha_vigencia en la fila 163
    Fila 163
    164 Faltantes
    Falta el dato de iata en la fila 164
    Falta el dato de fecha_vigencia en la fila 164
    Falta el dato de numero_vuelos_origen en la fila 164
    Fila 164
    165 Faltantes
    Falta el dato de iata en la fila 165
    Falta el dato de fecha_vigencia en la fila 165
    Falta el dato de numero_vuelos_origen en la fila 165
    Fila 165
    166 Faltantes
    Falta el dato de iata en la fila 166
    Falta el dato de fecha_vigencia en la fila 166
    Falta el dato de numero_vuelos_origen en la fila 166
    Fila 166
    167 Faltantes
    Falta el dato de iata en la fila 167
    Falta el dato de fecha_vigencia en la fila 167
    Falta el dato de numero_vuelos_origen en la fila 167
    Fila 167
    168 Faltantes
    Falta el dato de iata en la fila 168
    Falta el dato de fecha_vigencia en la fila 168
    Fila 168
    169 Faltantes
    Falta el dato de iata en la fila 169
    Falta el dato de numero_vuelos_origen en la fila 169
    Fila 169
    170 Faltantes
    Falta el dato de iata en la fila 170
    Falta el dato de numero_vuelos_origen en la fila 170
    Fila 170
    171 Faltantes
    Falta el dato de iata en la fila 171
    Falta el dato de fecha_vigencia en la fila 171
    Falta el dato de numero_vuelos_origen en la fila 171
    Fila 171
    172 Faltantes
    Falta el dato de iata en la fila 172
    Falta el dato de fecha_vigencia en la fila 172
    Falta el dato de numero_vuelos_origen en la fila 172
    Fila 172
    173 Faltantes
    Falta el dato de iata en la fila 173
    Falta el dato de fecha_vigencia en la fila 173
    Fila 173
    174 Faltantes
    Falta el dato de iata en la fila 174
    Falta el dato de fecha_vigencia en la fila 174
    Falta el dato de numero_vuelos_origen en la fila 174
    Fila 174
    175 Faltantes
    Falta el dato de iata en la fila 175
    Falta el dato de fecha_vigencia en la fila 175
    Falta el dato de numero_vuelos_origen en la fila 175
    Fila 175
    176 Faltantes
    Falta el dato de iata en la fila 176
    Falta el dato de numero_vuelos_origen en la fila 176
    Fila 176
    177 Faltantes
    Falta el dato de iata en la fila 177
    Falta el dato de fecha_vigencia en la fila 177
    Falta el dato de numero_vuelos_origen en la fila 177
    Fila 177
    178 Faltantes
    Falta el dato de iata en la fila 178
    Falta el dato de numero_vuelos_origen en la fila 178
    Fila 178
    179 Faltantes
    Falta el dato de iata en la fila 179
    Falta el dato de numero_vuelos_origen en la fila 179
    Fila 179
    180 Faltantes
    Falta el dato de iata en la fila 180
    Falta el dato de fecha_vigencia en la fila 180
    Falta el dato de numero_vuelos_origen en la fila 180
    Fila 180
    181 Faltantes
    Falta el dato de iata en la fila 181
    Falta el dato de numero_vuelos_origen en la fila 181
    Fila 181
    182 Faltantes
    Falta el dato de iata en la fila 182
    Falta el dato de resolucion en la fila 182
    Falta el dato de numero_vuelos_origen en la fila 182
    Fila 182
    183 Faltantes
    Falta el dato de iata en la fila 183
    Falta el dato de propietario en la fila 183
    Falta el dato de fecha_vigencia en la fila 183
    Falta el dato de numero_vuelos_origen en la fila 183
    Fila 183
    184 Faltantes
    Falta el dato de iata en la fila 184
    Falta el dato de fecha_vigencia en la fila 184
    Falta el dato de numero_vuelos_origen en la fila 184
    Fila 184
    185 Faltantes
    Falta el dato de iata en la fila 185
    Falta el dato de fecha_vigencia en la fila 185
    Falta el dato de numero_vuelos_origen en la fila 185
    Fila 185
    186 Faltantes
    Falta el dato de iata en la fila 186
    Falta el dato de numero_vuelos_origen en la fila 186
    Fila 186
    187 Faltantes
    Falta el dato de iata en la fila 187
    Falta el dato de propietario en la fila 187
    Falta el dato de resolucion en la fila 187
    Falta el dato de numero_vuelos_origen en la fila 187
    Fila 187
    188 Faltantes
    Falta el dato de iata en la fila 188
    Falta el dato de fecha_vigencia en la fila 188
    Falta el dato de numero_vuelos_origen en la fila 188
    Fila 188
    189 Faltantes
    Falta el dato de iata en la fila 189
    Falta el dato de numero_vuelos_origen en la fila 189
    Fila 189
    190 Faltantes
    Falta el dato de iata en la fila 190
    Falta el dato de fecha_vigencia en la fila 190
    Falta el dato de numero_vuelos_origen en la fila 190
    Fila 190
    191 Faltantes
    Falta el dato de iata en la fila 191
    Falta el dato de fecha_vigencia en la fila 191
    Falta el dato de numero_vuelos_origen en la fila 191
    Fila 191
    192 Faltantes
    Falta el dato de iata en la fila 192
    Falta el dato de fecha_vigencia en la fila 192
    Fila 192
    193 Faltantes
    Falta el dato de iata en la fila 193
    Falta el dato de fecha_vigencia en la fila 193
    Falta el dato de numero_vuelos_origen en la fila 193
    Fila 193
    194 Faltantes
    Falta el dato de iata en la fila 194
    Fila 194
    195 Faltantes
    Falta el dato de iata en la fila 195
    Falta el dato de fecha_vigencia en la fila 195
    Fila 195
    196 Faltantes
    Falta el dato de iata en la fila 196
    Falta el dato de fecha_vigencia en la fila 196
    Fila 196
    197 Faltantes
    Falta el dato de iata en la fila 197
    Falta el dato de fecha_vigencia en la fila 197
    Fila 197
    198 Faltantes
    Falta el dato de iata en la fila 198
    Fila 198
    199 Faltantes
    Falta el dato de pbmo en la fila 199
    Falta el dato de fecha_vigencia en la fila 199
    Fila 199
    200 Faltantes
    Falta el dato de iata en la fila 200
    Falta el dato de fecha_vigencia en la fila 200
    Fila 200
    201 Faltantes
    Falta el dato de fecha_vigencia en la fila 201
    Fila 201
    202 Faltantes
    Falta el dato de iata en la fila 202
    Falta el dato de numero_vuelos_origen en la fila 202
    Fila 202
    203 Faltantes
    Falta el dato de fecha_vigencia en la fila 203
    Fila 203
    204 Faltantes
    Falta el dato de pbmo en la fila 204
    Falta el dato de fecha_vigencia en la fila 204
    Fila 204
    205 Faltantes
    Falta el dato de iata en la fila 205
    Falta el dato de numero_vuelos_origen en la fila 205
    Fila 205
    206 Faltantes
    Falta el dato de fecha_vigencia en la fila 206
    Fila 206
    207 Faltantes
    Falta el dato de iata en la fila 207
    Falta el dato de fecha_vigencia en la fila 207
    Fila 207
    208 Faltantes
    Falta el dato de iata en la fila 208
    Falta el dato de fecha_vigencia en la fila 208
    Fila 208
    209 Faltantes
    Falta el dato de iata en la fila 209
    Falta el dato de fecha_vigencia en la fila 209
    Fila 209
    210 Faltantes
    Falta el dato de iata en la fila 210
    Falta el dato de fecha_vigencia en la fila 210
    Fila 210
    211 Faltantes
    Falta el dato de pbmo en la fila 211
    Falta el dato de fecha_vigencia en la fila 211
    Fila 211
    212 Faltantes
    Falta el dato de fecha_vigencia en la fila 212
    Fila 212
    213 Faltantes
    Falta el dato de fecha_vigencia en la fila 213
    Fila 213
    214 Faltantes
    Falta el dato de iata en la fila 214
    Falta el dato de fecha_vigencia en la fila 214
    Fila 214
    215 Faltantes
    Falta el dato de pbmo en la fila 215
    Falta el dato de fecha_vigencia en la fila 215
    Fila 215
    216 Faltantes
    Falta el dato de iata en la fila 216
    Falta el dato de fecha_vigencia en la fila 216
    Fila 216
    217 Faltantes
    Falta el dato de iata en la fila 217
    Falta el dato de numero_vuelos_origen en la fila 217
    Fila 217
    218 Faltantes
    Falta el dato de iata en la fila 218
    Falta el dato de fecha_vigencia en la fila 218
    Falta el dato de numero_vuelos_origen en la fila 218
    Fila 218
    219 Faltantes
    Falta el dato de iata en la fila 219
    Falta el dato de fecha_vigencia en la fila 219
    Fila 219
    220 Faltantes
    Falta el dato de iata en la fila 220
    Falta el dato de pbmo en la fila 220
    Falta el dato de fecha_vigencia en la fila 220
    Fila 220
    221 Faltantes
    Falta el dato de iata en la fila 221
    Falta el dato de fecha_vigencia en la fila 221
    Fila 221
    222 Faltantes
    Falta el dato de iata en la fila 222
    Falta el dato de fecha_vigencia en la fila 222
    Fila 222
    223 Faltantes
    Falta el dato de iata en la fila 223
    Falta el dato de fecha_vigencia en la fila 223
    Fila 223
    224 Faltantes
    Falta el dato de iata en la fila 224
    Falta el dato de pbmo en la fila 224
    Falta el dato de fecha_vigencia en la fila 224
    Fila 224
    225 Faltantes
    Falta el dato de iata en la fila 225
    Falta el dato de fecha_vigencia en la fila 225
    Falta el dato de numero_vuelos_origen en la fila 225
    Fila 225
    226 Faltantes
    Falta el dato de iata en la fila 226
    Falta el dato de fecha_vigencia en la fila 226
    Fila 226
    227 Faltantes
    Falta el dato de iata en la fila 227
    Fila 227
    228 Faltantes
    Falta el dato de pbmo en la fila 228
    Falta el dato de fecha_vigencia en la fila 228
    Fila 228
    229 Faltantes
    Falta el dato de pbmo en la fila 229
    Falta el dato de fecha_vigencia en la fila 229
    Fila 229
    230 Faltantes
    Falta el dato de fecha_vigencia en la fila 230
    Fila 230
    231 Faltantes
    Falta el dato de pbmo en la fila 231
    Falta el dato de fecha_vigencia en la fila 231
    Fila 231
    232 Faltantes
    Falta el dato de pbmo en la fila 232
    Falta el dato de fecha_vigencia en la fila 232
    Fila 232
    233 Faltantes
    Falta el dato de pbmo en la fila 233
    Falta el dato de fecha_vigencia en la fila 233
    Fila 233
    234 Faltantes
    Falta el dato de pbmo en la fila 234
    Falta el dato de fecha_vigencia en la fila 234
    Fila 234
    235 Faltantes
    Falta el dato de pbmo en la fila 235
    Falta el dato de fecha_vigencia en la fila 235
    Fila 235
    236 Faltantes
    Falta el dato de pbmo en la fila 236
    Falta el dato de fecha_vigencia en la fila 236
    Falta el dato de numero_vuelos_origen en la fila 236
    Fila 236
    237 Faltantes
    Falta el dato de pbmo en la fila 237
    Falta el dato de fecha_vigencia en la fila 237
    Fila 237
    238 Faltantes
    Falta el dato de fecha_vigencia en la fila 238
    Fila 238
    239 Faltantes
    Falta el dato de iata en la fila 239
    Falta el dato de fecha_vigencia en la fila 239
    Fila 239
    240 Faltantes
    Falta el dato de fecha_vigencia en la fila 240
    Fila 240
    241 Faltantes
    Falta el dato de pbmo en la fila 241
    Falta el dato de fecha_vigencia en la fila 241
    Fila 241
    242 Faltantes
    Falta el dato de pbmo en la fila 242
    Falta el dato de fecha_vigencia en la fila 242
    Fila 242
    243 Faltantes
    Falta el dato de pbmo en la fila 243
    Falta el dato de fecha_vigencia en la fila 243
    Fila 243
    244 Faltantes
    Falta el dato de iata en la fila 244
    Falta el dato de fecha_vigencia en la fila 244
    Fila 244
    245 Faltantes
    Falta el dato de fecha_vigencia en la fila 245
    Fila 245
    246 Faltantes
    Falta el dato de pbmo en la fila 246
    Falta el dato de fecha_vigencia en la fila 246
    Fila 246
    247 Faltantes
    Falta el dato de fecha_vigencia en la fila 247
    Fila 247
    248 Faltantes
    Falta el dato de pbmo en la fila 248
    Falta el dato de fecha_vigencia en la fila 248
    Fila 248
    249 Faltantes
    Falta el dato de fecha_vigencia en la fila 249
    Fila 249
    250 Faltantes
    Falta el dato de fecha_vigencia en la fila 250
    Fila 250
    251 Faltantes
    Falta el dato de fecha_vigencia en la fila 251
    Fila 251
    252 Faltantes
    Falta el dato de pbmo en la fila 252
    Falta el dato de fecha_vigencia en la fila 252
    Fila 252
    253 Faltantes
    Falta el dato de fecha_vigencia en la fila 253
    Fila 253
    254 Faltantes
    Falta el dato de pbmo en la fila 254
    Falta el dato de fecha_vigencia en la fila 254
    Fila 254
    255 Faltantes
    Falta el dato de pbmo en la fila 255
    Falta el dato de fecha_vigencia en la fila 255
    Fila 255
    256 Faltantes
    Falta el dato de pbmo en la fila 256
    Falta el dato de fecha_vigencia en la fila 256
    Fila 256
    257 Faltantes
    Falta el dato de pbmo en la fila 257
    Falta el dato de fecha_vigencia en la fila 257
    Fila 257
    258 Faltantes
    Falta el dato de fecha_vigencia en la fila 258
    Fila 258
    259 Faltantes
    Falta el dato de iata en la fila 259
    Falta el dato de fecha_vigencia en la fila 259
    Fila 259
    260 Faltantes
    Falta el dato de pbmo en la fila 260
    Falta el dato de fecha_vigencia en la fila 260
    Fila 260
    261 Faltantes
    Falta el dato de pbmo en la fila 261
    Falta el dato de fecha_vigencia en la fila 261
    Fila 261
    262 Faltantes
    Falta el dato de fecha_vigencia en la fila 262
    Fila 262
    263 Faltantes
    Falta el dato de fecha_vigencia en la fila 263
    Fila 263
    264 Faltantes
    Falta el dato de pbmo en la fila 264
    Falta el dato de fecha_vigencia en la fila 264
    Fila 264
    265 Faltantes
    Falta el dato de pbmo en la fila 265
    Falta el dato de fecha_vigencia en la fila 265
    Fila 265
    266 Faltantes
    Falta el dato de pbmo en la fila 266
    Falta el dato de fecha_vigencia en la fila 266
    Fila 266
    267 Faltantes
    Falta el dato de iata en la fila 267
    Fila 267
    268 Faltantes
    Falta el dato de fecha_vigencia en la fila 268
    Fila 268
    269 Faltantes
    Falta el dato de pbmo en la fila 269
    Falta el dato de fecha_vigencia en la fila 269
    Fila 269
    270 Faltantes
    Falta el dato de fecha_vigencia en la fila 270
    Fila 270
    271 Faltantes
    Falta el dato de fecha_vigencia en la fila 271
    Fila 271
    272 Faltantes
    Falta el dato de pbmo en la fila 272
    Falta el dato de fecha_vigencia en la fila 272
    Fila 272
    273 Faltantes
    Falta el dato de pbmo en la fila 273
    Falta el dato de fecha_vigencia en la fila 273
    Fila 273
    274 Faltantes
    Falta el dato de pbmo en la fila 274
    Falta el dato de fecha_vigencia en la fila 274
    Fila 274
    275 Faltantes
    Falta el dato de iata en la fila 275
    Falta el dato de fecha_vigencia en la fila 275
    Fila 275
    276 Faltantes
    Falta el dato de pbmo en la fila 276
    Falta el dato de fecha_vigencia en la fila 276
    Fila 276
    277 Faltantes
    Falta el dato de iata en la fila 277
    Falta el dato de pbmo en la fila 277
    Falta el dato de fecha_vigencia en la fila 277
    Fila 277
    278 Faltantes
    Falta el dato de fecha_vigencia en la fila 278
    Fila 278
    279 Faltantes
    Falta el dato de fecha_vigencia en la fila 279
    Fila 279
    280 Faltantes
    Falta el dato de fecha_vigencia en la fila 280
    Fila 280
    281 Faltantes
    Falta el dato de fecha_vigencia en la fila 281
    Fila 281
    282 Faltantes
    Falta el dato de fecha_vigencia en la fila 282
    Fila 282
    283 Faltantes
    Falta el dato de pbmo en la fila 283
    Falta el dato de fecha_vigencia en la fila 283
    Fila 283
    284 Faltantes
    Falta el dato de pbmo en la fila 284
    Falta el dato de fecha_vigencia en la fila 284
    Fila 284
    285 Faltantes
    Falta el dato de pbmo en la fila 285
    Falta el dato de fecha_vigencia en la fila 285
    Fila 285
    286 Faltantes
    Falta el dato de pbmo en la fila 286
    Falta el dato de fecha_vigencia en la fila 286
    Fila 286
    287 Faltantes
    Falta el dato de iata en la fila 287
    Falta el dato de fecha_vigencia en la fila 287
    Fila 287
    288 Faltantes
    Falta el dato de iata en la fila 288
    Falta el dato de fecha_vigencia en la fila 288
    Fila 288
    289 Faltantes
    Falta el dato de iata en la fila 289
    Falta el dato de fecha_vigencia en la fila 289
    Fila 289
    290 Faltantes
    Falta el dato de iata en la fila 290
    Fila 290
    291 Faltantes
    291 Filas incompletas
    %Ctabla = 


.. code:: ipython3

    aeropuertos_df.collect()[-1]['sigla']




.. parsed-literal::

    'ZPS'



.. code:: ipython3

    aeropuertos_df.where((aeropuertos_df['iata'] != 'nan') & (aeropuertos_df['fecha_vigencia'] != 'nan') ).count()




.. parsed-literal::

    0



4.2. ‘Cobertura de Aerea de Centros Poblados por Categoria de Aeropuerto1.csv’
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: ipython3

    # Completitud por columnas

.. code:: ipython3

    NTF = cobertura_df.count()
    for i in cobertura_df.columns:
        NVI = cobertura_df.select(i).where(cobertura_df[i] == 'nan').count()
        complet = (1  - (NVI/NTF)) * 100
        print('|',i,'|', round(float(complet),1),'|')


.. parsed-literal::

    | Centro Poblado | 100.0 |
    | Aeropuerto | 100.0 |
    | Distancia(Km) | 100.0 |
    | Cobertura | 100.0 |
    | Aerodromo | 100.0 |
    | D_Aerodromo | 100.0 |
    | Regional | 100.0 |
    | D_Regional | 100.0 |
    | Nacional | 100.0 |
    | D_Nacional | 100.0 |
    | Internacional | 100.0 |
    | D_Internacional | 100.0 |
    | Tipo_Cobertura | 100.0 |


4.3. ‘Matriz de distancias entre aeropuertos y centros poblados de Colombia1.csv’
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: ipython3

    # Completitud por columnas

.. code:: ipython3

    distancias_df




.. parsed-literal::

    DataFrame[Unnamed: 0: string, LA ESCONDIDA: double, MORICHITO: double, CAROLINA DEL PRINCIPE: double, DUBAI: double, BARU - HIDROPUERTO: double, LA CAROLINA: string, SAN FELIPE DEL PAUTO: double, VELASQUEZ: string, LA UNION: double, LA ILUSION: double, LA VENTUROSA: double, GUAYABAL DEL CRAVO: double, LAS VIOLETAS- CA: double, LOS MANGOS: double, EL CONDOR: double, HOTEL SAN DIEGO: double, EL CAFUCHE: double, GUACHARACAS (COLOMBAIMA): double, GAVILAN DE LA PASCUA: double, DOROTEA B1: double, HORIZONTES: double, MACOLLA: double, MULETOS- CA: double, LA MAPORA: double, LLANO CAUCHO: double, ARMENIA: double, RANCHO COLIBRI - CA: double, EL CAIRANO: double, NUEVA ROMA: double, JAGUAR: double, OCELOTE: double, GETSEMANI: double, URACA - CA.: double, SAN ROQUE  - CA.: double, SAN LUIS DE PACA: double, CANANARI: double, MARAREY: double, SANTA CLARA: double, SAN PABLO: double, COROCITO: double, LAS FURIAS: double, EL NOGAL: double, SANTA CRUZ: double, CURUMANI: string, SAN MIGUEL: string, CANTADELICIAS: double, LAS VEGAS: double, LA FRANCIA: double, SAN FELIPE: string, JULIAN: double, LOS HALCONES: string, EL SOÑADOR: double, EL CONCHAL - CA: double, SEVILLA: double, ASA SAN MARTIN: double, LA FAZENDA: double, LA ESTRELLA-CA.: double, LAS AGUILAS -CA: double, CAMARUCOS: double, EL TOTUMO: double, VARSOVIA: double, LA HERMOSA: double, SAN ESTEBAN: double, SAN JOSE DEL ARIPORO: double, COLINERAS: double, LA CAIMANA: double, EMAUS: double, SANTA MARIA DEL CAFÉ: double, MIRAMAR DE GUANAPALO: double, LA SALVACION: double, LOMA GRANDE - CA: double, EL CAPRICHO: double, COROCORA: double, GRISMANIA: double, PALMAS DE TUMACO  -CA: double, INGENIO LA CARMELITA: double, EL LAGO - CA: double, HACIENDA LA JOYA: double, LA PASTORA: double, TALANQUERA: double, VILLA GEORGINA: double, INGENIO PICHICHI: double, SAN SEBASTIAN: double, EL COROZO: double, HATO VIEJO: double, GUADUALITO: string, LA REDENCION: double, LA PONDEROSA: string, LOS REMANSOS: double, SAN LUIS: string, LOS GAVANES (LA MATA): double, EL RODEO: string, EL PALMAR: double, LA GAITANA: double, SAN ISIDRO II: double, EL DELIRIO: double, LA MONICA: double, LA ABEJITA: double, CACHIMBALITO - CA: double, MADREVIEJA: double, SAN JUAN: double, LA FORTUNA: double, AGROFORESTAL MATA AZUL: double, VILLA ISABELLA: double, MIRADOR: double, EL CARIBE: double, EL PEDRAL BONANZA: double, SAN NICOLAS: double, LOS LOBOS: double, EL RASTRO: double, LOS GANSOS: double, BETANIA: double, AGUAS CLARAS: double, LAS NUBES: double, CAMPO ALEGRE: string, CAÑO COLORADO: double, FORTUL: double, AGUA BLANCA: string, ALCIDES FERNANDEZ: double, EL MONASTERIO: double, AGUACLARA: double, ACAPULCO: double, ARARACUARA: double, GUSTAVO ROJAS PINILLA: double, AEROFLANDES - C.A.: double, EL RIO: double, HACARITAMA: double, MARIA ANGELICA: double, EL DIAMANTE: double, ANTONIO ROLDAN BETANCOURT: double, ARBOLETES: double, EL TRONCAL: double, ATACO: double, SANTIAGO PEREZ QUIROZ: double, EL CORAJE: string, INGENIO RISARALDA: double, BANCO LARGO: double, GUAICARAMO: double, PIZARRO: double, BECERRIL: string, BERASTEGUI: double, BIZERTA: double, BARRANCO MINAS: double, BOLUGA: double, BUENOS AIRES: double, BUENOS AIRES -FADELCE: double, EL DORADO: double, BUENAVENTURA-GERARDO TOBAR LOPEZ: double, ALFONSO BONILLA ARAGON: double, CONDOTO MANDINGA: double, NAVAS PARDO: double, RAFAEL NUÑEZ: double, CAMILO DAZA: double, CAMILO DAZA No.2: double, LAS BRUJAS: double, YARIGUIES: double, EL JUNCAL: double, LAS FLORES: double, EL ALCARAVAN: double, GUSTAVO ARTUNDUAGA PAREDES: double, JUAN CASIANO: double, FLAMINIO S. CAMACHO: double, HATO COROZAL: double, PERALES: double, JAIME ORTIZ BETANCUR: double, MIRAFLORES: double, BARACOA: double, SAN BERNARDO: double, JOSE CELESTINO: double, EL PINDO: double, LOS GARZONES: double, FABIO A. LEON BENTLEY: double, REYES MURILLO: double, BENITO SALAS: double, EL MEDANO: double, REMEDIOS OTU: double, GERMAN OLANO: double, PAIPA JUAN JOSE RONDON: double, GUILLERMO LEON VALENCIA: double, ANTONIO NARIÑO: double, CONTADOR: double, GEMELOS DORADOS: double, TRES DE MAYO: double, EL EMBRUJO: double, PAZ DE ARIPORO: double, CRAVO NORTE: double, ALMIRANTE PADILLA: double, COLONIZADORES: double, SIMON BOLIVAR: double, SAN MARTIN: double, EDUARDO FALLA SOLANO: double, LA FLORIDA: double, TRINIDAD: double, TOLU: double, GUSTAVO VARGAS: double, TABLON DE TAMARA: double, EL CARAÑO: double, ALI PIEDRAHITA: double, CANANGUCHAL: double, ALFONSO LOPEZ PUMAREJO: double, VANGUARDIA: double, SANTA ISABEL: double, YAPIMA: double, TROMPILLOS: double, GERMAN ALBERTO: double, GUILLERMO GOMEZ ORTIZ: double]



.. code:: ipython3

    distancias_df.count()




.. parsed-literal::

    5512



.. code:: ipython3

    distancias_df.select('LA ESCONDIDA').where(distancias_df['LA ESCONDIDA'].isNotNull()).count()/distancias_df.count()




.. parsed-literal::

    0.9976415094339622



.. code:: ipython3

    distancias_df.select('GAVILAN DE LA PASCUA').where(distancias_df['GAVILAN DE LA PASCUA'].isNotNull()).count()




.. parsed-literal::

    5499



.. code:: ipython3

    NTF = distancias_df.count()
    for i in distancias_df.columns:
        try:
            NVI = distancias_df.select(i).where(distancias_df[i].isNull()).count()
            complet = (1  - (NVI/NTF)) * 100
            print('|',i,'|', round(float(complet),2),'|')
        except:
            print('|',i,'|', 'Se requiere modificar nombre','|')


.. parsed-literal::

    | Unnamed: 0 | 100.0 |
    | LA ESCONDIDA | 99.76 |
    | MORICHITO | 99.76 |
    | CAROLINA DEL PRINCIPE | 99.76 |
    | DUBAI | 99.76 |
    | BARU - HIDROPUERTO | 99.76 |
    | LA CAROLINA | 0.0 |
    | SAN FELIPE DEL PAUTO | 99.76 |
    | VELASQUEZ | 0.0 |
    | LA UNION | 99.76 |
    | LA ILUSION | 99.76 |
    | LA VENTUROSA | 99.76 |
    | GUAYABAL DEL CRAVO | 99.76 |
    | LAS VIOLETAS- CA | 99.76 |
    | LOS MANGOS | 99.76 |
    | EL CONDOR | 99.76 |
    | HOTEL SAN DIEGO | 99.76 |
    | EL CAFUCHE | 99.76 |
    | GUACHARACAS (COLOMBAIMA) | 99.76 |
    | GAVILAN DE LA PASCUA | 99.76 |
    | DOROTEA B1 | 99.76 |
    | HORIZONTES | 99.76 |
    | MACOLLA | 99.76 |
    | MULETOS- CA | 99.76 |
    | LA MAPORA | 99.76 |
    | LLANO CAUCHO | 99.76 |
    | ARMENIA | 99.76 |
    | RANCHO COLIBRI - CA | 99.76 |
    | EL CAIRANO | 99.76 |
    | NUEVA ROMA | 99.76 |
    | JAGUAR | 99.76 |
    | OCELOTE | 99.76 |
    | GETSEMANI | 99.76 |
    | URACA - CA. | Se requiere modificar nombre |
    | SAN ROQUE  - CA. | Se requiere modificar nombre |
    | SAN LUIS DE PACA | 99.76 |
    | CANANARI | 99.76 |
    | MARAREY | 99.76 |
    | SANTA CLARA | 99.76 |
    | SAN PABLO | 99.76 |
    | COROCITO | 99.76 |
    | LAS FURIAS | 99.76 |
    | EL NOGAL | 99.76 |
    | SANTA CRUZ | 99.76 |
    | CURUMANI | 0.0 |
    | SAN MIGUEL | 0.0 |
    | CANTADELICIAS | 99.76 |
    | LAS VEGAS | 99.76 |
    | LA FRANCIA | 99.76 |
    | SAN FELIPE | 0.0 |
    | JULIAN | 99.76 |
    | LOS HALCONES | 0.0 |
    | EL SOÑADOR | 99.76 |
    | EL CONCHAL - CA | 99.76 |
    | SEVILLA | 99.76 |
    | ASA SAN MARTIN | 99.76 |
    | LA FAZENDA | 99.76 |
    | LA ESTRELLA-CA. | Se requiere modificar nombre |
    | LAS AGUILAS -CA | 99.76 |
    | CAMARUCOS | 99.76 |
    | EL TOTUMO | 99.76 |
    | VARSOVIA | 99.76 |
    | LA HERMOSA | 99.76 |
    | SAN ESTEBAN | 99.76 |
    | SAN JOSE DEL ARIPORO | 99.76 |
    | COLINERAS | 99.76 |
    | LA CAIMANA | 99.76 |
    | EMAUS | 99.76 |
    | SANTA MARIA DEL CAFÉ | 99.76 |
    | MIRAMAR DE GUANAPALO | 99.76 |
    | LA SALVACION | 99.76 |
    | LOMA GRANDE - CA | 99.76 |
    | EL CAPRICHO | 99.76 |
    | COROCORA | 99.76 |
    | GRISMANIA | 99.76 |
    | PALMAS DE TUMACO  -CA | 99.76 |
    | INGENIO LA CARMELITA | 99.76 |
    | EL LAGO - CA | 99.76 |
    | HACIENDA LA JOYA | 99.76 |
    | LA PASTORA | 99.76 |
    | TALANQUERA | 99.76 |
    | VILLA GEORGINA | 99.76 |
    | INGENIO PICHICHI | 99.76 |
    | SAN SEBASTIAN | 99.76 |
    | EL COROZO | 99.76 |
    | HATO VIEJO | 99.76 |
    | GUADUALITO | 0.0 |
    | LA REDENCION | 99.76 |
    | LA PONDEROSA | 0.0 |
    | LOS REMANSOS | 99.76 |
    | SAN LUIS | 0.0 |
    | LOS GAVANES (LA MATA) | 99.76 |
    | EL RODEO | 0.0 |
    | EL PALMAR | 99.76 |
    | LA GAITANA | 99.76 |
    | SAN ISIDRO II | 99.76 |
    | EL DELIRIO | 99.76 |
    | LA MONICA | 99.76 |
    | LA ABEJITA | 99.76 |
    | CACHIMBALITO - CA | 99.76 |
    | MADREVIEJA | 99.76 |
    | SAN JUAN | 99.76 |
    | LA FORTUNA | 99.76 |
    | AGROFORESTAL MATA AZUL | 99.76 |
    | VILLA ISABELLA | 99.76 |
    | MIRADOR | 99.76 |
    | EL CARIBE | 99.76 |
    | EL PEDRAL BONANZA | 99.76 |
    | SAN NICOLAS | 99.76 |
    | LOS LOBOS | 99.76 |
    | EL RASTRO | 99.76 |
    | LOS GANSOS | 99.76 |
    | BETANIA | 99.76 |
    | AGUAS CLARAS | 99.76 |
    | LAS NUBES | 99.76 |
    | CAMPO ALEGRE | 0.0 |
    | CAÑO COLORADO | 99.76 |
    | FORTUL | 99.76 |
    | AGUA BLANCA | 0.0 |
    | ALCIDES FERNANDEZ | 99.76 |
    | EL MONASTERIO | 99.76 |
    | AGUACLARA | 99.76 |
    | ACAPULCO | 99.76 |
    | ARARACUARA | 99.76 |
    | GUSTAVO ROJAS PINILLA | 99.76 |
    | AEROFLANDES - C.A. | Se requiere modificar nombre |
    | EL RIO | 99.76 |
    | HACARITAMA | 99.76 |
    | MARIA ANGELICA | 99.76 |
    | EL DIAMANTE | 99.76 |
    | ANTONIO ROLDAN BETANCOURT | 99.76 |
    | ARBOLETES | 99.76 |
    | EL TRONCAL | 99.76 |
    | ATACO | 99.76 |
    | SANTIAGO PEREZ QUIROZ | 99.76 |
    | EL CORAJE | 0.0 |
    | INGENIO RISARALDA | 99.76 |
    | BANCO LARGO | 99.76 |
    | GUAICARAMO | 99.76 |
    | PIZARRO | 99.76 |
    | BECERRIL | 0.0 |
    | BERASTEGUI | 99.76 |
    | BIZERTA | 99.76 |
    | BARRANCO MINAS | 99.76 |
    | BOLUGA | 99.76 |
    | BUENOS AIRES | 99.76 |
    | BUENOS AIRES -FADELCE | 99.76 |
    | EL DORADO | 99.76 |
    | BUENAVENTURA-GERARDO TOBAR LOPEZ | 99.76 |
    | ALFONSO BONILLA ARAGON | 99.76 |
    | CONDOTO MANDINGA | 99.76 |
    | NAVAS PARDO | 99.76 |
    | RAFAEL NUÑEZ | 99.76 |
    | CAMILO DAZA | 99.76 |
    | CAMILO DAZA No.2 | Se requiere modificar nombre |
    | LAS BRUJAS | 99.76 |
    | YARIGUIES | 99.76 |
    | EL JUNCAL | 99.76 |
    | LAS FLORES | 99.76 |
    | EL ALCARAVAN | 99.76 |
    | GUSTAVO ARTUNDUAGA PAREDES | 99.76 |
    | JUAN CASIANO | 99.76 |
    | FLAMINIO S. CAMACHO | Se requiere modificar nombre |
    | HATO COROZAL | 99.76 |
    | PERALES | 99.76 |
    | JAIME ORTIZ BETANCUR | 99.76 |
    | MIRAFLORES | 99.76 |
    | BARACOA | 99.76 |
    | SAN BERNARDO | 99.76 |
    | JOSE CELESTINO | 99.76 |
    | EL PINDO | 99.76 |
    | LOS GARZONES | 99.76 |
    | FABIO A. LEON BENTLEY | Se requiere modificar nombre |
    | REYES MURILLO | 99.76 |
    | BENITO SALAS | 99.76 |
    | EL MEDANO | 99.76 |
    | REMEDIOS OTU | 99.76 |
    | GERMAN OLANO | 99.76 |
    | PAIPA JUAN JOSE RONDON | 99.76 |
    | GUILLERMO LEON VALENCIA | 99.76 |
    | ANTONIO NARIÑO | 99.76 |
    | CONTADOR | 99.76 |
    | GEMELOS DORADOS | 99.76 |
    | TRES DE MAYO | 99.76 |
    | EL EMBRUJO | 99.76 |
    | PAZ DE ARIPORO | 99.76 |
    | CRAVO NORTE | 99.76 |
    | ALMIRANTE PADILLA | 99.76 |
    | COLONIZADORES | 99.76 |
    | SIMON BOLIVAR | 99.76 |
    | SAN MARTIN | 99.76 |
    | EDUARDO FALLA SOLANO | 99.76 |
    | LA FLORIDA | 99.76 |
    | TRINIDAD | 99.76 |
    | TOLU | 99.76 |
    | GUSTAVO VARGAS | 99.76 |
    | TABLON DE TAMARA | 99.76 |
    | EL CARAÑO | 99.76 |
    | ALI PIEDRAHITA | 99.76 |
    | CANANGUCHAL | 99.76 |
    | ALFONSO LOPEZ PUMAREJO | 99.76 |
    | VANGUARDIA | 99.76 |
    | SANTA ISABEL | 99.76 |
    | YAPIMA | 99.76 |
    | TROMPILLOS | 99.76 |
    | GERMAN ALBERTO | 99.76 |
    | GUILLERMO GOMEZ ORTIZ | 99.76 |


4.4. ‘vuelos.csv’
^^^^^^^^^^^^^^^^^

.. code:: ipython3

    # Completitud de columnas

.. code:: ipython3

    vuelos_df.select('origen').show()


.. parsed-literal::

    +------+
    |origen|
    +------+
    |   BOG|
    |   UIB|
    |   IBE|
    |   FLA|
    |   CUC|
    |   CUC|
    |   7NS|
    |   AUC|
    |   CLO|
    |   BOG|
    |   MVP|
    |   BOG|
    |   BOG|
    |   NVA|
    |   BOG|
    |   BOG|
    |   EYP|
    |   PUU|
    |   BOG|
    |   BOG|
    +------+
    only showing top 20 rows
    


.. code:: ipython3

    NTF = vuelos_df.count()
    for i in vuelos_df.columns:
        try:
            NVI = vuelos_df.select(i).where(vuelos_df[i] == 'nan').count()
            complet = (1  - (NVI/NTF)) * 100
            print('|',i,'|', round(float(complet),2),'|')
        except:
            print('|',i,'|', 'Se requiere modificar nombre','|')


.. parsed-literal::

    | ano | 100.0 |
    | mes | 100.0 |
    | origen | 100.0 |
    | destino | 100.0 |
    | tipo_equipo | 100.0 |
    | tipo_vuelo | 100.0 |
    | trafico | 98.68 |
    | empresa | 100.0 |
    | vuelos | 97.66 |
    | sillas | 94.06 |
    | carga_ofrecida | 100.0 |
    | pasajeros | 94.85 |
    | carga_bordo | 100.0 |




.. code:: ipython3

    vuelos_df.select('tipo_vuelo').distinct().show()


.. parsed-literal::

    +----------+
    |tipo_vuelo|
    +----------+
    |         T|
    |         C|
    |         A|
    |         R|
    +----------+
    


.. code:: ipython3

    cobertura_df.select('Aeropuerto').sort('Aeropuerto').show()


.. parsed-literal::

    +------------------+
    |        Aeropuerto|
    +------------------+
    |          ACAPULCO|
    |AEROFLANDES - C.A.|
    |AEROFLANDES - C.A.|
    |AEROFLANDES - C.A.|
    |         AGUACLARA|
    |         AGUACLARA|
    |         AGUACLARA|
    |         AGUACLARA|
    |         AGUACLARA|
    |         AGUACLARA|
    |         AGUACLARA|
    |         AGUACLARA|
    |         AGUACLARA|
    |         AGUACLARA|
    |         AGUACLARA|
    |         AGUACLARA|
    |         AGUACLARA|
    |         AGUACLARA|
    |         AGUACLARA|
    |         AGUACLARA|
    +------------------+
    only showing top 20 rows
    


.. code:: ipython3

    aeropuertos_df.select('nombre').sort('nombre').show()


.. parsed-literal::

    +--------------------+
    |              nombre|
    +--------------------+
    |            ACAPULCO|
    |            ACAPULCO|
    |  AEROFLANDES - C.A.|
    |  AEROFLANDES - C.A.|
    |AGROFORESTAL MATA...|
    |         AGUA BLANCA|
    |         AGUA BLANCA|
    |           AGUACLARA|
    |        AGUAS CLARAS|
    |        AGUAS CLARAS|
    |        AGUAS CLARAS|
    |   ALCIDES FERNANDEZ|
    |   ALCIDES FERNANDEZ|
    |ALFONSO BONILLA A...|
    |ALFONSO LOPEZ PUM...|
    |ALFONSO LOPEZ PUM...|
    |      ALI PIEDRAHITA|
    |   ALMIRANTE PADILLA|
    |      ANTONIO NARIÑO|
    |ANTONIO ROLDAN BE...|
    +--------------------+
    only showing top 20 rows
    


from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

def clean_json_data(json_data, id_list):
    """
    Limpia la lista de JSONs de mercado y asocia cada conjunto de datos con su respectivo identificador.

    Parameters:
    json_data (list): Una lista de diccionarios en formato JSON.
    id_list (list): Una lista de identificadores correspondientes a cada conjunto de datos en json_data.

    Returns:
    list: Una lista de diccionarios limpios con sus respectivos identificadores o diccionarios vacíos si hay algún error en los datos.
    """
    print(">>> Limpiando datos")
    cleaned_data = []

    for data, id in zip(json_data, id_list):
        if isinstance(data, dict):
            data['id'] = id  # Agregar el identificador a los datos
            cleaned_data.append(data)
        else:
            print(f">>> Data inválida: {data}")
            cleaned_data.append({})

    print(">>> Limpieza finalizada")
    return cleaned_data

def json_to_df_market_chart(json_data, id_list, spark_session):
    """
    Convierte los datos de mercado en formato JSON en un DataFrame de PySpark
    y realiza algunas transformaciones y asignaciones de columnas.

    Parameters:
    json_data (list): Una lista de datos de mercado en formato JSON.
    id_list (list): Una lista de identificadores correspondientes a cada conjunto de datos en json_data.
    spark_session (pyspark.sql.SparkSession): La sesión de Spark.

    Returns:
    pyspark.sql.DataFrame: Un DataFrame que contiene los datos de mercado procesados.
    """
    cleaned_data = clean_json_data(json_data, id_list)

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("prices", FloatType(), True),  # Cambiar el tipo de dato a FloatType
        StructField("market_caps", FloatType(), True),  # Cambiar el tipo de dato a FloatType
        StructField("total_volumes", FloatType(), True),  # Cambiar el tipo de dato a FloatType
        StructField("timestamp", LongType(), True)  # Cambiar el tipo de dato a LongType para date_unix
    ])

    df = spark_session.createDataFrame([], schema)

    for data in cleaned_data:
        temp_df = spark_session.createDataFrame([data], schema)
        df = df.union(temp_df)

    df = df.withColumn('timestamp', col('timestamp') // 1000)  # Convertir la fecha a segundos (date_unix)
    df = df.withColumn('cripto', col('id'))  # Agregar la columna cripto como copia del campo id

    print(">>> DataFrame listo para la carga")
    return df



def transformation_top(json, spark_session):
    """
    Transforma los datos de las 100 criptomonedas de mayor capitalización obtenidos de la API de CoinGecko
    en un DataFrame de Pandas y selecciona las columnas deseadas.

    Parameters:
    json (dict): Los datos de las 100 criptomonedas en formato JSON.

    Returns:
    pyspark.sql.DataFrame: Un DataFrame que contiene las columnas seleccionadas.
    """
    df = spark_session.read.json(
        spark_session.sparkContext.parallelize(json),
        multiLine = True
    )

    selected_columns = [
        'id',
        'symbol',
        'name',
        'current_price',
        'market_cap',
        'market_cap_rank',
        'total_volume',
        'high_24h',
        'low_24h',
        'price_change_24h',
        'price_change_percentage_24h',
        'market_cap_change_24h',
        'market_cap_change_percentage_24h',
        'circulating_supply',
        'ath',
        'ath_change_percentage',
        'ath_date',
        'atl',
        'atl_change_percentage',
        'atl_date',
        'last_updated',
    ]
    df = df.select(selected_columns)

    print(">>> Columnas filtradas.")
    
    return df
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, format_number
def json_to_df_market_chart(data, spark_session):
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
    schema = StructType([
        StructField("prices", FloatType(), True),
        StructField("market_caps", FloatType(), True),
        StructField("total_volumes", FloatType(), True),
        StructField("date_unix", LongType(), True),
        StructField("id", StringType(), True) 
    ])

    def process_json(data):
        prices = [float(prices_list[1]) if prices_list[1] is not None else 0.0 for prices_list in data[1]["prices"]]
        market_caps = [float(market_caps_list[1]) if market_caps_list[1] is not None else 0.0 for market_caps_list in data[1]["market_caps"]]
        total_volumes = [float(total_volumes_list[1]) if total_volumes_list[1] is not None else 0.0 for total_volumes_list in data[1]["total_volumes"]]
        timestamps = [timestamps_list[0] for timestamps_list in data[1]["prices"]]
        join = zip(prices, market_caps, total_volumes, timestamps)
        join_list = list(join)
        join_list = [(item + (data[0],)) for item in join_list]
        return spark_session.createDataFrame(join_list, schema)

    # Procesar cada json con la función map
    dfs = map(process_json, data)

    # Unir todos los DataFrames en uno solo con la función reduce
    final_df = reduce(DataFrame.union, dfs)

    # Formatear la columna "market_caps" sin notación científica
    final_df = final_df.withColumn("market_caps", format_number(col("market_caps"), 2))

    # Reordenar las columnas del DataFrame para que coincidan con el orden de la tabla
    final_df = final_df.select("id", "prices", "total_volumes", "market_caps", "date_unix")
    # Mostrar el DataFrame con market_caps formateado
    final_df.show()
    
    return final_df
        
        
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
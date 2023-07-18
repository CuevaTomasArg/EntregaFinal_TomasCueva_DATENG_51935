from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, format_number

def json_to_df_market_chart(data, spark_session):
    """
    Convierte los datos de mercado en formato JSON en un DataFrame de PySpark
    y realiza algunas transformaciones y asignaciones de columnas.

    Parameters:
        data (list): Una lista de datos de mercado en formato JSON.
        spark_session (pyspark.sql.SparkSession): La sesión de Spark.

    Returns:
        pyspark.sql.DataFrame: Un DataFrame que contiene los datos de mercado procesados.
    """

    def process_json(data, schema):
        prices = [float(prices_list[1]) if prices_list[1] is not None else 0.0 for prices_list in data[1]["prices"]]
        market_caps = [float(market_caps_list[1]) if market_caps_list[1] is not None else 0.0 for market_caps_list in data[1]["market_caps"]]
        total_volumes = [float(total_volumes_list[1]) if total_volumes_list[1] is not None else 0.0 for total_volumes_list in data[1]["total_volumes"]]
        timestamps = [timestamps_list[0] for timestamps_list in data[1]["prices"]]
        join = zip(prices, market_caps, total_volumes, timestamps)
        join_list = list(join)
        join_list = [(item + (data[0],)) for item in join_list]
        return spark_session.createDataFrame(join_list, schema)

    data_cleaned = [element for element in data if element is not None]
    
    schema = StructType([
        StructField("prices", FloatType(), True),
        StructField("market_caps", FloatType(), True),
        StructField("total_volumes", FloatType(), True),
        StructField("date_unix", LongType(), True),
        StructField("id", StringType(), True) 
    ])
    
    dfs = map(process_json, data_cleaned, schema)
    final_df = reduce(DataFrame.union, dfs)
    final_df = final_df.withColumn("market_caps", format_number(col("market_caps"), 2))
    final_df = final_df.select("id", "prices", "total_volumes", "market_caps", "date_unix")
    
    return final_df

def transformation_top(json, spark_session):
    """
    Transforma los datos de las 100 criptomonedas de mayor capitalización obtenidos de la API de CoinGecko
    en un DataFrame de Pandas y selecciona las columnas deseadas.

    Parameters:
        json (dict): Los datos de las 100 criptomonedas en formato JSON.
        spark_session (pyspark.sql.SparkSession): La sesión de Spark.

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
    
    return df
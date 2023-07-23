from pyspark.sql.types import StructType, StructField, LongType, FloatType
from pyspark.sql.functions import lit

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
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ENTRO AL MODULO json_to_df_market_chart")
    
    def create_df(data, value):
        schema = StructType([
            StructField("date_unix", LongType(), True),
            StructField(f"{value}", FloatType(), True),
        ])
        list_set = [(item[0], float(item[1])) if isinstance(item[1], (int, float)) else item for item in data]
        return spark_session.createDataFrame(list_set, schema)
    
    def process_data(tuple):
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ENTRO AL MODULO PROCESS_DATA")
        json = tuple[1]
        df_final = None
        for key, value in json.items():
            df = create_df(value, key)
            
            if df_final is None:
                df_final = df
            else:
                df_final = df_final.join(df, "date_unix", "outer")
                
        df_final = df_final.withColumn("id", lit(tuple[0]))
        df_final = df_final.select("id", "prices", "total_volumes", "volumes", "date_unix")
        return df_final

    data_cleaned = [element for element in data if element is not None]

    dfs = list(map(process_data, data_cleaned))

    for df in dfs:
        df.show()

    return dfs


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
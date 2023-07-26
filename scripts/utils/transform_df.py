from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql.functions import lit, expr
from functools import reduce

def clean_number(number):
    if number is not None:
        decimal_index = number.find('.')
        
        if decimal_index != -1:
            integer_part = number[:decimal_index]
            decimal_part = number[decimal_index + 1:decimal_index + 4].ljust(3, '0')
            cleaned_number = f"{integer_part}.{decimal_part}"
            return cleaned_number
        else:
            return number
    else:
        return None

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
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ENTRO A LA TRANSFORMACION")
    
    # Creo que el error venia de acá
    spark_session.udf.register("clean_number", clean_number)

    def process_data(tuple):
        json = tuple[1]
        df_dict = {}
        for key, value in json.items():
            schema = StructType([
                StructField("date_unix", LongType(), True),
                StructField(f"{key}", StringType(), True),
            ])
            df = spark_session.createDataFrame(value, schema)
            df = df.withColumn(f"{key}", expr(f"clean_number({key})").alias(f"{key}"))
            df_dict[key] = df
            
        df_final = reduce(lambda df1, df2: df1.join(df2, "date_unix", "outer"), df_dict.values())
        df_final = df_final.withColumn("id", lit(tuple[0]))
        df_final = df_final.select("id", "prices", "total_volumes", "market_caps", "date_unix")
        return df_final

    data_cleaned = [element for element in data if element is not None]
    dfs = list(map(process_data, data_cleaned))
    df = reduce(lambda df1, df2: df1.union(df2), dfs)

    df.show()
    return df


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
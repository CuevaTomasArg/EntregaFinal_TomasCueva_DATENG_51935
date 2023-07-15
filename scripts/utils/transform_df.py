from pyspark.sql.functions import col, expr

def transformation_top(json, spark_session):
    """
    Transforma los datos de las 100 criptomonedas de mayor capitalización obtenidos de la API de CoinGecko
    en un DataFrame de Pandas y selecciona las columnas deseadas.

    Parameters:
    json (dict): Los datos de las 100 criptomonedas en formato JSON.

    Returns:
    pyspark.sql.DataFrame: Un DataFrame que contiene las columnas seleccionadas.
    """
     # Crear un DataFrame de PySpark a partir de los datos JSON
    df = spark_session.read.json(
        spark_session.sparkContext.parallelize(json),
        multiLine = True
    )

    # Seleccionar las columnas deseadas
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

def json_to_df_market_chart(json, cripto, spark_session):
    """
    Convierte los datos de mercado en formato JSON en un DataFrame de PySpark
    y realiza algunas transformaciones y asignaciones de columnas.

    Parameters:
    json (dict): Los datos de mercado en formato JSON.
    cripto (str): El nombre de la criptomoneda.
    spark_session (pyspark.sql.SparkSession): La sesión de Spark.

    Returns:
    pyspark.sql.DataFrame: Un DataFrame que contiene los datos de mercado procesados.

    """
    df = spark_session.read.json(spark_session.sparkContext.parallelize([json]), multiLine=True)
    df = df.withColumn('timestamp', col('prices')[0])
    df = df.withColumn('prices', col('prices')[1])
    df = df.withColumn('market_caps', expr("market_caps[1] * 1000").cast('double'))
    df = df.withColumn('total_volumes', expr("total_volumes[1] * 1000").cast('double'))
    df = df.withColumn('cripto', col(cripto))
    
    return df


import pandas as pd
import numpy as np
    
def transformation_top(json, spark_session):
    """
    Transforma los datos de las 100 criptomonedas de mayor capitalización obtenidos de la API de CoinGecko
    en un DataFrame de Pandas y selecciona las columnas deseadas.

    Parameters:
    json (list): Los datos de las 100 criptomonedas en formato JSON.

    Returns:
    pandas.DataFrame: Un DataFrame que contiene las columnas seleccionadas.
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

def json_to_df_market_chart(json,cripto):
    """
    Convierte los datos de mercado en formato JSON en un DataFrame de Pandas
    y realiza algunas transformaciones y asignaciones de columnas.

    Parameters:
    json (dict): Los datos de mercado en formato JSON.
    cripto (str): El nombre de la criptomoneda.

    Returns:
    pandas.DataFrame: Un DataFrame que contiene los datos de mercado procesados.

    """
    df = pd.DataFrame(json)
    df['timestamp'] = df['prices'].str[0]
    df['prices'] = df['prices'].str[1]
    df['market_caps'] = df['market_caps'].str[1]
    df['market_caps'] = df['market_caps'].apply(lambda x: float('{:.3f}'.format(x)))
    df['total_volumes'] = df['total_volumes'].str[1]
    df['total_volumes'] = df['total_volumes'].apply(lambda x: float('{:.3f}'.format(x)))
    df['cripto'] = cripto
    
    return df

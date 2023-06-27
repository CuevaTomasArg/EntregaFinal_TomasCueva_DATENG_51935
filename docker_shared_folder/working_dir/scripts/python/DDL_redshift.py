from os import environ as env
import psycopg2
from psycopg2 import sql

conn_params = {
    'host': env['REDSHIFT_HOST'],
    'port': env['REDSHIFT_PORT'],
    'user': env['REDSHIFT_USER'],
    'password': env['REDSHIFT_PASSWORD'],
    'database': env['REDSHIFT_DB']
}

# Establecer la conexión y crear el cursor dentro del contexto 'with'
with psycopg2.connect(**conn_params) as conn:
    with conn.cursor() as cur:
        # Definir el comando SQL para crear la tabla criptos_market_cap
        create_table_markets_cap = sql.SQL('''
            CREATE TABLE IF NOT EXISTS cuevatomass02_coderhouse.criptos_market_cap (
                id VARCHAR(100) DISTKEY PRIMARY KEY,
                symbol CHAR(4),
                name VARCHAR(100),
                current_price FLOAT,
                market_cap FLOAT,
                market_cap_rank INT,
                total_volume FLOAT,
                high_24h FLOAT,
                low_24h FLOAT,
                price_change_24h FLOAT,
                price_change_percentage_24h FLOAT,
                market_cap_change_24h FLOAT,
                market_cap_change_percentage_24h FLOAT,
                circulating_supply FLOAT,
                ath FLOAT,
                ath_change_percentage FLOAT,
                ath_date TIMESTAMP,
                atl FLOAT,
                atl_change_percentage FLOAT,
                atl_date TIMESTAMP,
                last_updated TIMESTAMP
            ) SORTKEY(market_cap_rank, high_24h, low_24h, total_volume);
        ''')

        # Definir el comando SQL para crear la tabla market_chart_criptos
        create_table_markets_chart = sql.SQL('''
            CREATE TABLE IF NOT EXISTS cuevatomass02_coderhouse.market_chart_criptos (
                id VARCHAR(100) DISTKEY PRIMARY KEY,
                prices FLOAT,
                total_volumes FLOAT,
                market_caps FLOAT,
                date_unix BIGINT
            ) SORTKEY(date_unix);
        ''')

        # Ejecutar los comandos SQL para crear las tablas
        cur.execute(create_table_markets_cap)
        cur.execute(create_table_markets_chart)

        # Confirmar los cambios en la base de datos
        conn.commit()

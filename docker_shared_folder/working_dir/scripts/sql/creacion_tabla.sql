CREATE TABLE IF NOT EXISTS cuevatomass02_coderhouse.criptos_market_cap (
  id VARCHAR(100) DISTKEY PRIMARY KEY,
  symbol CHAR(4) ,
  name VARCHAR(100) ,
  current_price FLOAT ,
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

CREATE TABLE IF NOT EXISTS cuevatomass02_coderhouse.market_chart_criptos(
  id VARCHAR(100) DISTKEY PRIMARY KEY,
  prices FLOAT,
  total_volumes FLOAT,
  market_caps FLOAT,
  date_unix BIGINT, 
) SORTKEY(date_unix)


/*
notas al lector:
symbol es de tipo CHAR(4) ya que los tickets o simbolos de los activos
no suele superar los 4 caracteres.

Los demas tipos de datos son lo más parecido posible al dataframe final
df_coins que se encuentra dentro del notebook "api_criptos.ipynb"

*/
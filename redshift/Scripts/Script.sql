DROP TABLE IF EXISTS cuevatomass02_coderhouse.ethereum_historical_data ;
CREATE TABLE cuevatomass02_coderhouse.ethereum_historical_data (
  prices_timestamp        TIMESTAMP DISTKEY,
  prices_usd              FLOAT,
  market_caps_timestamp   TIMESTAMP,
  market_caps_usd         FLOAT,
  total_volumes_timestamp TIMESTAMP SORTKEY,
  total_volumes_usd       FLOAT
);

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
);

CREATE TABLE IF NOT EXISTS cuevatomass02_coderhouse.market_charts(
    id VARCHAR(100) DISTKEY PRIMARY KEY,
    prices FLOAT,
    total_volumes FLOAT,
    market_caps FLOAT,
    date_unix DOUBLE PRECISION
);
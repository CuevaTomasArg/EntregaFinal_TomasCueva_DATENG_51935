from utils.extract_CoinGecko import get_market_chart
from utils.transform_df import json_to_df_market_chart
from utils.load_redshift import load_to_redshift
from utils.pyspark import PySparkSession
class ETLMarketCharts(PySparkSession):
    """
    Proceso ETL del top 100 de criptomonedas con mayor capitalizacion de mercado.
    """

    def __init__(self):
        super().__init__()
        self.table = "market_charts"
        self.URL_BASE = "https://api.coingecko.com/api/v3/"
        self.id_list = ["bitcoin", "ethereum", "tether", "binancecoin", "ripple"]
        
    def extract(self):
        json = get_market_chart(self.URL_BASE, self.id_list)
        return json
    
    def transform(self, json):
        df = json_to_df_market_chart(json, self.id_list, self.spark)
        return df
     
    def load(self, df):
        load_to_redshift(df, self.table, self.REDSHIFT_URL, self.REDSHIFT_USER, self.REDSHIFT_PASSWORD)



if __name__ == "__main__":
    etl = ETLMarketCharts()
    json = etl.extract()
    
    if isinstance(json, str):
        print('Error:', json)
    
    else:
        df = etl.transform(json)
        etl.load(df)
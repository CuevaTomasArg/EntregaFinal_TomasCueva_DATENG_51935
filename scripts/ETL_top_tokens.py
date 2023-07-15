from os import environ as env
from pyspark.sql import SparkSession
from utils.extract_CoinGecko import get_criptos_top
from utils.transform_df import transformation_top
from utils.load_redshift import load_to_redshift

class ETLTopTokens:
    """
    Proceso ETL del top 100 de criptomonedas con mayor capitalizacion de mercado.
    """
    DRIVER_PATH = env['DRIVER_PATH']
    REDSHIFT_HOST = env['REDSHIFT_HOST']
    REDSHIFT_PORT = env['REDSHIFT_PORT']
    REDSHIFT_DB = env['REDSHIFT_DB']
    REDSHIFT_USER = env["REDSHIFT_USER"]
    REDSHIFT_PASSWORD = env["REDSHIFT_PASSWORD"]
    REDSHIFT_URL = f"jdbc:postgresql://{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}?user={REDSHIFT_USER}&password={REDSHIFT_PASSWORD}"
    URL_BASE = "https://api.coingecko.com/api/v3/"

    def __init__(self):
        """
        Inicializa la clase Load.

        Configura la sesión de Spark.

        """
        env['PYSPARK_SUBMIT_ARGS'] = f'--driver-class-path {self.DRIVER_PATH} --jars {self.DRIVER_PATH} pyspark-shell'
        env['SPARK_CLASSPATH'] = self.DRIVER_PATH

        self.table = "criptos_market_cap"
        self.spark = SparkSession.builder \
                .master("local[1]") \
                .appName("Spark y Redshift") \
                .config("spark.jars", self.DRIVER_PATH) \
                .config("spark.executor.extraClassPath", self.DRIVER_PATH) \
                .getOrCreate()
    
    def extract(self):
        json = get_criptos_top(self.URL_BASE)
        
        return json
    
    def transform(self, json):
         df = transformation_top(json, self.spark)
         
         return df
     
    def load(self, df):
         load_to_redshift(df, self.table, self.REDSHIFT_URL, self.REDSHIFT_USER, self.REDSHIFT_PASSWORD)


if __name__ == "__main__":
    etl = ETLTopTokens()
    json = etl.extract()
    
    if isinstance(json, str):
        # Si el response es un string significa que hubo error en la petición a API
        print('Error:', json)
    else:
        df = etl.transform(json)
        etl.load(df)
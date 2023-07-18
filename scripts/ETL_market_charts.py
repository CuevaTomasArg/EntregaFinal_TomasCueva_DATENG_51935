from utils.extract_CoinGecko import get_market_chart
from utils.transform_df import json_to_df_market_chart
from utils.load_redshift import load_to_redshift
from utils.pyspark import PySparkSession

class ETLMarketCharts(PySparkSession):
    """
    Clase que implementa un proceso ETL para el top 100 de criptomonedas con mayor capitalización de mercado.

    Atributos:
        table (str): El nombre de la tabla en Redshift donde se cargarán los datos.
        URL_BASE (str): La URL base de la API de CoinGecko.
        id_list (list): Una lista de identificadores de criptomonedas.

    Métodos:
        __init__(): Constructor de la clase. Inicializa los atributos y llama al constructor de la superclase.
        extract(): Realiza la extracción de los datos de mercado de las criptomonedas utilizando la API de CoinGecko.
        transform(data): Realiza la transformación de los datos obtenidos en formato JSON y devuelve un DataFrame de PySpark.
        load(df): Carga el DataFrame de PySpark en la tabla de Redshift.

    """

    def __init__(self):
        """
        Constructor de la clase ETLMarketCharts.

        Inicializa los atributos table, URL_BASE y id_list, y llama al constructor de la superclase PySparkSession.
        """
        super().__init__()
        self.table = "market_charts"
        self.URL_BASE = "https://api.coingecko.com/api/v3/"
        self.id_list = ["bitcoin", "ethereum", "tether", "binancecoin", "ripple"]

    def extract(self):
        """
        Realiza la extracción de los datos de mercado de las criptomonedas utilizando la API de CoinGecko.

        Returns:
            list: Una lista que contiene los datos de mercado de las criptomonedas en formato JSON.

        """
        data = get_market_chart(self.URL_BASE, self.id_list)
        return data

    def transform(self, data):
        """
        Realiza la transformación de los datos obtenidos en formato JSON y devuelve un DataFrame de PySpark.

        Parameters:
            data (list): Una lista que contiene los datos de mercado de las criptomonedas en formato JSON.

        Returns:
            pyspark.sql.DataFrame: Un DataFrame que contiene los datos de mercado transformados.

        """
        df = json_to_df_market_chart(data, self.id_list, self.spark)
        return df

    def load(self, df):
        """
        Carga el DataFrame de PySpark en la tabla de Redshift.

        Parameters:
            df (pyspark.sql.DataFrame): El DataFrame de PySpark a cargar.

        """
        load_to_redshift(df, self.table, self.REDSHIFT_URL, self.REDSHIFT_USER, self.REDSHIFT_PASSWORD)


if __name__ == "__main__":
    etl = ETLMarketCharts()
    data = etl.extract()
    df = etl.transform(data)
    etl.load(df)

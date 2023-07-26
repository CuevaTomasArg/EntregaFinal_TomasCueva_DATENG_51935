from .utils.extract_CoinGecko import get_criptos_top
from .utils.transform_df import transformation_top
from .utils.load_redshift import load_to_redshift
from .utils.connection_spark import PySparkSession

class ETLTopTokens(PySparkSession):
    """
    Clase que implementa un proceso ETL para el top 100 de criptomonedas con mayor capitalización de mercado.
    
    Atributos:
        table (str): El nombre de la tabla en Redshift donde se cargarán los datos.
        URL_BASE (str): La URL base de la API de CoinGecko.

    Métodos:
        __init__(): Constructor de la clase. Inicializa los atributos y llama al constructor de la superclase.
        extract(): Realiza la extracción de los datos de las criptomonedas utilizando la API de CoinGecko.
        transform(json): Realiza la transformación de los datos obtenidos en formato JSON y devuelve un DataFrame de PySpark.
        load(df): Carga el DataFrame de PySpark en la tabla de Redshift.

    """

    def __init__(self):
        """
        Constructor de la clase ETLTopTokens.

        Inicializa los atributos table y URL_BASE, y llama al constructor de la superclase PySparkSession.
        """
        super().__init__()
        self.table = "criptos_market_cap"
        self.URL_BASE = "https://api.coingecko.com/api/v3/"

    def extract(self):
        """
        Realiza la extracción de los datos de las criptomonedas utilizando la API de CoinGecko.

        Returns:
            list: Una lista que contiene los datos de las criptomonedas en formato JSON.

        """
        json = get_criptos_top(self.URL_BASE)
        return json

    def transform(self, json):
        """
        Realiza la transformación de los datos obtenidos en formato JSON y devuelve un DataFrame de PySpark.

        Parameters:
            json (list): Una lista que contiene los datos de las criptomonedas en formato JSON.

        Returns:
            pyspark.sql.DataFrame: Un DataFrame que contiene los datos de las criptomonedas transformados.

        """
        df = transformation_top(json, self.spark)
        return df

    def load(self, df):
        """
        Carga el DataFrame de PySpark en la tabla de Redshift.

        Parameters:
            df (pyspark.sql.DataFrame): El DataFrame de PySpark a cargar.

        """
        load_to_redshift(df, self.table, self.REDSHIFT_URL, self.REDSHIFT_USER, self.REDSHIFT_PASSWORD)


if __name__ == "__main__":
    etl = ETLTopTokens()
    json = etl.extract()
    
    if isinstance(json, str):
        print('Error:', json)
    else:
        df = etl.transform(json)
        etl.load(df)

from utils.load_redshift import load_to_redshift
from utils.connection_spark import PySparkSession
import requests

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
        Obtiene los datos de las criptomonedas con mayor capitalización de mercado.

        Returns:
            dict: Los datos de las criptomonedas con mayor capitalización en formato JSON.

        Raises:
            requests.exceptions.RequestException: Si ocurre un error al realizar la solicitud.

        """
        endpoint = "/coins/markets"
        url = self.URL_BASE + endpoint
        parameters = {
            "vs_currency": "usd"
        }

        try:
            response = requests.get(url, params=parameters)
            response.raise_for_status()
            data = response.json()
            print(">>> Solicitud exitosa")
            return data
        except requests.exceptions.RequestException as e:
            error = f"Error al realizar la solicitud: {e}"
            return error

    def transform(self, json):
        """
        Transforma los datos de las 100 criptomonedas de mayor capitalización obtenidos de la API de CoinGecko
        en un DataFrame de PySpark y selecciona las columnas deseadas.

        Parameters:
            json (dict): Los datos de las 100 criptomonedas en formato JSON.

        Returns:
            pyspark.sql.DataFrame: Un DataFrame que contiene las columnas seleccionadas.
        """
        df = self.spark.read.json(
            self.spark.sparkContext.parallelize(json),
            multiLine=True
        )

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
        
        df.printSchema()
        
        df.show()
        
        df = df.select(selected_columns)
        
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

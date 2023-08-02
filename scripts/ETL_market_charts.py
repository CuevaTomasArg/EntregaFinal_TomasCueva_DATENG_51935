import requests
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql.functions import lit, expr
from functools import reduce
import concurrent.futures
from utils.load_redshift import load_to_redshift
from utils.connection_spark import PySparkSession
from datetime import datetime

class ETLMarketCharts(PySparkSession):
    """
    Clase que implementa un proceso ETL para obtener el gráfico de mercado de las criptomonedas con mayor capitalización.

    Atributos:
        table (str): El nombre de la tabla en Redshift donde se cargarán los datos.
        URL_BASE (str): La URL base de la API de CoinGecko.
        id_list (list): Una lista de identificadores de criptomonedas.

    Métodos:
        __init__(): Constructor de la clase. Inicializa los atributos y llama al constructor de la superclase.
        fetch_market_chart(): Realiza una solicitud a la API para obtener el gráfico de mercado de una criptomoneda específica.
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

    def fetch_market_chart(self, id, parameters):
        """
        Realiza una solicitud a la API para obtener el gráfico de mercado de una criptomoneda específica.

        Parameters:
            id (str): El identificador de la criptomoneda.
            parameters (dict): Un diccionario que contiene los parámetros de la solicitud, como moneda de cambio, días, intervalo, etc.

        Returns:
            tuple: Una tupla que contiene el identificador de la criptomoneda y los datos del gráfico de mercado en formato JSON.

        Raises:
            requests.exceptions.RequestException: Si ocurre un error al realizar la solicitud.
        """
        endpoint = f"/coins/{id}/market_chart"
        url = self.URL_BASE + endpoint

        try:
            response = requests.get(url, params=parameters)
            response.raise_for_status()
            data = response.json()
            print(f">>> Solicitud de id:{id} exitosa")
            return (id, data)

        except requests.exceptions.RequestException as e:
            print(f"Error al realizar la solicitud: {e}")
            return None

    def extract(self):
        """
        Obtiene los datos del gráfico de mercado para una lista de criptomonedas.

        Returns:
            list: Una lista que contiene las tuplas de (identificador de criptomoneda, datos de gráfico de mercado en formato JSON).
        """
        market_chart_list = []
        parameters = {
            "vs_currency": "usd",
            "days": "max",
            "interval": "daily",
            "precision": "3"
        }

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_id = {executor.submit(self.fetch_market_chart, id, parameters): id for id in self.id_list}

            for future in concurrent.futures.as_completed(future_to_id):
                data = future.result()
                market_chart_list.append(data)

        return market_chart_list

    def transform(self, data):
        """
        Convierte los datos de mercado en formato JSON en un DataFrame de PySpark
        y realiza algunas transformaciones y asignaciones de columnas.

        Parameters:
            data (list): Una lista de datos de mercado en formato JSON.

        Returns:
            pyspark.sql.DataFrame: Un DataFrame que contiene los datos de mercado procesados.
        """
        def clean_number(number):
            if number is not None:
                decimal_index = number.find('.')

                if decimal_index != -1:
                    integer_part = number[:decimal_index]
                    decimal_part = number[decimal_index + 1:decimal_index + 4].ljust(3, '0')
                    cleaned_number = f"{integer_part}.{decimal_part}"
                    return cleaned_number
                else:
                    return number
            else:
                return None

        # Registro de la función clean_number como UDF (User-Defined Function) en Spark
        self.spark.udf.register("clean_number", clean_number)

        def process_data(tuple):
            json = tuple[1]
            df_dict = {}
            for key, value in json.items():
                schema = StructType([
                    StructField("date_unix", LongType(), True),
                    StructField(f"{key}", StringType(), True),
                ])
                df = self.spark.createDataFrame(value, schema)
                df = df.withColumn(f"{key}", expr(f"clean_number({key})").alias(f"{key}"))
                df_dict[key] = df

            df_final = reduce(lambda df1, df2: df1.join(df2, "date_unix", "outer"), df_dict.values())
            df_final = df_final.withColumn("id", lit(tuple[0]))

            current_date = datetime.now().date()
            df_final = df_final.withColumn("date_load", lit(current_date))

            df_final = df_final.select("id", "prices", "total_volumes", "market_caps", "date_unix", "date_load")
            return df_final

        data_cleaned = [element for element in data if element is not None]
        dfs = list(map(process_data, data_cleaned))
        df = reduce(lambda df1, df2: df1.union(df2), dfs)

        df.show()
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

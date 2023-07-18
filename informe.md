**Módulo: etl_top_tokens.py**

Este módulo contiene una clase llamada `ETLTopTokens` que se encarga de realizar un proceso de Extracción, Transformación y Carga (ETL) de los datos del top 100 de criptomonedas con mayor capitalización de mercado. El proceso utiliza la API de CoinGecko para obtener los datos de las criptomonedas, realiza algunas transformaciones sobre los datos y finalmente carga el resultado en una tabla de Amazon Redshift.

**Documentación de la Clase ETLTopTokens:**

```python
from utils.extract_CoinGecko import get_criptos_top
from utils.transform_df import transformation_top
from utils.load_redshift import load_to_redshift
from utils.pyspark import PySparkSession

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
```

**Descripción:**

La clase `ETLTopTokens` es una subclase de `PySparkSession`, lo que permite utilizar la funcionalidad de Spark para el procesamiento de datos. El proceso ETL se divide en tres etapas: extracción, transformación y carga.

- **Extracción (`extract()`):** Utiliza la función `get_criptos_top()` del módulo `utils.extract_CoinGecko` para obtener los datos de las criptomonedas desde la API de CoinGecko. Los datos se almacenan en formato JSON y se devuelven como una lista.

- **Transformación (`transform(json)`):** Utiliza la función `transformation_top()` del módulo `utils.transform_df` para transformar los datos JSON en un DataFrame de PySpark. Esta transformación incluye la selección y conversión de columnas, limpieza de datos, y otras operaciones necesarias para preparar los datos para su carga.

- **Carga (`load(df)`):** Utiliza la función `load_to_redshift()` del módulo `utils.load_redshift` para cargar el DataFrame de PySpark en la tabla `criptos_market_cap` de Amazon Redshift. Esta función utiliza la biblioteca JDBC para realizar la carga.

**Nota:** Se asume que las funciones `get_criptos_top()`, `transformation_top()`, y `load_to_redshift()` están correctamente implementadas en los módulos `utils.extract_CoinGecko`, `utils.transform_df`, y `utils.load_redshift`, respectivamente. También se asume que la clase `PySparkSession` proporciona una sesión válida de Spark para el procesamiento de datos en Spark.







**Módulo: etl_market_charts.py**

Este módulo contiene una clase llamada `ETLMarketCharts` que se encarga de realizar un proceso de Extracción, Transformación y Carga (ETL) de los datos del top 100 de criptomonedas con mayor capitalización de mercado. El proceso utiliza la API de CoinGecko para obtener los datos de mercado de las criptomonedas, realiza algunas transformaciones sobre los datos y finalmente carga el resultado en una tabla de Amazon Redshift.

**Documentación de la Clase ETLMarketCharts:**

```python
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
```

**Descripción:**

La clase `ETLMarketCharts` es una subclase de `PySparkSession`, lo que permite utilizar la funcionalidad de Spark para el procesamiento de datos. El proceso ETL se divide en tres etapas: extracción, transformación y carga.

- **Extracción (`extract()`):** Utiliza la función `get_market_chart()` del módulo `utils.extract_CoinGecko` para obtener los datos de mercado de las criptomonedas desde la API de CoinGecko. Los datos se almacenan en formato JSON y se devuelven como una lista.

- **Transformación (`transform(data)`):** Utiliza la función `json_to_df_market_chart()` del módulo `utils.transform_df` para transformar los datos JSON en un DataFrame de PySpark. Esta transformación incluye la selección y conversión de columnas, limpieza de datos y otras operaciones necesarias para preparar los datos para su carga.

- **Carga (`load(df)`):** Utiliza la función `load_to_redshift()` del módulo `utils.load_redshift` para cargar el DataFrame de PySpark en la tabla `market_charts` de Amazon Redshift. Esta función utiliza la biblioteca JDBC para realizar la carga.

**Nota:** Se asume que las funciones `get_market_chart()` y `json_to_df_market_chart()` están correctamente implementadas en los módulos `utils.extract_CoinGecko` y `utils.transform_df`, respectivamente. También se asume que la clase `PySparkSession` proporciona una sesión válida de Spark para el procesamiento de datos en Spark.


**Módulo: utils.extract_CoinGecko**

Este módulo contiene funciones para extraer datos de la API de CoinGecko relacionados con el gráfico de mercado de criptomonedas y las criptomonedas con mayor capitalización.

**Función: fetch_market_chart**

```python
def fetch_market_chart(base_url, id, parameters):
    """
    Realiza una solicitud a la API para obtener el gráfico de mercado de una criptomoneda específica.

    Parameters:
        base_url (str): La URL base de la API de CoinGecko.
        id (str): El identificador de la criptomoneda.
        parameters (dict): Un diccionario que contiene los parámetros de la solicitud, como moneda de cambio, días, intervalo, etc.

    Returns:
        tuple: Una tupla que contiene el identificador de la criptomoneda y los datos del gráfico de mercado en formato JSON.

    Raises:
        requests.exceptions.RequestException: Si ocurre un error al realizar la solicitud.

    """
    endpoint = f"/coins/{id}/market_chart"
    url = base_url + endpoint

    try:
        response = requests.get(url, params=parameters)
        response.raise_for_status()  # Verificar si la solicitud fue exitosa
        data = response.json()
        print(f">>> Solicitud de id:{id} exitosa")
        return (id, data)

    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la solicitud: {e}")
        return None
```

**Función: get_market_chart**

```python
def get_market_chart(base_url, id_list):
    """
    Obtiene los datos del gráfico de mercado para una lista de criptomonedas.

    Parameters:
        base_url (str): La URL base de la API de CoinGecko.
        id_list (list): Una lista de identificadores de criptomonedas.

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
        future_to_id = {executor.submit(fetch_market_chart, base_url, id, parameters): id for id in id_list}
        
        for future in concurrent.futures.as_completed(future_to_id):
            data = future.result()
            market_chart_list.append(data)
            
    return market_chart_list
```

**Función: get_criptos_top**

```python
def get_criptos_top(base_url):
    """
    Obtiene los datos de las criptomonedas con mayor capitalización de mercado.

    Parameters:
        base_url (str): La URL base de la API de CoinGecko.

    Returns:
        dict: Los datos de las criptomonedas con mayor capitalización en formato JSON.

    Raises:
        requests.exceptions.RequestException: Si ocurre un error al realizar la solicitud.

    """
    endpoint = "/coins/markets"
    url = base_url + endpoint
    parameters = {
        "vs_currency": "usd"
    }

    try:
        response = requests.get(url, params=parameters)
        response.raise_for_status()  # Verificar si la solicitud fue exitosa
        data = response.json()
        print(">>> Solicitud exitosa")
        return data
    except requests.exceptions.RequestException as e:
        error = f"Error al realizar la solicitud: {e}"
        return error
```

**Descripción:**

El módulo `utils.extract_CoinGecko` contiene tres funciones para obtener datos de la API de CoinGecko:

1. La función `fetch_market_chart()` realiza una solicitud a la API para obtener el gráfico de mercado de una criptomoneda específica. Toma la URL base de la API, el identificador de la criptomoneda y un diccionario de parámetros como entrada. Devuelve una tupla que contiene el identificador de la criptomoneda y los datos del gráfico de mercado en formato JSON.

2. La función `get_market_chart()` obtiene los datos del gráfico de mercado para una lista de criptomonedas. Toma la URL base de la API y una lista de identificadores de criptomonedas como entrada. Utiliza la función `fetch_market_chart()` con el uso de hilos para realizar múltiples solicitudes de forma simultánea y optimizar el tiempo de respuesta. Devuelve una lista de tuplas que contiene el identificador de la criptomoneda y los datos del gráfico de mercado en formato JSON.

3. La función `get_criptos_top()` obtiene los datos de las criptomonedas con mayor capitalización de mercado. Toma la URL base de la API como entrada y realiza una solicitud a la API para obtener los datos de las criptomonedas con mayor capitalización. Devuelve los datos en formato JSON.

Cada función está debidamente documentada con docstrings que describen sus parámetros, valores de retorno y posibles excepciones que puedan ser generadas durante su ejecución. El uso de docstrings ayuda a entender el propósito y la funcionalidad de cada función, facilitando su mantenimiento y uso en futuros desarrollos. Además, se incluyen mensajes de impresión para proporcionar información sobre el estado del proceso y mensajes de error en caso de que se produzcan excepciones.

**Módulo: transform_data.py**

Este módulo contiene dos funciones: `json_to_df_market_chart(data, spark_session)` y `transformation_top(json, spark_session)`, que se encargan de transformar los datos de mercado de criptomonedas y datos de las 100 criptomonedas de mayor capitalización respectivamente. El procesamiento se realiza utilizando la librería PySpark.

**Documentación de la función json_to_df_market_chart:**

```python
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, format_number

def json_to_df_market_chart(data, spark_session):
    """
    Convierte los datos de mercado en formato JSON en un DataFrame de PySpark
    y realiza algunas transformaciones y asignaciones de columnas.

    Parameters:
        data (list): Una lista de datos de mercado en formato JSON.
        spark_session (pyspark.sql.SparkSession): La sesión de Spark.

    Returns:
        pyspark.sql.DataFrame: Un DataFrame que contiene los datos de mercado procesados.
    """

    def process_json(data, schema):
        prices = [float(prices_list[1]) if prices_list[1] is not None else 0.0 for prices_list in data[1]["prices"]]
        market_caps = [float(market_caps_list[1]) if market_caps_list[1] is not None else 0.0 for market_caps_list in data[1]["market_caps"]]
        total_volumes = [float(total_volumes_list[1]) if total_volumes_list[1] is not None else 0.0 for total_volumes_list in data[1]["total_volumes"]]
        timestamps = [timestamps_list[0] for timestamps_list in data[1]["prices"]]
        join = zip(prices, market_caps, total_volumes, timestamps)
        join_list = list(join)
        join_list = [(item + (data[0],)) for item in join_list]
        return spark_session.createDataFrame(join_list, schema)

    data_cleaned = [element for element in data if element is not None]
    
    schema = StructType([
        StructField("prices", FloatType(), True),
        StructField("market_caps", FloatType(), True),
        StructField("total_volumes", FloatType(), True),
        StructField("date_unix", LongType(), True),
        StructField("id", StringType(), True) 
    ])
    
    dfs = map(process_json, data_cleaned, schema)
    final_df = reduce(DataFrame.union, dfs)
    final_df = final_df.withColumn("market_caps", format_number(col("market_caps"), 2))
    final_df = final_df.select("id", "prices", "total_volumes", "market_caps", "date_unix")
    
    return final_df
```

**Descripción:**

La función `json_to_df_market_chart` toma como entrada una lista de datos de mercado en formato JSON y una sesión de Spark (`spark_session`). Realiza transformaciones y asignaciones de columnas sobre los datos de mercado y devuelve un DataFrame de PySpark con los datos procesados. Los datos de mercado incluyen información sobre los precios, capitalización de mercado y volúmenes de criptomonedas.

**Parámetros:**

- `data (list)`: Una lista que contiene los datos de mercado de las criptomonedas en formato JSON. Cada elemento de la lista representa los datos de una criptomoneda.

- `spark_session (pyspark.sql.SparkSession)`: La sesión de Spark utilizada para crear el DataFrame y realizar las transformaciones.

**Retorno:**

- `pyspark.sql.DataFrame`: Un DataFrame de PySpark que contiene los datos de mercado procesados con las siguientes columnas: "id" (identificador de la criptomoneda), "prices" (precios de la criptomoneda), "market_caps" (capitalización de mercado de la criptomoneda), "total_volumes" (volumen total de la criptomoneda) y "date_unix" (timestamp de la fecha del dato).

**Documentación de la función transformation_top:**

```python
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from pyspark.sql import DataFrame

def transformation_top(json, spark_session):
    """
    Transforma los datos de las 100 criptomonedas de mayor capitalización obtenidos de la API de CoinGecko
    en un DataFrame de Pandas y selecciona las columnas deseadas.

    Parameters:
        json (dict): Los datos de las 100 criptomonedas en formato JSON.
        spark_session (pyspark.sql.SparkSession): La sesión de Spark.

    Returns:
        pyspark.sql.DataFrame: Un DataFrame que contiene las columnas seleccionadas.
    """
    df = spark_session.read.json(
        spark_session.sparkContext.parallelize(json),
        multiLine = True
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
    df = df.select(selected_columns)
    
    return df
```

**Descripción:**

La función `transformation_top` toma como entrada los datos de las 100 criptomonedas de mayor capitalización en formato JSON y una sesión de Spark (`spark_session`). Transforma los datos para seleccionar solo las columnas deseadas y devuelve un DataFrame de PySpark con las columnas seleccionadas.

**Parámetros:**

- `json (dict)

`: Los datos de las 100 criptomonedas de mayor capitalización en formato JSON.

- `spark_session (pyspark.sql.SparkSession)`: La sesión de Spark utilizada para crear el DataFrame y realizar la selección de columnas.

**Retorno:**

- `pyspark.sql.DataFrame`: Un DataFrame de PySpark que contiene las columnas seleccionadas de los datos de las 100 criptomonedas de mayor capitalización. Las columnas seleccionadas son: 'id', 'symbol', 'name', 'current_price', 'market_cap', 'market_cap_rank', 'total_volume', 'high_24h', 'low_24h', 'price_change_24h', 'price_change_percentage_24h', 'market_cap_change_24h', 'market_cap_change_percentage_24h', 'circulating_supply', 'ath', 'ath_change_percentage', 'ath_date', 'atl', 'atl_change_percentage', 'atl_date' y 'last_updated'.

**Módulo: pyspark_session.py**

Este módulo contiene una clase llamada `PySparkSession`, que se encarga de configurar una sesión de Spark para el procesamiento de datos y establecer la conexión con una base de datos de Amazon Redshift para realizar un proceso ETL del top 100 de criptomonedas con mayor capitalización de mercado.

**Documentación de la Clase PySparkSession:**

```python
from os import environ as env
from pyspark.sql import SparkSession

class PySparkSession:
    """
    Clase para configurar una sesión de Spark y establecer la conexión con Amazon Redshift.

    Atributos:
        DRIVER_PATH (str): La ruta del controlador JDBC para la conexión con Redshift.
        REDSHIFT_HOST (str): El host de la base de datos Redshift.
        REDSHIFT_PORT (str): El puerto de la base de datos Redshift.
        REDSHIFT_DB (str): El nombre de la base de datos Redshift.
        REDSHIFT_USER (str): El nombre de usuario para la conexión con Redshift.
        REDSHIFT_PASSWORD (str): La contraseña para la conexión con Redshift.
        REDSHIFT_URL (str): La URL de conexión para la base de datos Redshift.

    Métodos:
        __init__(): Constructor de la clase. Configura la sesión de Spark y la conexión con Redshift.

    """

    DRIVER_PATH = env['DRIVER_PATH']
    REDSHIFT_HOST = env['REDSHIFT_HOST']
    REDSHIFT_PORT = env['REDSHIFT_PORT']
    REDSHIFT_DB = env['REDSHIFT_DB']
    REDSHIFT_USER = env["REDSHIFT_USER"]
    REDSHIFT_PASSWORD = env["REDSHIFT_PASSWORD"]
    REDSHIFT_URL = f"jdbc:postgresql://{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}?user={REDSHIFT_USER}&password={REDSHIFT_PASSWORD}"

    def __init__(self):
        """
        Constructor de la clase PySparkSession.

        Configura la sesión de Spark y establece la conexión con la base de datos de Amazon Redshift.
        """
        env['PYSPARK_SUBMIT_ARGS'] = f'--driver-class-path {self.DRIVER_PATH} --jars {self.DRIVER_PATH} pyspark-shell'
        env['SPARK_CLASSPATH'] = self.DRIVER_PATH

        self.spark = SparkSession.builder \
                .master("local[1]") \
                .appName("Spark y Redshift") \
                .config("spark.jars", self.DRIVER_PATH) \
                .config("spark.executor.extraClassPath", self.DRIVER_PATH) \
                .getOrCreate()

        print(">>> Session de Spark creada.")
```

**Descripción:**

La clase `PySparkSession` se encarga de configurar una sesión de Spark para el procesamiento de datos y establecer la conexión con una base de datos de Amazon Redshift para realizar un proceso ETL del top 100 de criptomonedas con mayor capitalización de mercado.

- **Atributos:**

    - `DRIVER_PATH`: Ruta del controlador JDBC para la conexión con Redshift.
    - `REDSHIFT_HOST`: Host de la base de datos Redshift.
    - `REDSHIFT_PORT`: Puerto de la base de datos Redshift.
    - `REDSHIFT_DB`: Nombre de la base de datos Redshift.
    - `REDSHIFT_USER`: Nombre de usuario para la conexión con Redshift.
    - `REDSHIFT_PASSWORD`: Contraseña para la conexión con Redshift.
    - `REDSHIFT_URL`: URL de conexión para la base de datos Redshift, construida a partir de los atributos anteriores.

- **Métodos:**

    - `__init__()`: Constructor de la clase. Configura la sesión de Spark y la conexión con Redshift. Primero, se establecen las variables de entorno `PYSPARK_SUBMIT_ARGS` y `SPARK_CLASSPATH` para especificar el controlador JDBC necesario para la conexión con Redshift. Luego, se crea la sesión de Spark utilizando el builder de SparkSession con la configuración necesaria, como el número de núcleos locales y el nombre de la aplicación. Finalmente, se imprime un mensaje para indicar que la sesión de Spark ha sido creada.

**Nota:** La clase `PySparkSession` es una clase base que se utiliza en otros módulos para crear la sesión de Spark antes de realizar el proceso ETL. No se incluyen otros métodos o funcionalidades adicionales en esta clase, ya que su propósito principal es la configuración de la sesión de Spark y la conexión con Redshift. Los métodos para el proceso ETL específico se implementan en otras clases que heredan de `PySparkSession`.
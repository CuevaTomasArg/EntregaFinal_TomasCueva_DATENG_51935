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

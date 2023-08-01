"""
Este Script se realiza una consulta a la base de datos de Redshift para analizar el precio de bitcoin
"""
from utils.connection_spark import PySparkSession

class BitcoinTrigger(PySparkSession):
    def __init__(self):
        super().__init__()
        self.table = "market_charts"
        
    def select_query(self):
        QUERY_SELECT_BITCOIN = f"SELECT * FROM {self.table}"

        df = self.spark.spark.read \
            .format("jdbc") \
            .option("url", self.spark.REDSHIFT_URL) \
            .option("dbtable", f"({QUERY_SELECT_BITCOIN}) AS tmp") \
            .load()

        # Mostrar el contenido del DataFrame (opcional)
        df.show()
        
        return df

if __name__ == "__main__":
    trigger = BitcoinTrigger()
    df = trigger.select_query()
def load_to_redshift(self, df, table):
    """
    Carga un DataFrame de pandas en Redshift.

    Parameters:
    df (pandas.DataFrame): El DataFrame de pandas a cargar.
    table (str): El nombre de la tabla en Redshift donde se cargará el DataFrame.

    """

    print("Convertir el DataFrame de pandas a un PySpark DataFrame") 
    spark_df = self.spark.createDataFrame(df)
    print(spark_df)
    
    print("Cargar el PySpark DataFrame en Redshift") 
    try:
        spark_df.write \
            .format("jdbc") \
            .option("url", self.REDSHIFT_URL) \
            .option("dbtable", table) \
            .option("user", self.REDSHIFT_USER) \
            .option("password", self.REDSHIFT_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        print("Dataframe subido")
    except Exception as e:
        print("Se produjo excepción:", e)
        

def execute(self, df, table):
    """
    Ejecuta el proceso de ETL para cargar un DataFrame en Redshift.

    Parameters:
    df (pandas.DataFrame): El DataFrame de pandas a cargar.
    table (str): El nombre de la tabla en Redshift donde se cargará el DataFrame.

    """

    print("Ejecutando ETL")
    self.load_to_redshift(df, table)

def load_to_redshift(df, table, redshift_url, redshift_user, redshift_password):
    """
    Carga un DataFrame de pandas en Redshift.

    Parameters:
    df (pandas.DataFrame): El DataFrame de pandas a cargar.
    table (str): El nombre de la tabla en Redshift donde se cargará el DataFrame.

    """

    print(">>> Ejecutando carga")  
    try:
        df.write \
            .format("jdbc") \
            .option("url", redshift_url) \
            .option("dbtable", table) \
            .option("user", redshift_user) \
            .option("password", redshift_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        print(">>> Dataframe subido con exito")
    except Exception as e:
        print(">>>  Se produjo excepción:", e)
        
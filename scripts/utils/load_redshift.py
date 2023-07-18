def load_to_redshift(df, table, redshift_url, redshift_user, redshift_password):
    """
    Carga un DataFrame de pandas en una tabla de Amazon Redshift.

    Esta función utiliza la biblioteca JDBC (Java Database Connectivity) para cargar el DataFrame de pandas en una tabla
    de Amazon Redshift. La tabla debe existir previamente en Redshift y debe tener una estructura que coincida con la
    estructura del DataFrame.

    Parameters:
        df (pandas.DataFrame): El DataFrame de pandas a cargar en Redshift.
        table (str): El nombre de la tabla en Redshift donde se cargará el DataFrame.
        redshift_url (str): La URL de conexión a la base de datos de Amazon Redshift.
        redshift_user (str): El nombre de usuario para la conexión a Amazon Redshift.
        redshift_password (str): La contraseña para la conexión a Amazon Redshift.

    Raises:
        Exception: Si se produce un error durante la carga del DataFrame en Redshift, se mostrará un mensaje de error.

    Example:
        # Cargar el DataFrame "df" en la tabla "my_table" de Amazon Redshift
        load_to_redshift(df, "my_table", "jdbc:redshift://my-redshift-cluster.amazonaws.com:5439/my_database",
                         "my_redshift_user", "my_redshift_password")

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
        
        print(">>> Dataframe subido con éxito")
    except Exception as e:
        print(">>>  Se produjo una excepción:", e)

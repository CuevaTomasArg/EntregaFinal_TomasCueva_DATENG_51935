from os import environ as env
from sqlalchemy import create_engine
import pandas as pd


class Load():
    def __init__(self):
        host = env['REDSHIFT_HOST']
        port = env['REDSHIFT_PORT']
        user = env['REDSHIFT_USER']
        password = env['REDSHIFT_PASSWORD']
        database = env['REDSHIFT_DB']
        connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
        self.engine = create_engine(connection_string)

    def load_to_table(self, df, table):
        df.to_sql(table, self.engine, if_exists='replace', index=False)
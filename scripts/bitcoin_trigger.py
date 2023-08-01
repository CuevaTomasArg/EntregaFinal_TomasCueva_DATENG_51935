"""
Este Script se realiza una consulta a la base de datos de Redshift para analizar el precio de bitcoin
"""
from utils.connection_spark import PySparkSession
from os import environ as env
import smtplib
from datetime import datetime, timedelta

class BitcoinTrigger(PySparkSession):
    def __init__(self):
        super().__init__(env['DRIVER_PATH'])
        self.table = "market_charts"
        
    def select_query(self):
        QUERY_SELECT_BITCOIN = f"SELECT * FROM {self.table}"

        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.REDSHIFT_URL) \
            .option("dbtable", f"({QUERY_SELECT_BITCOIN}) AS tmp") \
            .load()

        # Mostrar el contenido del DataFrame (opcional)
        df.show()
        
        return df
    def send_alert(self, subject, body_text):
        try:
            smtp_conn = smtplib.SMTP('smtp.gmail.com', 587)
            smtp_conn.starttls()
            smtp_conn.login(env['FROM_EMAIL'], env['PASSWORD_EMAIL'])
            message = f'Subject: {subject}\n\n{body_text}'
            smtp_conn.sendmail(env['FROM_EMAIL'], env['TO_EMAIL'], message)
            print('Alerta enviada exitosamente.')
        except Exception as exception:
            print('Error al enviar la alerta.')
            print(exception)


def bitcoin_trend(df, func_send_email):
    # Análisis de tendencia en el último período (por ejemplo, últimos 7 días)
    current_date = datetime.today().date()
    last_week = current_date - timedelta(days=7)
    df_trend = df.filter((df.date_load >= last_week) & (df.date_load <= current_date))
    price_increase = df_trend.agg((df_trend.prices - df_trend.prices[0]) / df_trend.prices[0] * 100).collect()[0][0]

    if price_increase >= 10:  # Si la tendencia es de aumento del 10% o más
        subject = 'Alerta de Tendencia de Bitcoin'
        body_text = f'El precio de Bitcoin ha aumentado un {price_increase:.2f}% en los últimos 7 días. Considera la posibilidad de comprar Bitcoin.'
        func_send_email(subject, body_text)
    elif price_increase <= -10:  # Si la tendencia es de disminución del 10% o más
        subject = 'Alerta de Tendencia de Bitcoin'
        body_text = f'El precio de Bitcoin ha disminuido un {abs(price_increase):.2f}% en los últimos 7 días. Considera la posibilidad de vender Bitcoin.'
        func_send_email(subject, body_text)


if __name__ == "__main__":
    trigger = BitcoinTrigger()
    df = trigger.select_query()
    send_email = trigger.send_alert
    bitcoin_trend(df, send_email)
     
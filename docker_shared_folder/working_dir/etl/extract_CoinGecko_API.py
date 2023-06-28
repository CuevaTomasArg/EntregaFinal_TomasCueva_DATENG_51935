import pandas as pd
import requests


class Extract():
    """
    Clase para extraer datos de criptomonedas utilizando la API de CoinGecko.

    Parameters:
    base_url (str): La URL base de la API de CoinGecko.

    """

    def __init__(self, base_url):
        self.base_url = base_url
    
    def get_market_chart(self, id):
        """
        Obtiene los datos del gráfico de mercado para una criptomoneda específica.

        Parameters:
        id (str): El identificador de la criptomoneda.

        Returns:
        dict: Los datos del gráfico de mercado en formato JSON.

        Raises:
        requests.exceptions.RequestException: Si ocurre un error al realizar la solicitud.

        """

        endpoint = f"/coins/{id}/market_chart"
        url = self.base_url + endpoint
        parameters = {
            "id": id,
            "vs_currency": "usd",
            "days": "max",
            "interval": "daily",
            "precision": "3"
        }

        try:
            response = requests.get(url, params=parameters)
            response.raise_for_status()  # Verificar si la solicitud fue exitosa
            data = response.json()
            print("Solicitud exitosa")
            return data
        except requests.exceptions.RequestException as e:
            exception = f"Error al realizar la solicitud: {e}"
            return exception

    def get_criptos_top(self):
        """
        Obtiene los datos de las criptomonedas con mayor capitalización de mercado.

        Returns:
        dict: Los datos de las criptomonedas con mayor capitalización en formato JSON.

        Raises:
        requests.exceptions.RequestException: Si ocurre un error al realizar la solicitud.

        """

        endpoint = "/coins/markets"
        url = self.base_url + endpoint
        parameters = {
            "vs_currency": "usd"
        }

        try:
            response = requests.get(url, params=parameters)
            response.raise_for_status()  # Verificar si la solicitud fue exitosa
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            error = f"Error al realizar la solicitud: {e}"
            return error

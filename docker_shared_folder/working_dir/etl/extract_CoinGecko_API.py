import pandas as pd
import requests


class Extract():
    
    def __init__(self,base_url):
        self.base_url = base_url
    
    def get_market_chart(self,id):
        
        
        endpoint = f"/coins/{id}/market_chart"
        url = self.base_url + endpoint
        parameters = {
            "id" : id,
            "vs_currency" : "usd",
            "days" : "max",
            "interval" : "daily",
            "precision" : "3"
        }

        try:
            
            response = requests.get(url, params = parameters)
            response.raise_for_status()  # Verificar si la solicitud fue exitosa
            data = response.json()
            print("Solicitud exitosa")
            return data
        except requests.exceptions.RequestException as e:
            exception = f"Error al realizar la solicitud:, {e}" 
            return exception

    def get_criptos_top(self):
        endpoint = "/coins/markets"
        url = self.base_url + endpoint
        parameters = {
            "vs_currency" : "usd"
        }

        try:
            response = requests.get(url, params = parameters)
            response.raise_for_status()  # Verificar si la solicitud fue exitosa
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            error = f"Error al realizar la solicitud:, {e}"
            return error

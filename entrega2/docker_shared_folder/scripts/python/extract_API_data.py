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




if __name__ == "__main__":
    base_url = "https://api.coingecko.com/api/v3"
    etl = Extract(base_url)
    coins = etl.get_criptos_top()
    if isinstance(coins,str):
        print("Error:", coins)
    else:
        df = pd.DataFrame(coins)
        df.to_json('./../../data/coins.json')
        serie_id = df['id']
        list_id = list(serie_id)
        historical_per_id = {
            "id" : [],
            "Historical" : []
        }

        for elem in list_id:
            data = etl.get_market_chart(elem)
            try:
                data = pd.DataFrame(data)
            except:
                print(data)
            else:
                historical_per_id['id'].append(elem)
                historical_per_id['Historical'].append(data)    
                print(f"se agrego .......... {elem}")
        else:
            df_historical = pd.DataFrame(historical_per_id)
            df_historical.to_json('./../../data/coins_market_data.json')
        
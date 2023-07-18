import concurrent.futures
import requests

def fetch_market_chart(base_url, id, parameters):
    """
    Realiza una solicitud a la API para obtener el gráfico de mercado de una criptomoneda específica.

    Parameters:
        base_url (str): La URL base de la API de CoinGecko.
        id (str): El identificador de la criptomoneda.
        parameters (dict): Un diccionario que contiene los parámetros de la solicitud, como moneda de cambio, días, intervalo, etc.

    Returns:
        tuple: Una tupla que contiene el identificador de la criptomoneda y los datos del gráfico de mercado en formato JSON.

    Raises:
        requests.exceptions.RequestException: Si ocurre un error al realizar la solicitud.

    """
    endpoint = f"/coins/{id}/market_chart"
    url = base_url + endpoint

    try:
        response = requests.get(url, params=parameters)
        response.raise_for_status()
        data = response.json()
        print(f">>> Solicitud de id:{id} exitosa")
        return (id, data)

    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la solicitud: {e}")
        return None


def get_market_chart(base_url, id_list):
    """
    Obtiene los datos del gráfico de mercado para una lista de criptomonedas.

    Parameters:
        base_url (str): La URL base de la API de CoinGecko.
        id_list (list): Una lista de identificadores de criptomonedas.

    Returns:
        list: Una lista que contiene las tuplas de (identificador de criptomoneda, datos de gráfico de mercado en formato JSON).

    """
    market_chart_list = []
    parameters = {
        "vs_currency": "usd",
        "days": "max",
        "interval": "daily",
        "precision": "3"
    }
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_id = {executor.submit(fetch_market_chart, base_url, id, parameters): id for id in id_list}
        
        for future in concurrent.futures.as_completed(future_to_id):
            data = future.result()
            market_chart_list.append(data)
            
    return market_chart_list

def get_criptos_top(base_url):
    """
    Obtiene los datos de las criptomonedas con mayor capitalización de mercado.

    Parameters:
        base_url (str): La URL base de la API de CoinGecko.

    Returns:
        dict: Los datos de las criptomonedas con mayor capitalización en formato JSON.

    Raises:
        requests.exceptions.RequestException: Si ocurre un error al realizar la solicitud.

    """
    endpoint = "/coins/markets"
    url = base_url + endpoint
    parameters = {
        "vs_currency": "usd"
    }

    try:
        response = requests.get(url, params=parameters)
        response.raise_for_status()  # Verificar si la solicitud fue exitosa
        data = response.json()
        print(">>> Solicitud exitosa")
        return data
    except requests.exceptions.RequestException as e:
        error = f"Error al realizar la solicitud: {e}"
        return error

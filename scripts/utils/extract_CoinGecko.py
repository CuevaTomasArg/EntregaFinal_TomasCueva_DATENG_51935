import concurrent.futures
import requests

def fetch_market_chart(base_url, id):
    """
    Realiza una solicitud a la API para obtener el gráfico de mercado de una criptomoneda específica.

    Parameters:
    base_url (str): La URL base de la API.
    id (str): El identificador de la criptomoneda.

    Returns:
    dict: Los datos del gráfico de mercado en formato JSON.
    str: En caso de error, devuelve un mensaje de error.
    """
    endpoint = f"/coins/{id}/market_chart"
    url = base_url + endpoint
    parameters = {
        "id": None,
        "vs_currency": "usd",
        "days": "max",
        "interval": "daily",
        "precision": "3"
    }

    try:
        response = requests.get(url, params = parameters)
        response.raise_for_status()
        data = response.json()
        print(f">>> Solicitud de id:{id} exitosa")
        return (id, data)

    except requests.exceptions.RequestException as e:
        exception = f"Error al realizar la solicitud: {e}"
        return exception

def get_market_chart(base_url, id_list):
    """
    Obtiene los datos del gráfico de mercado para una lista de criptomonedas.

    Parameters:
    base_url (str): La URL base de la API.
    id_list (list): Una lista de identificadores de criptomonedas.

    Returns:
    list: Una lista que contiene los datos del gráfico de mercado de cada criptomoneda.
    """
    market_chart_list = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_id = {executor.submit(fetch_market_chart, base_url, id): id for id in id_list}
        
        for future in concurrent.futures.as_completed(future_to_id):
            id = future_to_id[future]
            try:
                data = future.result()
                market_chart_list.append(data)
            except Exception as e:
                exception = f"Error en la solicitud de {id}: {e}"
                market_chart_list.append(exception)

    return market_chart_list

    
def get_criptos_top(base_url):
    """
    Obtiene los datos de las criptomonedas con mayor capitalización de mercado.

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

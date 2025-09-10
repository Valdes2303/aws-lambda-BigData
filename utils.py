import requests

def get_dolar_data(url):
    """
    Realiza una solicitud GET a la URL de la API del Banco de la República
    y devuelve los datos en formato JSON.
    """
    response = requests.get(url)
    response.raise_for_status()  # Lanza una excepción si la respuesta no es 200 OK
    return response.json()
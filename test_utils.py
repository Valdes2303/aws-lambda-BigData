from unittest.mock import patch, MagicMock
import pytest
from utils import get_dolar_data

# URL de prueba para la simulación
TEST_URL = "https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario"

@patch('requests.get')
def test_get_dolar_data_success(mock_get):
    """
    Prueba que la función 'get_dolar_data' devuelva los datos correctos
    cuando la solicitud es exitosa.
    """
    # Configurar la respuesta simulada (mock)
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"valor": 4000}]  # Datos de prueba
    mock_get.return_value = mock_response

    # Llamar a la función que queremos probar
    datos = get_dolar_data(TEST_URL)

    # Afirmar que la función requests.get fue llamada con la URL correcta
    mock_get.assert_called_once_with(TEST_URL)

    # Afirmar que los datos devueltos son los esperados
    assert datos == [{"valor": 4000}]

@patch('requests.get')
def test_get_dolar_data_http_error(mock_get):
    """
    Prueba que la función 'get_dolar_data' lance una excepción
    cuando la solicitud HTTP falla (ej. 404 Not Found).
    """
    # Configurar la respuesta simulada para que lance un error HTTP
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
    mock_get.return_value = mock_response

    # Afirmar que la excepción se lanza correctamente
    with pytest.raises(requests.exceptions.HTTPError):
        get_dolar_data(TEST_URL)
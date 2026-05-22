"""
Cliente HTTP para interactuar con la API de Axur.
Responsabilidad: Realizar peticiones HTTP con reintentos y manejo de rate limiting.
"""
import logging
import time
from typing import Dict, List, Optional, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logger = logging.getLogger(__name__)


class AxurAPIClient:
    """Cliente para consumir la API de Axur con manejo de paginación y reintentos."""

    BASE_URL = "https://api.axur.com/gateway/1.0/api"
    MAX_RETRIES = 3
    INITIAL_WAIT = 1.0
    WAIT_INCREMENT = 0.5

    def __init__(self, headers: Dict[str, str], timeout: int = 30):
        """
        Inicializa el cliente API.

        Args:
            headers: Headers HTTP con autenticación.
            timeout: Timeout para las peticiones HTTP en segundos.
        """
        self.headers = headers
        self.timeout = timeout
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        Crea una sesión HTTP con configuración de reintentos.

        Returns:
            Sesión configurada de requests.
        """
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PATCH"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        return session

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None,
            retries: int = 0, wait_time: float = INITIAL_WAIT) -> Optional[Dict[str, Any]]:
        """
        Realiza una petición GET con manejo de rate limiting.

        Args:
            endpoint: Endpoint de la API (sin base URL).
            params: Parámetros de query string.
            retries: Número actual de reintentos.
            wait_time: Tiempo de espera entre reintentos.

        Returns:
            Respuesta JSON o None si falla.
        """
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"

        try:
            response = self.session.get(
                url,
                headers=self.headers,
                params=params,
                timeout=self.timeout
            )

            # Manejo de rate limiting (429)
            if response.status_code == 429:
                if retries >= self.MAX_RETRIES:
                    logger.error(f"Rate limit excedido después de {self.MAX_RETRIES} reintentos")
                    response.raise_for_status()

                logger.warning(f"Rate limit alcanzado. Esperando {wait_time}s antes de reintentar...")
                time.sleep(wait_time)
                return self.get(endpoint, params, retries + 1, wait_time + self.WAIT_INCREMENT)

            # Respuestas exitosas
            if 200 <= response.status_code < 300:
                return response.json() if response.text else {}

            # Otros errores
            logger.error(f"Error HTTP {response.status_code}: {response.text}")
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            logger.error(f"Error en petición a {url}: {e}")
            raise

    def get_paginated(self, endpoint: str, params: Dict[str, Any],
                     page_size: int = 200, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Obtiene todos los resultados paginados de un endpoint.

        Args:
            endpoint: Endpoint de la API.
            params: Parámetros base de la query.
            page_size: Tamaño de página (máx 200 para tickets, 1000 para credentials).
            max_pages: Límite máximo de páginas a obtener (None = sin límite).

        Returns:
            Lista con todos los registros obtenidos.
        """
        all_results = []
        page = 1

        while True:
            # Verificar límite de páginas
            if max_pages and page > max_pages:
                logger.info(f"Alcanzado límite de {max_pages} páginas")
                break

            params_with_page = {
                **params,
                'page': page,
                'pageSize': page_size
            }

            logger.info(f"Obteniendo página {page} de {endpoint}...")
            response = self.get(endpoint, params_with_page)

            if not response:
                break

            # Detectar campo de datos según el endpoint
            data_key = self._get_data_key(endpoint, response)
            results = response.get(data_key, [])

            if not results:
                logger.info(f"No hay más resultados en página {page}")
                break

            all_results.extend(results)
            logger.info(f"Obtenidos {len(results)} registros en página {page}")

            # Verificar si hay más páginas
            pageable = response.get('pageable', {})
            total_pages = pageable.get('total', 0) // page_size + 1

            if page >= total_pages:
                break

            page += 1

        logger.info(f"Total de registros obtenidos: {len(all_results)}")
        return all_results

    def _get_data_key(self, endpoint: str, response: Dict[str, Any]) -> str:
        """
        Identifica la clave donde están los datos en la respuesta.

        Args:
            endpoint: Endpoint consultado.
            response: Respuesta JSON de la API.

        Returns:
            Clave del array de datos.
        """
        if 'credentials' in endpoint:
            return 'detections'
        elif 'tickets' in endpoint:
            return 'tickets'

        # Detectar automáticamente
        for key in ['detections', 'tickets', 'data', 'results']:
            if key in response and isinstance(response[key], list):
                return key

        return 'data'

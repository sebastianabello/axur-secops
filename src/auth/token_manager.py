"""
Módulo de gestión de autenticación para la API de Axur.
Responsabilidad: Manejar tokens de autenticación y headers HTTP.
"""
import os
from typing import Dict


class TokenManager:
    """Gestiona la autenticación mediante Bearer Token."""

    def __init__(self, api_key: str = None):
        """
        Inicializa el gestor de tokens.

        Args:
            api_key: API Key de Axur. Si no se proporciona, se busca en variable de entorno.
        """
        self.api_key = api_key or os.getenv('AXUR_API_KEY')
        if not self.api_key:
            raise ValueError("API Key no proporcionada. Configura AXUR_API_KEY en variables de entorno.")

    def get_headers(self) -> Dict[str, str]:
        """
        Genera los headers HTTP necesarios para la autenticación.

        Returns:
            Diccionario con headers de autenticación.
        """
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def validate_token(self) -> bool:
        """
        Valida que el token esté configurado correctamente.

        Returns:
            True si el token es válido, False en caso contrario.
        """
        return bool(self.api_key and len(self.api_key) > 0)

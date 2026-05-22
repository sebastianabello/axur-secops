"""
Gestor de estado de sincronización.
Responsabilidad: Mantener y persistir marcas de tiempo de última sincronización.
"""
import json
import os
from datetime import datetime, timezone
from typing import Dict, Optional
from pathlib import Path
import logging


logger = logging.getLogger(__name__)


class StateManager:
    """Gestiona el estado de sincronización incremental."""

    def __init__(self, state_file: str = "sync_state.json"):
        """
        Inicializa el gestor de estado.

        Args:
            state_file: Ruta al archivo de estado.
        """
        self.state_file = Path(state_file)
        self.state: Dict[str, str] = self._load_state()

    def _load_state(self) -> Dict[str, str]:
        """
        Carga el estado desde el archivo.

        Returns:
            Diccionario con las marcas de tiempo por endpoint.
        """
        if not self.state_file.exists():
            logger.info(f"Archivo de estado {self.state_file} no existe. Creando uno nuevo.")
            return {}

        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                logger.info(f"Estado cargado: {state}")
                return state
        except json.JSONDecodeError as e:
            logger.error(f"Error al leer archivo de estado: {e}. Iniciando con estado vacío.")
            return {}

    def _save_state(self) -> None:
        """Persiste el estado actual en el archivo."""
        try:
            # Crear directorio si no existe
            self.state_file.parent.mkdir(parents=True, exist_ok=True)

            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
            logger.info(f"Estado guardado exitosamente en {self.state_file}")
        except IOError as e:
            logger.error(f"Error al guardar estado: {e}")

    def get_last_sync_time(self, endpoint: str) -> Optional[str]:
        """
        Obtiene la última marca de tiempo de sincronización para un endpoint.

        Args:
            endpoint: Identificador del endpoint (ej: 'tickets', 'credentials').

        Returns:
            Timestamp en formato ISO 8601 o None si es primera sincronización.
        """
        return self.state.get(endpoint)

    def update_sync_time(self, endpoint: str, timestamp: Optional[str] = None) -> None:
        """
        Actualiza la marca de tiempo de sincronización.

        Args:
            endpoint: Identificador del endpoint.
            timestamp: Timestamp en formato ISO 8601. Si es None, usa el tiempo actual.
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).isoformat()

        self.state[endpoint] = timestamp
        self._save_state()
        logger.info(f"Actualizado estado de '{endpoint}' a {timestamp}")

    def get_sync_filter(self, endpoint: str, field: str = "ticket.last-update.date") -> Dict[str, str]:
        """
        Genera el filtro de sincronización incremental.

        Args:
            endpoint: Identificador del endpoint.
            field: Campo de fecha a filtrar.

        Returns:
            Diccionario con el parámetro de filtro para la API.
        """
        last_sync = self.get_last_sync_time(endpoint)

        if last_sync:
            # Convertir timestamp ISO completo al formato aceptado por la API
            # API acepta: yyyy-MM-dd o yyyy-MM-ddTHH:mm:ss (sin microsegundos ni timezone)
            try:
                dt = datetime.fromisoformat(last_sync)
                # Formatear sin microsegundos ni timezone
                formatted_date = dt.strftime('%Y-%m-%dT%H:%M:%S')
                logger.info(f"Sincronización incremental desde {last_sync} (formato API: {formatted_date})")
                return {field: f"ge:{formatted_date}"}
            except ValueError as e:
                logger.error(f"Error parseando timestamp {last_sync}: {e}")
                return {}
        else:
            logger.info("Primera sincronización - obteniendo todos los datos")
            return {}

    def reset_endpoint(self, endpoint: str) -> None:
        """
        Resetea el estado de un endpoint específico.

        Args:
            endpoint: Identificador del endpoint a resetear.
        """
        if endpoint in self.state:
            del self.state[endpoint]
            self._save_state()
            logger.warning(f"Estado de '{endpoint}' reseteado")

    def reset_all(self) -> None:
        """Resetea todo el estado de sincronización."""
        self.state = {}
        self._save_state()
        logger.warning("Todo el estado de sincronización ha sido reseteado")

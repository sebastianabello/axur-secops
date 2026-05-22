"""
Transformador de datos Axur → Google SecOps.

Responsabilidad: Construir el objeto JSON individual que el parser CBN
de Google SecOps espera recibir como un evento independiente.

IMPORTANTE sobre la estructura del evento:
  - Para TICKETS: envía el ticket individual completo (ticket + detection + snapshots)
    El parser CBN lo normalizará a UDM.
  - Para CREDENTIALS: envía la credencial plana tal como llega de la API.
    Requiere un log type y parser CBN separado: AXUR_DRP_CREDENTIALS_CUSTOM.

Por qué dos log types:
  - La estructura de un ticket y una credencial son completamente distintas.
  - No es posible tener un parser CBN que maneje ambas estructuras sin hacerlo
    muy frágil. Dos log types = dos parsers = mantenibilidad y claridad.
  - En SecOps UI: AXUR_DRP_TICKETS_CUSTOM y AXUR_DRP_CREDENTIALS_CUSTOM.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Transforma registros crudos de Axur al formato de evento para SecOps.

    El transformador NO normaliza a UDM — eso lo hace el parser CBN.
    Su rol es:
      1. Construir el objeto JSON que el CBN recibirá como 'message'.
      2. Extraer el timestamp más apropiado para el campo logEntryTime.
      3. Enmascarar contraseñas (solo credenciales).
      4. Limpiar campos nulos.
    """

    def __init__(
        self,
        timezone: str = "Z",
        mask_passwords: bool = True,
        mask_chars: int = 4,
    ):
        """
        Args:
            timezone:       Timezone para queries (no afecta transform, se conserva para
                            compatibilidad con el orchestrator del proyecto base).
            mask_passwords: Si True, enmascara contraseñas en credenciales.
            mask_chars:     Caracteres visibles al enmascarar.
        """
        self.timezone       = timezone
        self.mask_passwords = mask_passwords
        self.mask_chars     = mask_chars

    # ─── API pública ──────────────────────────────────────────────────────────

    def transform_ticket(self, raw_ticket: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        Transforma un ticket crudo de Axur al objeto de evento para SecOps.

        La API de Axur devuelve tickets con la estructura:
          { ticket: {...}, detection: {...}, snapshots: {...}, attachments: [...] }

        Para el parser CBN necesitamos un objeto con EXACTAMENTE UN ticket individual,
        con los datos del feed aplanados al nivel raíz del objeto (no dentro de
        collectionData.tickets[]).

        Args:
            raw_ticket: Elemento individual del array collectionData.tickets[]
                        (o tickets[] de la respuesta paginada de la API).

        Returns:
            Tuple (event_dict, log_entry_time):
              - event_dict: El JSON completo a enviar como log.
              - log_entry_time: Timestamp RFC3339 para logEntryTime (o None).
        """
        ticket_info = raw_ticket.get("ticket", {})
        detection   = raw_ticket.get("detection", {})
        snapshots   = raw_ticket.get("snapshots", {})
        attachments = raw_ticket.get("attachments", [])
        texts       = raw_ticket.get("texts", [])

        # Construir el evento en la estructura que el parser CBN espera
        # (según AXUR_DRP_CUSTOM.conf que ya tenemos)
        event = {
            "ticket":      self._clean(ticket_info),
            "detection":   self._clean(detection),
            "snapshots":   snapshots,          # se conserva nested para el parser
            "attachments": attachments,
            "texts":       texts,
            # Metadata de control (no se parsea en UDM, útil para debugging)
            "_meta": {
                "source":     "axur_drp",
                "data_type":  "ticket",
                "synced_at":  self._now_iso(),
            },
        }

        # Timestamp para logEntryTime (prioridad: last-update.date > creation.date)
        log_entry_time = (
            self._normalize_ts(ticket_info.get("last-update.date"))
            or self._normalize_ts(ticket_info.get("creation.date"))
        )

        return event, log_entry_time

    def transform_credential(self, raw_credential: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        Transforma una credencial cruda de Axur al objeto de evento para SecOps.

        La API de Axur de credenciales devuelve objetos planos (no nested como tickets):
          { "id": "...", "user": "...", "password": "...", "status": "...", ... }

        Args:
            raw_credential: Un elemento del array detections[] de la API.

        Returns:
            Tuple (event_dict, log_entry_time).
        """
        # Enmascarar contraseña
        cred = dict(raw_credential)
        if self.mask_passwords and cred.get("password"):
            cred["password"] = self._mask_password(cred["password"])

        event = {
            **self._clean(cred),
            # Metadata de control
            "_meta": {
                "source":    "axur_drp",
                "data_type": "credential",
                "synced_at": self._now_iso(),
            },
        }

        # Timestamp para logEntryTime: updated > created > detectionDate
        log_entry_time = (
            self._normalize_ts(raw_credential.get("updated"))
            or self._normalize_ts(raw_credential.get("created"))
            or self._normalize_ts(raw_credential.get("detectionDate"))
        )

        return event, log_entry_time

    def transform_batch(
        self,
        data: List[Dict[str, Any]],
        data_type: str = "ticket",
    ) -> List[Dict[str, Any]]:
        """
        Transforma un lote de registros.
        Retorna lista de dicts con claves 'data' y 'logEntryTime'
        en el formato que espera SecOpsClient.ingest_batch().

        Args:
            data:      Lista de registros crudos de la API de Axur.
            data_type: 'ticket' o 'credential'.

        Returns:
            Lista de {"data": {...}, "logEntryTime": "..."}.
        """
        transformer = (
            self.transform_ticket if data_type == "ticket"
            else self.transform_credential
        )

        result = []
        errors = 0

        for i, item in enumerate(data):
            try:
                event_dict, log_entry_time = transformer(item)
                entry = {"data": event_dict}
                if log_entry_time:
                    entry["logEntryTime"] = log_entry_time
                result.append(entry)
            except Exception as exc:
                errors += 1
                item_id = (
                    item.get("ticket", {}).get("ticketKey")
                    or item.get("id")
                    or f"index_{i}"
                )
                logger.error("Error transformando %s %s: %s", data_type, item_id, exc)

        logger.info(
            "Transformados %d %ss (%d errores de %d total)",
            len(result), data_type, errors, len(data)
        )
        return result

    # ─── Helpers privados ─────────────────────────────────────────────────────

    def _normalize_ts(self, ts: Any) -> Optional[str]:
        """
        Normaliza un timestamp a formato RFC3339 con 'Z'.

        Acepta:
          - str ISO 8601: '2026-05-20T12:57:38Z' o '2026-05-20T12:57:38'
          - int en milisegundos: 1778685501030
          - None → None
        """
        if ts is None:
            return None

        try:
            if isinstance(ts, (int, float)):
                # Milisegundos epoch
                dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            if isinstance(ts, str):
                ts_clean = ts.strip()
                if not ts_clean:
                    return None
                # Normalizar a formato con Z
                if ts_clean.endswith("Z"):
                    return ts_clean
                if "+" in ts_clean or (ts_clean.count("-") > 2):
                    # Tiene offset timezone
                    dt = datetime.fromisoformat(ts_clean)
                    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                # Sin timezone: asumir UTC
                return ts_clean + "Z"

        except Exception as exc:
            logger.debug("No se pudo parsear timestamp '%s': %s", ts, exc)

        return None

    def _mask_password(self, password: str) -> str:
        """Muestra los primeros N chars y enmascara el resto con asteriscos."""
        if not password:
            return password
        if len(password) <= self.mask_chars:
            return "*" * len(password)
        return password[: self.mask_chars] + "*" * (len(password) - self.mask_chars)

    def _clean(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Elimina campos None y strings vacíos del primer nivel del dict."""
        return {
            k: v for k, v in data.items()
            if v is not None and v != "" and v != [] and v != {}
        }

    def _now_iso(self) -> str:
        """Timestamp UTC actual en ISO 8601."""
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

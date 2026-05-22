"""
Cliente de ingesta para Google SecOps (Chronicle).

Soporta 3 métodos de autenticación:
  1. Webhook (API Key + Secret) → Modo principal. Feed HTTPS configurado en SecOps UI.
  2. Service Account JSON file  → Alternativa. ADC estándar para producción.
  3. Service Account JSON env   → Alternativa. Para Docker/Cloud Run sin archivos.

Modo webhook (endpoint):
  https://{region}-chronicle.googleapis.com/v1alpha/
    projects/{project}/locations/{region}/instances/{instance}/
    feeds/{feed_id}:importPushLogs

  Cada evento se envía como un POST individual con el JSON plano en el body.
  Sin base64, sin wrapper. El log type está configurado en el feed de SecOps UI.

Modo SA (endpoint):
  https://{region}-chronicle.googleapis.com/v1alpha/
    projects/{project}/locations/{region}/instances/{instance}/
    logTypes/{log_type}/logs:import
"""

import base64
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# ─── Constantes ───────────────────────────────────────────────────────────────
CHRONICLE_API_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
TOKEN_URL           = "https://oauth2.googleapis.com/token"
BATCH_SIZE_MAX      = 1000      # máx. logs por request según docs Chronicle
REQUEST_SIZE_MAX_MB = 4         # límite de tamaño total por request
LOG_SIZE_MAX_MB     = 1         # límite por log individual

# Log types separados por tipo de dato Axur
LOG_TYPE_TICKETS     = "AXUR_DRP_TICKETS_CUSTOM"
LOG_TYPE_CREDENTIALS = "AXUR_DRP_CREDENTIALS_CUSTOM"


class AuthMode:
    SA_FILE   = "sa_file"    # JSON key file en disco
    SA_ENV    = "sa_env"     # JSON key como variable de entorno (base64 o raw)
    WEBHOOK   = "webhook"    # API Key + Webhook Secret (feed HTTPS en SecOps UI)


class SecOpsClient:
    """
    Cliente para ingestar logs hacia Google SecOps Chronicle Ingestion API v1alpha.

    Ejemplos de uso:

        # Opción 1: Service Account desde archivo
        client = SecOpsClient.from_service_account_file(
            sa_key_file="/secrets/chronicle-sa.json",
            project_id="mi-proyecto",
            region="us",
            instance_id="uuid-del-tenant",
            forwarder_id="uuid-del-forwarder"
        )

        # Opción 2: Service Account desde variable de entorno
        client = SecOpsClient.from_service_account_env(
            project_id="mi-proyecto",
            region="us",
            instance_id="uuid-del-tenant",
            forwarder_id="uuid-del-forwarder"
        )

        # Opción 3: Webhook (API Key + Secret del feed)
        client = SecOpsClient.from_webhook(
            project_id="mi-proyecto",
            region="us",
            instance_id="uuid-del-tenant",
            feed_id="uuid-del-feed",
            api_key="X-goog-api-key",
            webhook_secret="X-Webhook-Access-Key"
        )
    """

    def __init__(
        self,
        auth_mode: str,
        project_id: str,
        region: str,
        instance_id: str,
        # Para SA auth
        forwarder_id: Optional[str] = None,
        sa_credentials: Optional[Dict] = None,
        # Para Webhook auth
        feed_id: Optional[str] = None,
        api_key: Optional[str] = None,
        webhook_secret: Optional[str] = None,
        # Configuración general
        max_retries: int = 3,
        retry_backoff: float = 2.0,
        timeout: int = 30,
    ):
        self.auth_mode      = auth_mode
        self.project_id     = project_id
        self.region         = region
        self.instance_id    = instance_id
        self.forwarder_id   = forwarder_id
        self.sa_credentials = sa_credentials
        self.feed_id        = feed_id
        self.api_key        = api_key
        self.webhook_secret = webhook_secret
        self.max_retries    = max_retries
        self.retry_backoff  = retry_backoff
        self.timeout        = timeout

        # Cache de access token (SA auth)
        self._access_token: Optional[str]  = None
        self._token_expiry: float          = 0.0

        self.session = requests.Session()
        logger.info(f"SecOpsClient inicializado | modo={auth_mode} | región={region} | instancia={instance_id}")

    # ─── Constructores alternativos ───────────────────────────────────────────

    @classmethod
    def from_service_account_file(
        cls,
        sa_key_file: str,
        project_id: str,
        region: str,
        instance_id: str,
        forwarder_id: str,
        **kwargs
    ) -> "SecOpsClient":
        """
        Crea cliente autenticado con un archivo JSON de Service Account.

        Args:
            sa_key_file: Ruta al archivo .json de la Service Account.
            project_id:  GCP Project ID del tenant de Chronicle.
            region:      Región del tenant (ej: 'us', 'europe', 'asia-southeast1').
            instance_id: UUID del tenant de Chronicle (Settings → Profile).
            forwarder_id: UUID del Forwarder lógico creado en Chronicle.
        """
        if not os.path.exists(sa_key_file):
            raise FileNotFoundError(f"Archivo de Service Account no encontrado: {sa_key_file}")

        with open(sa_key_file, "r") as f:
            credentials = json.load(f)

        logger.info(f"SA cargada desde archivo: {sa_key_file} ({credentials.get('client_email', 'N/A')})")
        return cls(
            auth_mode=AuthMode.SA_FILE,
            project_id=project_id,
            region=region,
            instance_id=instance_id,
            forwarder_id=forwarder_id,
            sa_credentials=credentials,
            **kwargs
        )

    @classmethod
    def from_service_account_env(
        cls,
        project_id: str,
        region: str,
        instance_id: str,
        forwarder_id: str,
        env_var: str = "CHRONICLE_SA_KEY_JSON",
        **kwargs
    ) -> "SecOpsClient":
        """
        Crea cliente con SA JSON desde variable de entorno.
        Acepta JSON raw o JSON codificado en base64.

        Args:
            env_var: Nombre de la variable de entorno (default: CHRONICLE_SA_KEY_JSON).
        """
        raw = os.getenv(env_var, "")
        if not raw:
            raise ValueError(f"Variable de entorno {env_var} no está configurada o está vacía")

        # Intentar base64 primero, luego raw JSON
        try:
            decoded = base64.b64decode(raw).decode("utf-8")
            credentials = json.loads(decoded)
            logger.info(f"SA cargada desde {env_var} (base64)")
        except Exception:
            try:
                credentials = json.loads(raw)
                logger.info(f"SA cargada desde {env_var} (raw JSON)")
            except json.JSONDecodeError:
                raise ValueError(f"El contenido de {env_var} no es JSON válido ni base64 de JSON")

        return cls(
            auth_mode=AuthMode.SA_ENV,
            project_id=project_id,
            region=region,
            instance_id=instance_id,
            forwarder_id=forwarder_id,
            sa_credentials=credentials,
            **kwargs
        )

    @classmethod
    def from_webhook(
        cls,
        project_id: str,
        region: str,
        instance_id: str,
        feed_id: str,
        api_key: str,
        webhook_secret: str,
        **kwargs
    ) -> "SecOpsClient":
        """
        Crea cliente que usa el endpoint de Webhook Feed (HTTPS push).
        Este modo no usa Service Account — usa las credenciales del feed configurado
        en la UI de SecOps (Settings → Feeds → HTTPS Webhook).

        Endpoint utilizado:
            https://{region}-chronicle.googleapis.com/v1alpha/
              projects/{project}/locations/{region}/instances/{instance}/
              feeds/{feed_id}:importPushLogs

        Args:
            feed_id:        ID del feed HTTPS configurado en SecOps UI.
            api_key:        Valor de X-goog-api-key del feed (cada feed tiene el suyo).
            webhook_secret: Valor de X-Webhook-Access-Key del feed (cada feed tiene el suyo).

        IMPORTANTE: Cada feed en SecOps tiene su propio feed_id, api_key y webhook_secret.
                    Para tickets y credentials (dos log types distintos) se deben crear
                    dos feeds en SecOps UI y construir dos SecOpsClient separados,
                    uno por feed, con sus credenciales independientes.
                    El orquestador hace esto automáticamente via _build_secops_client().
        """
        return cls(
            auth_mode=AuthMode.WEBHOOK,
            project_id=project_id,
            region=region,
            instance_id=instance_id,
            feed_id=feed_id,
            api_key=api_key,
            webhook_secret=webhook_secret,
            **kwargs
        )

    # ─── Autenticación SA ─────────────────────────────────────────────────────

    def _get_access_token(self) -> str:
        """
        Obtiene un access token OAuth2 para la Service Account.
        Cachea el token hasta 60s antes de su expiración.
        """
        if self._access_token and time.time() < self._token_expiry - 60:
            return self._access_token

        import math

        creds = self.sa_credentials
        if not creds:
            raise ValueError("No hay credenciales de Service Account configuradas")

        # Construir JWT para el token request
        import hmac, hashlib

        now = int(time.time())
        exp = now + 3600

        header  = {"alg": "RS256", "typ": "JWT"}
        payload = {
            "iss":   creds["client_email"],
            "scope": CHRONICLE_API_SCOPE,
            "aud":   TOKEN_URL,
            "iat":   now,
            "exp":   exp,
        }

        def b64url(data: bytes) -> str:
            return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

        header_b64  = b64url(json.dumps(header, separators=(",", ":")).encode())
        payload_b64 = b64url(json.dumps(payload, separators=(",", ":")).encode())
        signing_input = f"{header_b64}.{payload_b64}"

        # Firmar con RSA-SHA256 usando la private key del SA JSON
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding

        private_key = serialization.load_pem_private_key(
            creds["private_key"].encode(),
            password=None,
        )
        signature = private_key.sign(signing_input.encode(), padding.PKCS1v15(), hashes.SHA256())
        jwt_token = f"{signing_input}.{b64url(signature)}"

        # Intercambiar JWT por access token
        resp = self.session.post(
            TOKEN_URL,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion":  jwt_token,
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()
        token_data = resp.json()

        self._access_token = token_data["access_token"]
        self._token_expiry = now + token_data.get("expires_in", 3600)
        logger.debug("Access token renovado para SA %s", creds.get("client_email"))
        return self._access_token

    # ─── Construcción de endpoints ────────────────────────────────────────────

    def _import_endpoint(self, log_type: str) -> str:
        """
        Endpoint de la Chronicle Ingestion API v1alpha para logs:import.
        Usa el endpoint regional locational (más confiable que el global).
        """
        parent = (
            f"projects/{self.project_id}"
            f"/locations/{self.region}"
            f"/instances/{self.instance_id}"
        )
        return (
            f"https://{self.region}-chronicle.googleapis.com/v1alpha"
            f"/{parent}/logTypes/{log_type}/logs:import"
        )

    def _webhook_endpoint(self) -> str:
        """Endpoint del feed HTTPS (modo webhook)."""
        parent = (
            f"projects/{self.project_id}"
            f"/locations/{self.region}"
            f"/instances/{self.instance_id}"
        )
        return (
            f"https://{self.region}-chronicle.googleapis.com/v1alpha"
            f"/{parent}/feeds/{self.feed_id}:importPushLogs"
        )

    def _forwarder_resource(self) -> str:
        """Nombre completo del recurso forwarder (requerido en logs:import)."""
        return (
            f"projects/{self.project_id}"
            f"/locations/{self.region}"
            f"/instances/{self.instance_id}"
            f"/forwarders/{self.forwarder_id}"
        )

    # ─── Construcción de payloads ─────────────────────────────────────────────

    def _build_sa_payload(self, log_entries: List[Dict]) -> Dict:
        """
        Construye el payload para logs:import (SA auth).
        Cada entrada debe tener: data (str), logEntryTime (ISO8601, opcional).
        """
        logs = []
        for entry in log_entries:
            raw_json = json.dumps(entry["data"], ensure_ascii=False, separators=(",", ":"))
            data_b64  = base64.b64encode(raw_json.encode("utf-8")).decode("ascii")
            log_obj   = {"data": data_b64}
            if entry.get("logEntryTime"):
                log_obj["logEntryTime"] = entry["logEntryTime"]
            logs.append(log_obj)

        return {
            "inlineSource": {
                "forwarder": self._forwarder_resource(),
                "logs": logs,
            }
        }

    # ─── HTTP con reintentos ──────────────────────────────────────────────────

    def _post_with_retry(self, url: str, headers: Dict, payload: Dict) -> bool:
        """Envía POST con reintentos y backoff exponencial."""
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.post(url, headers=headers, json=payload, timeout=self.timeout)

                if resp.status_code == 200:
                    return True

                if resp.status_code == 429:
                    wait = self.retry_backoff ** attempt
                    logger.warning("Rate limit (429). Intento %d/%d. Esperando %.1fs...",
                                   attempt, self.max_retries, wait)
                    time.sleep(wait)
                    continue

                if resp.status_code in (500, 502, 503, 504):
                    wait = self.retry_backoff ** attempt
                    logger.warning("Error servidor %d. Intento %d/%d. Esperando %.1fs...",
                                   resp.status_code, attempt, self.max_retries, wait)
                    time.sleep(wait)
                    continue

                logger.error("Error %d al ingestar: %s", resp.status_code, resp.text[:300])
                return False

            except requests.RequestException as exc:
                wait = self.retry_backoff ** attempt
                logger.warning("Excepción en intento %d/%d: %s. Esperando %.1fs...",
                               attempt, self.max_retries, exc, wait)
                time.sleep(wait)

        logger.error("Falló ingesta tras %d intentos", self.max_retries)
        return False

    # ─── API pública ──────────────────────────────────────────────────────────

    def ingest_batch(
        self,
        log_entries: List[Dict],
        log_type: str,
        batch_size: int = 500,
    ) -> Dict[str, int]:
        """
        Ingesta un lote de eventos a Google SecOps.

        Args:
            log_entries: Lista de dicts con:
                - data (dict): el objeto JSON del evento
                - logEntryTime (str, opcional): RFC3339 timestamp
            log_type: Log type de destino (ej: AXUR_DRP_TICKETS_CUSTOM).
                      Ignorado en modo webhook (configurado en el feed).
            batch_size: Logs por request (máx 1000, recomendado ≤500).

        Returns:
            {"sent": N, "failed": M}
        """
        if not log_entries:
            return {"sent": 0, "failed": 0}

        effective_batch = min(batch_size, BATCH_SIZE_MAX)
        stats = {"sent": 0, "failed": 0}

        # Dividir en batches
        for i in range(0, len(log_entries), effective_batch):
            chunk = log_entries[i : i + effective_batch]
            sent_c, failed_c = self._send_chunk(chunk, log_type)
            stats["sent"]   += sent_c
            stats["failed"] += failed_c
            end = i + len(chunk) - 1
            if sent_c > 0:
                logger.info("✓ Batch [%d-%d]: %d enviados", i, end, sent_c)
            if failed_c > 0:
                logger.error("✗ Batch [%d-%d]: %d fallidos", i, end, failed_c)

        logger.info("Resultado ingesta: %s", stats)
        return stats

    def _send_chunk(self, chunk: List[Dict], log_type: str) -> Tuple[int, int]:
        """Envía un chunk al endpoint correcto según el modo de auth. Retorna (sent, failed)."""
        if self.auth_mode in (AuthMode.SA_FILE, AuthMode.SA_ENV):
            return self._send_chunk_sa(chunk, log_type)
        elif self.auth_mode == AuthMode.WEBHOOK:
            return self._send_chunk_webhook(chunk)
        else:
            raise ValueError(f"Modo de autenticación desconocido: {self.auth_mode}")

    def _send_chunk_sa(self, chunk: List[Dict], log_type: str) -> Tuple[int, int]:
        """Envía chunk usando Service Account auth (Bearer token)."""
        token   = self._get_access_token()
        url     = self._import_endpoint(log_type)
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        }
        payload = self._build_sa_payload(chunk)
        ok = self._post_with_retry(url, headers, payload)
        return (len(chunk), 0) if ok else (0, len(chunk))

    def _send_chunk_webhook(self, chunk: List[Dict]) -> Tuple[int, int]:
        """
        Envía cada evento como un POST individual al endpoint de webhook feed.
        El body es el JSON plano del evento: sin base64, sin wrapper.
        """
        url = self._webhook_endpoint()
        headers = {
            "Content-Type":         "application/json",
            "X-goog-api-key":       self.api_key or "",
            "X-Webhook-Access-Key": self.webhook_secret or "",
        }
        sent = 0
        failed = 0
        for entry in chunk:
            ok = self._post_with_retry(url, headers, entry["data"])
            if ok:
                sent += 1
            else:
                failed += 1
        return sent, failed

    def ingest_single(self, data: Dict, log_type: str, log_entry_time: Optional[str] = None) -> bool:
        """
        Ingesta un único evento. Conveniente para testing.

        Args:
            data:           Diccionario con los datos del evento.
            log_type:       Log type de destino.
            log_entry_time: Timestamp RFC3339 del evento (opcional).

        Returns:
            True si fue exitoso.
        """
        entry = {"data": data}
        if log_entry_time:
            entry["logEntryTime"] = log_entry_time
        result = self.ingest_batch([entry], log_type=log_type, batch_size=1)
        return result["failed"] == 0

    def health_check(self, log_type: str) -> bool:
        """
        Verifica que la autenticación y el endpoint sean accesibles
        enviando un evento de prueba mínimo.
        """
        test_event = {"data": {"_health_check": True, "source": "axur-secops"}}
        logger.info("Ejecutando health check hacia log_type=%s...", log_type)
        sent, failed = self._send_chunk([test_event], log_type)
        ok = sent > 0 and failed == 0
        if ok:
            logger.info("✓ Health check exitoso")
        else:
            logger.error("✗ Health check falló")
        return ok

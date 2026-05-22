"""
Orquestador principal del sistema de sincronización Axur → Google SecOps.
Adaptado desde axur-elastic/src/orchestrator.py.

Cambios respecto al proyecto base:
  - ElasticsearchClient → SecOpsClient
  - Dos log types distintos: AXUR_DRP_TICKETS_CUSTOM y AXUR_DRP_CREDENTIALS_CUSTOM
  - Sin índices, sin mappings: SecOps gestiona el storage internamente
  - Sin multi-cluster: SecOps tiene un único tenant por instancia
  - Lógica de paginación y estado incremental: IDÉNTICA al proyecto base
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.api.axur_client import AxurAPIClient
from src.auth.token_manager import TokenManager
from src.storage.secops_client import AuthMode, SecOpsClient
from src.sync.state_manager import StateManager
from src.transform.data_transformer import DataTransformer

logger = logging.getLogger(__name__)

# Log types en Google SecOps (deben existir antes de ingestar)
LOG_TYPE_TICKETS     = "AXUR_DRP_TICKETS_CUSTOM"
LOG_TYPE_CREDENTIALS = "AXUR_DRP_CREDENTIALS_CUSTOM"


class SyncOrchestrator:
    """Orquesta la sincronización de datos desde Axur hacia Google SecOps."""

    def __init__(self, config: Dict[str, Any]):
        """
        Inicializa el orquestador con la configuración del sistema.

        Args:
            config: Diccionario con las siguientes claves:

            # Axur
            axur_api_key     (str)  : API Key de Axur
            customers        (list) : Lista de customerKeys a sincronizar
            timezone         (str)  : Offset UTC para queries (default 'Z')
            timeout          (int)  : Timeout HTTP en segundos (default 30)

            # Google SecOps — común
            chronicle_project_id  (str) : GCP Project ID del tenant
            chronicle_region      (str) : Región (ej: 'us', 'europe')
            chronicle_instance_id (str) : UUID del tenant de Chronicle

            # Google SecOps — Webhook auth (modo principal, recomendado)
            chronicle_auth_mode              (str) : 'webhook' (default recomendado)
            chronicle_tickets_feed_id        (str) : Feed ID del feed de tickets en SecOps UI
            chronicle_tickets_api_key        (str) : X-goog-api-key del feed de tickets
            chronicle_tickets_webhook_secret (str) : X-Webhook-Access-Key del feed de tickets
            chronicle_creds_feed_id          (str) : Feed ID del feed de credentials en SecOps UI
            chronicle_creds_api_key          (str) : X-goog-api-key del feed de credentials
            chronicle_creds_webhook_secret   (str) : X-Webhook-Access-Key del feed de credentials

            # Google SecOps — SA auth (alternativa, requiere archivo JSON de credenciales)
            chronicle_sa_key_file  (str) : Ruta al archivo JSON de la SA
            chronicle_forwarder_id (str) : UUID del forwarder lógico en Chronicle

            # Estado
            state_file       (str) : Ruta al archivo sync_state.json
            mask_passwords   (bool): Enmascarar contraseñas (default True)
        """
        self.config = config

        # Componentes de Axur (idénticos al proyecto base)
        self.token_manager = TokenManager(api_key=config.get("axur_api_key"))
        self.api_client    = AxurAPIClient(
            headers=self.token_manager.get_headers(),
            timeout=config.get("timeout", 30),
        )
        self.state_manager = StateManager(
            state_file=config.get("state_file", "sync_state.json")
        )
        self.transformer   = DataTransformer(
            timezone=config.get("timezone", "Z"),
            mask_passwords=config.get("mask_passwords", True),
        )

        # Componentes de Google SecOps
        self.secops_tickets     = self._build_secops_client(data_type="tickets")
        self.secops_credentials = self._build_secops_client(data_type="credentials")

        logger.info("Orquestador inicializado correctamente")

    def _build_secops_client(self, data_type: str) -> SecOpsClient:
        """
        Construye el SecOpsClient según el modo de autenticación configurado.

        data_type: 'tickets' o 'credentials' (relevante solo para webhook,
                   donde cada tipo puede tener un feed distinto).
        """
        cfg      = self.config
        auth_mode = cfg.get("chronicle_auth_mode", AuthMode.SA_FILE)
        project   = cfg["chronicle_project_id"]
        region    = cfg["chronicle_region"]
        instance  = cfg["chronicle_instance_id"]

        if auth_mode == AuthMode.WEBHOOK:
            # Cada feed tiene su propio feed_id, api_key y webhook_secret independientes.
            # Variables por tipo:
            #   Tickets:     CHRONICLE_TICKETS_FEED_ID / CHRONICLE_TICKETS_API_KEY / CHRONICLE_TICKETS_WEBHOOK_SECRET
            #   Credentials: CHRONICLE_CREDS_FEED_ID   / CHRONICLE_CREDS_API_KEY   / CHRONICLE_CREDS_WEBHOOK_SECRET
            if data_type == "tickets":
                feed_id        = cfg["chronicle_tickets_feed_id"]
                api_key        = cfg["chronicle_tickets_api_key"]
                webhook_secret = cfg["chronicle_tickets_webhook_secret"]
            else:
                feed_id        = cfg["chronicle_creds_feed_id"]
                api_key        = cfg["chronicle_creds_api_key"]
                webhook_secret = cfg["chronicle_creds_webhook_secret"]

            return SecOpsClient.from_webhook(
                project_id=project,
                region=region,
                instance_id=instance,
                feed_id=feed_id,
                api_key=api_key,
                webhook_secret=webhook_secret,
            )

        # Modo SA: detectar si viene de archivo o de env var
        sa_file = cfg.get("chronicle_sa_key_file", "")
        if sa_file:
            return SecOpsClient.from_service_account_file(
                sa_key_file=sa_file,
                project_id=project,
                region=region,
                instance_id=instance,
                forwarder_id=cfg["chronicle_forwarder_id"],
            )
        else:
            # SA desde variable de entorno CHRONICLE_SA_KEY_JSON
            return SecOpsClient.from_service_account_env(
                project_id=project,
                region=region,
                instance_id=instance,
                forwarder_id=cfg["chronicle_forwarder_id"],
            )

    # ─── Sincronización de tickets ────────────────────────────────────────────

    def sync_tickets(
        self,
        ticket_types:  Optional[List[str]] = None,
        full_sync:     bool                = False,
        max_pages:     Optional[int]        = None,
        customers:     Optional[List[str]]  = None,
        batch_size:    int                  = 500,
    ) -> Dict[str, int]:
        """
        Sincroniza tickets de Axur hacia Google SecOps (log type: AXUR_DRP_TICKETS_CUSTOM).

        Args:
            ticket_types: Tipos de ticket a filtrar (ej: ['phishing', 'paid-search']).
                          None = todos los tipos.
            full_sync:    Si True, ignora el estado incremental y trae todo.
            max_pages:    Límite de páginas (útil para pruebas).
            customers:    Lista de customerKeys. None = usa config.customers.
            batch_size:   Logs por request a SecOps (máx 1000).

        Returns:
            {"fetched": N, "transformed": N, "sent": N, "failed": N}
        """
        target_customers = customers or self.config.get("customers", [])
        logger.info("Sincronizando tickets para clientes: %s", target_customers or "todos")

        # Preparar parámetros Axur
        params = {
            "sortBy": "ticket.last-update.date",
            "order":  "asc",
            "include": "fields",
        }

        if ticket_types:
            params["type"] = ",".join(ticket_types)

        if target_customers:
            params["ticket.customer"] = ",".join(target_customers)

        # Filtro incremental (idéntico al proyecto base)
        state_key = "tickets"
        if not full_sync:
            sync_filter = self.state_manager.get_sync_filter(
                state_key, "ticket.last-update.date"
            )
            params.update(sync_filter)

        # Consultar API de Axur
        logger.info("Consultando Axur tickets con params: %s", params)
        raw_tickets = self.api_client.get_paginated(
            endpoint="tickets-api/tickets",
            params=params,
            page_size=200,
            max_pages=max_pages,
        )

        if not raw_tickets:
            logger.info("No hay nuevos tickets para sincronizar")
            return {"fetched": 0, "transformed": 0, "sent": 0, "failed": 0}

        logger.info("Obtenidos %d tickets de Axur", len(raw_tickets))

        # Transformar
        log_entries = self.transformer.transform_batch(raw_tickets, data_type="ticket")

        # Enviar a SecOps
        result = self.secops_tickets.ingest_batch(
            log_entries=log_entries,
            log_type=LOG_TYPE_TICKETS,
            batch_size=batch_size,
        )

        # Actualizar estado incremental
        if result["sent"] > 0:
            self.state_manager.update_sync_time(
                state_key,
                datetime.now(timezone.utc).isoformat()
            )

        stats = {
            "fetched":     len(raw_tickets),
            "transformed": len(log_entries),
            "sent":        result["sent"],
            "failed":      result["failed"],
        }
        logger.info("Sincronización de tickets completada: %s", stats)
        return stats

    # ─── Sincronización de credentials ───────────────────────────────────────

    def sync_credentials(
        self,
        statuses:   Optional[List[str]] = None,
        full_sync:  bool                = False,
        max_pages:  Optional[int]        = None,
        customers:  Optional[List[str]]  = None,
        batch_size: int                  = 500,
    ) -> Dict[str, int]:
        """
        Sincroniza credenciales expuestas de Axur hacia SecOps
        (log type: AXUR_DRP_CREDENTIALS_CUSTOM).

        La API de credentials de Axur requiere un customer por request,
        por lo que se itera sobre la lista de customers.

        Args:
            statuses:  Estados a filtrar (NEW, IN_TREATMENT, SOLVED, DISCARDED).
            full_sync: Si True, sincroniza todo ignorando el estado.
            max_pages: Límite de páginas por customer.
            customers: Lista de customerKeys. None = usa config.customers.
            batch_size: Logs por request a SecOps.

        Returns:
            {"fetched": N, "transformed": N, "sent": N, "failed": N}
        """
        target_customers = customers or self.config.get("customers", [])

        if not target_customers:
            logger.warning(
                "No hay customers configurados para sync de credentials. "
                "Define AXUR_CUSTOMERS en .env"
            )
            return {"fetched": 0, "transformed": 0, "sent": 0, "failed": 0}

        total = {"fetched": 0, "transformed": 0, "sent": 0, "failed": 0}

        for customer in target_customers:
            logger.info("--- Sincronizando credentials de %s ---", customer)
            try:
                stats = self._sync_credentials_customer(
                    customer=customer,
                    statuses=statuses,
                    full_sync=full_sync,
                    max_pages=max_pages,
                    batch_size=batch_size,
                )
                for k in total:
                    total[k] += stats.get(k, 0)
            except Exception as exc:
                logger.error("Error sincronizando credentials de %s: %s", customer, exc)
                total["failed"] += 1

        logger.info("Sincronización de credentials completada: %s", total)
        return total

    def _sync_credentials_customer(
        self,
        customer:   str,
        statuses:   Optional[List[str]] = None,
        full_sync:  bool                = False,
        max_pages:  Optional[int]        = None,
        batch_size: int                  = 500,
    ) -> Dict[str, int]:
        """Sincroniza credenciales de un customer específico."""
        params: Dict[str, Any] = {
            "sortBy":   "updated",
            "order":    "asc",
            "customer": customer,
        }

        if statuses:
            params["status"] = ",".join(statuses)

        state_key = f"credentials_{customer}"
        if not full_sync:
            sync_filter = self.state_manager.get_sync_filter(state_key, "created")
            params.update(sync_filter)

        logger.info("Consultando Axur credentials para %s con params: %s", customer, params)
        raw_creds = self.api_client.get_paginated(
            endpoint="exposure-api/credentials",
            params=params,
            page_size=1000,
            max_pages=max_pages,
        )

        if not raw_creds:
            logger.info("No hay nuevas credentials para %s", customer)
            return {"fetched": 0, "transformed": 0, "sent": 0, "failed": 0}

        logger.info("Obtenidas %d credentials de %s", len(raw_creds), customer)

        log_entries = self.transformer.transform_batch(raw_creds, data_type="credential")

        result = self.secops_credentials.ingest_batch(
            log_entries=log_entries,
            log_type=LOG_TYPE_CREDENTIALS,
            batch_size=batch_size,
        )

        if result["sent"] > 0:
            self.state_manager.update_sync_time(
                state_key,
                datetime.now(timezone.utc).isoformat()
            )

        return {
            "fetched":     len(raw_creds),
            "transformed": len(log_entries),
            "sent":        result["sent"],
            "failed":      result["failed"],
        }

    # ─── Sync completo ────────────────────────────────────────────────────────

    def sync_all(
        self,
        full_sync:  bool               = False,
        max_pages:  Optional[int]       = None,
        customers:  Optional[List[str]] = None,
        batch_size: int                 = 500,
    ) -> Dict[str, Dict[str, int]]:
        """
        Ejecuta la sincronización completa de tickets y credentials.

        Returns:
            {"tickets": {...}, "credentials": {...}}
        """
        logger.info("=== Iniciando sincronización completa ===")
        results: Dict[str, Any] = {}

        try:
            results["tickets"] = self.sync_tickets(
                full_sync=full_sync,
                max_pages=max_pages,
                customers=customers,
                batch_size=batch_size,
            )
        except Exception as exc:
            logger.error("Error en sync_tickets: %s", exc)
            results["tickets"] = {"error": str(exc)}

        try:
            results["credentials"] = self.sync_credentials(
                full_sync=full_sync,
                max_pages=max_pages,
                customers=customers,
                batch_size=batch_size,
            )
        except Exception as exc:
            logger.error("Error en sync_credentials: %s", exc)
            results["credentials"] = {"error": str(exc)}

        logger.info("=== Sincronización completa finalizada: %s ===", results)
        return results

    # ─── Estado y diagnóstico ─────────────────────────────────────────────────

    def get_sync_status(self) -> Dict[str, Any]:
        """
        Devuelve el estado actual de sincronización.

        Returns:
            Diccionario con timestamps de último sync por endpoint/customer.
        """
        customers = self.config.get("customers", [])
        status: Dict[str, Any] = {"last_sync": {}}

        if customers:
            for customer in customers:
                status["last_sync"][customer] = {
                    "tickets":     self.state_manager.get_last_sync_time("tickets"),
                    "credentials": self.state_manager.get_last_sync_time(
                        f"credentials_{customer}"
                    ),
                }
        else:
            status["last_sync"] = {
                "tickets":     self.state_manager.get_last_sync_time("tickets"),
                "credentials": self.state_manager.get_last_sync_time("credentials"),
            }

        status["secops_config"] = {
            "project":   self.config.get("chronicle_project_id"),
            "region":    self.config.get("chronicle_region"),
            "instance":  self.config.get("chronicle_instance_id"),
            "auth_mode": self.config.get("chronicle_auth_mode", AuthMode.SA_FILE),
            "log_types": {
                "tickets":     LOG_TYPE_TICKETS,
                "credentials": LOG_TYPE_CREDENTIALS,
            },
        }

        return status

"""
CLI principal para el sistema de sincronización Axur → Google SecOps.
Adaptado desde axur-elastic/src/main.py.
"""

import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.orchestrator import SyncOrchestrator
from src.storage.secops_client import AuthMode

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("sync.log"),
    ],
)
logger = logging.getLogger(__name__)


def load_config() -> dict:
    """
    Carga la configuración desde variables de entorno (.env).

    Modos de autenticación con Google SecOps:
      SA_FILE  : Archivo JSON de Service Account en disco
      SA_ENV   : JSON de SA en variable de entorno CHRONICLE_SA_KEY_JSON
      WEBHOOK  : API Key + Secret del feed HTTPS configurado en SecOps UI

    Variables de entorno soportadas:
      # Axur
      AXUR_API_KEY          API Key de Axur
      AXUR_CUSTOMERS        CustomerKeys separados por coma (ej: DCMR,CLIENT2)
      TIMEZONE              Offset UTC (default Z)
      API_TIMEOUT           Segundos (default 30)
      MASK_PASSWORDS        true/false (default true)

      # SecOps — Comunes
      CHRONICLE_PROJECT_ID  GCP Project ID del tenant
      CHRONICLE_REGION      Región del tenant (ej: us, europe, asia-southeast1)
      CHRONICLE_INSTANCE_ID UUID del tenant (SIEM Settings → Profile)
      CHRONICLE_AUTH_MODE   sa_file | sa_env | webhook (default sa_file)

      # SecOps — SA auth (sa_file o sa_env)
      CHRONICLE_SA_KEY_FILE Ruta al JSON de la SA (modo sa_file)
      CHRONICLE_SA_KEY_JSON JSON de la SA o base64 del JSON (modo sa_env)
      CHRONICLE_FORWARDER_ID UUID del forwarder lógico (requerido para SA auth)

      # SecOps — Webhook auth (cada feed tiene credenciales propias)
      CHRONICLE_TICKETS_FEED_ID        Feed ID del feed HTTPS de tickets en SecOps UI
      CHRONICLE_TICKETS_API_KEY        X-goog-api-key del feed de tickets
      CHRONICLE_TICKETS_WEBHOOK_SECRET X-Webhook-Access-Key del feed de tickets
      CHRONICLE_CREDS_FEED_ID          Feed ID del feed HTTPS de credentials en SecOps UI
      CHRONICLE_CREDS_API_KEY          X-goog-api-key del feed de credentials
      CHRONICLE_CREDS_WEBHOOK_SECRET   X-Webhook-Access-Key del feed de credentials

      # Estado
      STATE_FILE            Ruta al JSON de estado (default sync_state.json)
    """
    config = {
        # Axur
        "axur_api_key":  os.getenv("AXUR_API_KEY"),
        "customers": [
            c.strip()
            for c in os.getenv("AXUR_CUSTOMERS", "").split(",")
            if c.strip()
        ],
        "timezone":       os.getenv("TIMEZONE", "Z"),
        "timeout":        int(os.getenv("API_TIMEOUT", "30")),
        "mask_passwords": os.getenv("MASK_PASSWORDS", "true").lower() == "true",

        # SecOps — comunes
        "chronicle_project_id":  os.getenv("CHRONICLE_PROJECT_ID"),
        "chronicle_region":      os.getenv("CHRONICLE_REGION", "us"),
        "chronicle_instance_id": os.getenv("CHRONICLE_INSTANCE_ID"),
        "chronicle_auth_mode":   os.getenv("CHRONICLE_AUTH_MODE", AuthMode.SA_FILE),

        # SecOps — SA auth
        "chronicle_sa_key_file":  os.getenv("CHRONICLE_SA_KEY_FILE", ""),
        "chronicle_forwarder_id": os.getenv("CHRONICLE_FORWARDER_ID", ""),

        # SecOps — Webhook auth (credenciales independientes por feed)
        "chronicle_tickets_feed_id":        os.getenv("CHRONICLE_TICKETS_FEED_ID", ""),
        "chronicle_tickets_api_key":        os.getenv("CHRONICLE_TICKETS_API_KEY", ""),
        "chronicle_tickets_webhook_secret": os.getenv("CHRONICLE_TICKETS_WEBHOOK_SECRET", ""),
        "chronicle_creds_feed_id":          os.getenv("CHRONICLE_CREDS_FEED_ID", ""),
        "chronicle_creds_api_key":          os.getenv("CHRONICLE_CREDS_API_KEY", ""),
        "chronicle_creds_webhook_secret":   os.getenv("CHRONICLE_CREDS_WEBHOOK_SECRET", ""),

        # Estado
        "state_file": os.getenv("STATE_FILE", "sync_state.json"),
    }

    # Validaciones mínimas
    if not config["axur_api_key"]:
        raise ValueError("AXUR_API_KEY es requerida")
    if not config["chronicle_project_id"]:
        raise ValueError("CHRONICLE_PROJECT_ID es requerida")
    if not config["chronicle_instance_id"]:
        raise ValueError("CHRONICLE_INSTANCE_ID es requerida")

    auth_mode = config["chronicle_auth_mode"]
    if auth_mode in (AuthMode.SA_FILE, AuthMode.SA_ENV):
        if not config["chronicle_forwarder_id"]:
            raise ValueError(
                "CHRONICLE_FORWARDER_ID es requerido para auth mode sa_file / sa_env"
            )
        if auth_mode == AuthMode.SA_FILE and not config["chronicle_sa_key_file"]:
            raise ValueError(
                "CHRONICLE_SA_KEY_FILE es requerido para auth mode sa_file"
            )
    elif auth_mode == AuthMode.WEBHOOK:
        # Cada feed tiene sus propias credenciales — validar los 6 campos
        missing = [
            env_name for env_name, cfg_key in [
                ("CHRONICLE_TICKETS_FEED_ID",        "chronicle_tickets_feed_id"),
                ("CHRONICLE_TICKETS_API_KEY",         "chronicle_tickets_api_key"),
                ("CHRONICLE_TICKETS_WEBHOOK_SECRET",  "chronicle_tickets_webhook_secret"),
                ("CHRONICLE_CREDS_FEED_ID",           "chronicle_creds_feed_id"),
                ("CHRONICLE_CREDS_API_KEY",           "chronicle_creds_api_key"),
                ("CHRONICLE_CREDS_WEBHOOK_SECRET",    "chronicle_creds_webhook_secret"),
            ]
            if not config.get(cfg_key)
        ]
        if missing:
            raise ValueError(
                f"Modo webhook requiere estas variables de entorno: {', '.join(missing)}"
            )

    return config


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Sistema de sincronización Axur → Google SecOps"
    )
    parser.add_argument(
        "--mode",
        choices=["tickets", "credentials", "all"],
        default="all",
        help="Tipo de sincronización a realizar (default: all)",
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Sincronización completa (ignora el estado incremental)",
    )
    parser.add_argument(
        "--ticket-types",
        nargs="+",
        help="Tipos de ticket a filtrar (ej: phishing paid-search dark-web)",
    )
    parser.add_argument(
        "--credential-statuses",
        nargs="+",
        choices=["NEW", "IN_TREATMENT", "SOLVED", "DISCARDED"],
        help="Estados de credentials a sincronizar",
    )
    parser.add_argument(
        "--customers",
        nargs="+",
        help="Clientes específicos (ej: DCMR CLIENT2). Default: usa AXUR_CUSTOMERS del .env",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        help="Límite de páginas a traer por endpoint (útil para pruebas)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Logs por request a SecOps (max 1000, default 500)",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Mostrar estado de sincronización y salir",
    )
    parser.add_argument(
        "--health-check",
        action="store_true",
        help="Verificar conectividad con SecOps y salir",
    )

    args = parser.parse_args()

    try:
        config = load_config()
        orchestrator = SyncOrchestrator(config)

        # ── Mostrar estado
        if args.status:
            status = orchestrator.get_sync_status()
            print("\n=== Estado de Sincronización ===")
            print(json.dumps(status, indent=2, default=str))
            return

        # ── Health check
        if args.health_check:
            from src.storage.secops_client import LOG_TYPE_TICKETS, LOG_TYPE_CREDENTIALS
            print("\n=== Health Check — Google SecOps ===")
            ok_t = orchestrator.secops_tickets.health_check(LOG_TYPE_TICKETS)
            ok_c = orchestrator.secops_credentials.health_check(LOG_TYPE_CREDENTIALS)
            print(f"  Tickets  ({LOG_TYPE_TICKETS}): {'✓ OK' if ok_t else '✗ FAIL'}")
            print(f"  Credentials ({LOG_TYPE_CREDENTIALS}): {'✓ OK' if ok_c else '✗ FAIL'}")
            sys.exit(0 if (ok_t and ok_c) else 1)

        # ── Sincronización
        common = dict(
            full_sync=args.full_sync,
            max_pages=args.max_pages,
            customers=args.customers,
            batch_size=args.batch_size,
        )

        if args.mode == "tickets":
            result = orchestrator.sync_tickets(
                ticket_types=args.ticket_types,
                **common,
            )
            print(f"\n✅ Tickets: {result}")

        elif args.mode == "credentials":
            result = orchestrator.sync_credentials(
                statuses=args.credential_statuses,
                **common,
            )
            print(f"\n✅ Credentials: {result}")

        else:  # all
            results = orchestrator.sync_all(**common)
            print(f"\n✅ Sincronización completa:")
            print(f"   Tickets:     {results.get('tickets', {})}")
            print(f"   Credentials: {results.get('credentials', {})}")

        logger.info("Sincronización finalizada exitosamente")

    except Exception as exc:
        logger.error("Error fatal: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

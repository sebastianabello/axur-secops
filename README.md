# axur-secops

Sistema de sincronización incremental de **Axur Digital Risk Protection** hacia **Google SecOps (Chronicle)**.

Basado en `axur-elastic` — reutiliza el cliente de Axur, el gestor de tokens y el gestor de estado sin modificaciones. Reemplaza Elasticsearch por la Chronicle Ingestion API v1alpha.

---

## Estructura del proyecto

```
axur-secops/
├── src/
│   ├── api/
│   │   └── axur_client.py          ← REUTILIZADO sin cambios de axur-elastic
│   ├── auth/
│   │   └── token_manager.py        ← REUTILIZADO sin cambios de axur-elastic
│   ├── sync/
│   │   └── state_manager.py        ← REUTILIZADO sin cambios de axur-elastic
│   ├── transform/
│   │   └── data_transformer.py     ← REESCRITO para SecOps (sin UDM, eso es el parser CBN)
│   ├── storage/
│   │   └── secops_client.py        ← NUEVO: cliente Chronicle Ingestion API v1alpha
│   ├── orchestrator.py             ← ADAPTADO: ES → SecOps, dos log types
│   └── main.py                     ← ADAPTADO: CLI con los mismos args + nuevos para SecOps
├── .env.example
├── requirements.txt
├── Dockerfile
└── docker-compose.yml
```

---

## Por qué dos Log Types en SecOps

| Log Type | Endpoint Axur | Estructura |
|---|---|---|
| `AXUR_DRP_TICKETS_CUSTOM` | `tickets-api/tickets` | Nested: `{ticket, detection, snapshots, attachments}` |
| `AXUR_DRP_CREDENTIALS_CUSTOM` | `exposure-api/credentials` | Plano: `{id, user, password, status, ...}` |

Las estructuras son completamente distintas → dos parsers CBN separados → dos log types.

---

## Modos de autenticación con Google SecOps

### Opción 1: Service Account desde archivo (`sa_file`)
**Recomendado para producción con Secret Manager o volúmenes montados.**

```bash
CHRONICLE_AUTH_MODE=sa_file
CHRONICLE_SA_KEY_FILE=/secrets/chronicle-sa-key.json
CHRONICLE_FORWARDER_ID=uuid-del-forwarder
```

Pre-requisitos:
1. Crear SA en GCP: `gcloud iam service-accounts create axur-secops-ingest`
2. Asignar rol: `gcloud projects add-iam-policy-binding PROJECT_ID --member=serviceAccount:... --role=roles/chronicle.editor`
3. Crear JSON key: `gcloud iam service-accounts keys create chronicle-sa-key.json --iam-account=...`
4. Habilitar API: `gcloud services enable chronicle.googleapis.com`
5. Crear Forwarder lógico (solo una vez):
   ```bash
   curl -X POST "https://us-chronicle.googleapis.com/v1alpha/projects/PROJECT/locations/us/instances/INSTANCE/forwarders" \
     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     -H "Content-Type: application/json" \
     -d '{"displayName": "axur-secops"}'
   ```

### Opción 2: Service Account desde env var (`sa_env`)
**Ideal para Cloud Run donde no se montan archivos.**

```bash
CHRONICLE_AUTH_MODE=sa_env
# Codificar el JSON en base64:
CHRONICLE_SA_KEY_JSON=$(base64 -w0 chronicle-sa-key.json)
CHRONICLE_FORWARDER_ID=uuid-del-forwarder
```

### Opción 3: Webhook Feed (`webhook`)
**Para usar los feeds HTTPS de SecOps sin SA. Más simple pero menos control.**

Pre-requisitos:
1. En SecOps UI → Settings → Feeds → Add Feed → HTTPS Webhook
2. Crear Feed 1: Log Type = `AXUR_DRP_TICKETS_CUSTOM`
3. Crear Feed 2: Log Type = `AXUR_DRP_CREDENTIALS_CUSTOM`
4. Copiar el Feed ID, API Key y Webhook Secret de cada feed.

```bash
CHRONICLE_AUTH_MODE=webhook
CHRONICLE_TICKETS_FEED_ID=uuid-feed-tickets
CHRONICLE_CREDS_FEED_ID=uuid-feed-credentials
CHRONICLE_API_KEY=AIza...
CHRONICLE_WEBHOOK_SECRET=abc123...
```

---

## Instalación y uso

### Local (desarrollo)

```bash
# 1. Clonar y configurar
cp .env.example .env
# Editar .env con tus valores

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Verificar conectividad
python -m src.main --health-check

# 4. Sincronización de prueba (2 páginas)
python -m src.main --mode tickets --max-pages 2

# 5. Sincronización completa
python -m src.main --mode all

# 6. Ver estado
python -m src.main --status
```

### Docker

```bash
# Build
docker build -t axur-secops .

# Ejecutar (modo all, variables desde .env)
docker compose up axur-secops

# Prueba solo tickets
docker compose run --rm axur-secops --mode tickets --max-pages 1

# Full sync (ignorar estado incremental)
docker compose run --rm axur-secops --mode all --full-sync
```

### Cloud Run Job (producción)

```bash
# Build y push
gcloud builds submit --tag gcr.io/PROJECT_ID/axur-secops

# Crear el job
gcloud run jobs create axur-secops \
  --image gcr.io/PROJECT_ID/axur-secops \
  --region us-central1 \
  --service-account axur-secops-ingest@PROJECT_ID.iam.gserviceaccount.com \
  --set-env-vars "CHRONICLE_AUTH_MODE=sa_env" \
  --set-secrets "CHRONICLE_SA_KEY_JSON=chronicle-sa-json:latest,AXUR_API_KEY=axur-api-key:latest" \
  --args="--mode,all"

# Programar ejecución cada 5 minutos con Cloud Scheduler
gcloud scheduler jobs create http axur-secops-schedule \
  --schedule="*/5 * * * *" \
  --uri="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/PROJECT_ID/jobs/axur-secops:run" \
  --oauth-service-account-email=axur-secops-ingest@PROJECT_ID.iam.gserviceaccount.com
```

---

## Referencia de argumentos CLI

```
python -m src.main [opciones]

--mode              tickets | credentials | all  (default: all)
--full-sync         Sincronización completa (ignora estado incremental)
--ticket-types      Tipos de ticket a filtrar (ej: phishing paid-search)
--credential-statuses  Estados de credentials (NEW IN_TREATMENT SOLVED DISCARDED)
--customers         CustomerKeys específicos (ej: DCMR CLIENT2)
--max-pages         Límite de páginas de Axur (para pruebas)
--batch-size        Logs por request a SecOps (default: 500, max: 1000)
--status            Mostrar estado de sincronización y salir
--health-check      Verificar conectividad con SecOps y salir
```

---

## Parser CBN en Google SecOps

Los log types deben existir en SecOps antes de ingestar. Crearlos:
1. SIEM Settings → Available Log Types → Create a custom log type
2. Nombre: `AXUR_DRP_TICKETS` → SecOps agrega `_CUSTOM` automáticamente
3. Repetir para `AXUR_DRP_CREDENTIALS`

Los parsers CBN (`AXUR_DRP_TICKETS_CUSTOM.conf` y `AXUR_DRP_CREDENTIALS_CUSTOM.conf`)
se suben via:
```bash
secops parser create --log-type AXUR_DRP_TICKETS_CUSTOM --parser-code-file tickets.conf
secops parser activate --log-type AXUR_DRP_TICKETS_CUSTOM
```

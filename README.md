# axur-secops

Sistema de sincronización incremental de **Axur Digital Risk Protection** hacia **Google SecOps (Chronicle)**.

Basado en `axur-elastic` — reutiliza el cliente de Axur, el gestor de tokens y el gestor de estado sin modificaciones. Reemplaza Elasticsearch por feeds Webhook de Chronicle.

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
│   │   └── secops_client.py        ← NUEVO: cliente Chronicle Webhook Feed + Ingestion API
│   ├── orchestrator.py             ← ADAPTADO: ES → SecOps, dos log types, dos feeds
│   └── main.py                     ← ADAPTADO: CLI con los mismos args + nuevos para SecOps
├── .env.example
├── requirements.txt
└── README.md
```

---

## Por qué dos Log Types en SecOps

| Log Type | Endpoint Axur | Estructura |
|---|---|---|
| `AXUR_DRP_TICKETS_CUSTOM` | `tickets-api/tickets` | Nested: `{ticket, detection, snapshots, attachments}` |
| `AXUR_DRP_CREDENTIALS_CUSTOM` | `exposure-api/credentials` | Plano: `{id, user, password, status, ...}` |

Las estructuras son completamente distintas → dos parsers CBN separados → dos log types → **dos feeds independientes en SecOps UI**, cada uno con su propio `feed_id`, `api_key` y `webhook_secret`.

---

## Modos de autenticación con Google SecOps

### Modo principal: Webhook Feed (`webhook`) ← Recomendado

Cada evento se envía como un **POST individual** al endpoint del feed con el JSON plano en el body. Sin base64, sin wrapper. El log type está configurado en el feed de SecOps UI, no en el código.

**Endpoint:**
```
https://{region}-chronicle.googleapis.com/v1alpha/projects/{project}/locations/{region}/instances/{instance}/feeds/{feed_id}:importPushLogs
```

**Headers por request:**
```
Content-Type: application/json
X-goog-api-key: <api_key del feed>
X-Webhook-Access-Key: <webhook_secret del feed>
```

**Pre-requisitos:**
1. En SecOps UI → Settings → Feeds → Add Feed → HTTPS Webhook
2. Crear **Feed 1**: Log Type = `AXUR_DRP_TICKETS_CUSTOM` → copiar Feed ID, API Key y Webhook Secret
3. Crear **Feed 2**: Log Type = `AXUR_DRP_CREDENTIALS_CUSTOM` → copiar Feed ID, API Key y Webhook Secret
4. Cada feed genera su propio par único de credenciales — **no reutilizar entre feeds**

**Variables de entorno:**
```bash
CHRONICLE_AUTH_MODE=webhook
CHRONICLE_PROJECT_ID=my-gcp-project
CHRONICLE_REGION=us
CHRONICLE_INSTANCE_ID=00000000-0000-0000-0000-000000000000

# Feed de tickets (credenciales propias)
CHRONICLE_TICKETS_FEED_ID=00000000-0000-0000-0000-000000000000
CHRONICLE_TICKETS_API_KEY=AIzaSy...
CHRONICLE_TICKETS_WEBHOOK_SECRET=abc123...

# Feed de credentials (credenciales propias, distintas a las de tickets)
CHRONICLE_CREDS_FEED_ID=00000000-0000-0000-0000-111111111111
CHRONICLE_CREDS_API_KEY=AIzaSy...
CHRONICLE_CREDS_WEBHOOK_SECRET=xyz789...
```

---

### Alternativa: Service Account desde archivo (`sa_file`)

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

### Alternativa: Service Account desde env var (`sa_env`)

```bash
CHRONICLE_AUTH_MODE=sa_env
CHRONICLE_SA_KEY_JSON=$(base64 -w0 chronicle-sa-key.json)
CHRONICLE_FORWARDER_ID=uuid-del-forwarder
```

---

## Instalación y uso

### Local (desarrollo)

```bash
# 0. Creacion de entorno
python3 -m venv .venv
# activarla
source .venv/bin/activate

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

---

## Automatización con Cron

Para sincronización periódica, agregar a crontab:
dar permisos de ejecucion al archivo **sync.sh**

chmod +x sync.sh

```bash
# Sincronizar cada hora
0 * * * * cd /home/**/**/axur-secops && ./sync.sh --mode all >> sync.log 2>&1

# Sincronizar cada 15 minutos
*/15 * * * * cd /home/**/**/axur-secops && ./sync.sh --mode all >> sync.log 2>&1
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

Los log types deben existir en SecOps antes de ingestar:
1. SIEM Settings → Available Log Types → Create a custom log type
2. Nombre: `AXUR_DRP_TICKETS` → SecOps agrega `_CUSTOM` automáticamente
3. Repetir para `AXUR_DRP_CREDENTIALS`

Los parsers CBN (`AXUR_DRP_TICKETS_CUSTOM.conf` y `AXUR_DRP_CREDENTIALS_CUSTOM.conf`)
se suben via:
```bash
secops parser create --log-type AXUR_DRP_TICKETS_CUSTOM --parser-code-file tickets.conf
secops parser activate --log-type AXUR_DRP_TICKETS_CUSTOM
```

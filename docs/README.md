# KafkaMed — Plataforma de monitoreo cardíaco en streaming

**Pipeline:** Apache Kafka → PySpark Structured Streaming → MongoDB → Flask API → Power BI

| Campo | Detalle |
|---|---|
| Curso | Big Data — Institución Universitaria de Envigado |
| Periodo | 2026-1 |
| Actividad | 2 — Proyecto integrador (35% nota final) |
| Dataset | [Heart Failure Prediction — Kaggle](https://www.kaggle.com/datasets/fedesoriano/heart-failure-prediction) |

---

## 1. Arquitectura del sistema

```
heart.csv
    │
    ▼
[producer.py] ──── Kafka topic: heart-records ────► [consumer_spark.py]
    │                   (apache/kafka KRaft)              │
    │                                                      ▼
    │                                               [Modelo ML - RandomForest]
    │                                                      │
    │                                                      ▼
    │                                              [MongoDB: kafkamed.predictions]
    │                                                      │
    │                                          ┌───────────┴──────────────┐
    │                                          ▼                          ▼
    │                                    [Flask API :5000]          [Power BI]
    │                                    /patients
    │                                    /predictions
    │                                    /stats
    │                                    /risk-summary
    └──────────────────────────────────────────────────────────────────────┘
```

---

## 2. Estructura del repositorio

```
KafkaMed/
├── producer/
│   ├── producer.py             # Productor Kafka
│   ├── requirements.txt        # kafka-python
│   └── Dockerfile
├── spark/
│   ├── consumer_spark.py       # Consumidor Structured Streaming + ML
│   ├── requirements.txt        # pyspark, pymongo
│   └── Dockerfile
├── api/
│   ├── app.py                  # API REST Flask (4 endpoints)
│   ├── requirements.txt        # flask, pymongo
│   └── Dockerfile
├── data/
│   └── heart.csv               # ← debes colocar aquí el dataset de Kaggle
├── docker-compose.yml
└── README.md
```

---

## 3. Descarga del dataset (paso obligatorio antes de correr el proyecto)

El dataset **no se incluye en el repositorio** porque está bajo licencia Kaggle. Sigue uno de estos métodos:

### Método A — Kaggle CLI (recomendado)

```bash
# 1. Instala la CLI de Kaggle si no la tienes
pip install kaggle

# 2. Descarga tu archivo kaggle.json desde:
#    https://www.kaggle.com/settings  →  "Create New Token"
#    Ubícalo en ~/.kaggle/kaggle.json

# 3. Descarga y descomprime el dataset
kaggle datasets download -d fedesoriano/heart-failure-prediction
unzip heart-failure-prediction.zip -d data/
# El archivo resultante se llama heart.csv — ya está en data/
```

### Método B — Descarga manual

1. Ve a: `https://www.kaggle.com/datasets/fedesoriano/heart-failure-prediction`
2. Haz clic en **Download** (requiere cuenta Kaggle gratuita).
3. Descomprime el ZIP y copia `heart.csv` dentro de la carpeta `data/` del proyecto.

### Verificación

```bash
head -3 data/heart.csv
# Debe mostrar:
# Age,Sex,ChestPainType,RestingBP,Cholesterol,FastingBS,RestingECG,MaxHR,ExerciseAngina,Oldpeak,ST_Slope,HeartDisease
# 40,M,ATA,140,289,0,Normal,172,N,0,Up,0
# 49,F,NAP,160,180,0,Normal,156,N,1,Flat,1
```

---

## 4. Dockerfiles necesarios

Debes crear un `Dockerfile` en cada subcarpeta. Aquí las plantillas:

### producer/Dockerfile

```dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer.py .
CMD ["python", "producer.py"]
```

### producer/requirements.txt

```
kafka-python==2.0.2
```

### spark/Dockerfile

```dockerfile
FROM apache/spark:3.5.0
USER root
RUN pip install pymongo==4.8.0 numpy
COPY consumer_spark.py /opt/spark/work-dir/
WORKDIR /opt/spark/work-dir
```

### spark/requirements.txt

```
pymongo==4.8.0
numpy
```

### api/Dockerfile

```dockerfile
FROM python:3.10-slim
WORKDIR /opt/api
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
CMD ["python", "app.py"]
```

### api/requirements.txt

```
flask==3.0.3
pymongo==4.8.0
```

---

## 5. Levantar el sistema

```bash
# Clonar el repositorio y posicionarse en la raíz
cd KafkaMed

# Verificar que heart.csv está en data/
ls data/heart.csv

# Primera ejecución (construye imágenes y levanta todos los servicios)
docker compose up --build

docker compose -f infra/docker-compose.yml logs --no-color --tail=200 producer
# Ejecuciones posteriores
docker compose up
docker compose -f infra/docker-compose.yml logs --no-color --tail=200 producer
```

El sistema arranca en este orden gracias a los `healthcheck`:
1. **Kafka** se inicializa en modo KRaft (~30 s).
2. **MongoDB** queda listo.
3. **Producer** empieza a publicar registros cada 1 segundo.
4. **Spark** lee del topic, aplica el modelo y graba en MongoDB.
5. **API Flask** queda disponible en `http://localhost:5000`.

---

## 6. Endpoints de la API

### GET `/patients` — Listado de pacientes procesados

```bash
curl http://localhost:5000/patients
curl "http://localhost:5000/patients?riesgo=alto&sex=M&limit=10"
```

**Respuesta:**
```json
{
  "total": 10,
  "pacientes": [
    {
      "_id": "...",
      "age": 63, "sex": "M", "chest_pain_type": "ASY",
      "resting_bp": 145, "cholesterol": 233,
      "prediction_label": "riesgo",
      "probability": 0.91,
      "timestamp": "2026-05-18T14:32:01.123456"
    }
  ]
}
```

### GET `/predictions` — Solo campos de predicción

```bash
curl http://localhost:5000/predictions
curl "http://localhost:5000/predictions?riesgo=alto&limit=50"
```

### GET `/stats` — Métricas globales

```bash
curl http://localhost:5000/stats
```

**Respuesta:**
```json
{
  "total_pacientes": 918,
  "distribucion_riesgo": { "riesgo": 508, "sin_riesgo": 410 },
  "probabilidad_promedio": 0.7341,
  "distribucion_por_sexo": {
    "M": { "riesgo": 437, "sin_riesgo": 189 },
    "F": { "riesgo": 71, "sin_riesgo": 221 }
  }
}
```

### GET `/risk-summary` — Resumen ejecutivo para el equipo médico

```bash
curl http://localhost:5000/risk-summary
curl "http://localhost:5000/risk-summary?limit=5"
```

**Respuesta:**
```json
{
  "alertas_activas": 20,
  "promedios_clinicos": {
    "age": 58.3, "resting_bp": 141.2,
    "cholesterol": 221.7, "max_hr": 126.4, "oldpeak": 1.8
  },
  "ultimo_procesado": "2026-05-18T14:35:22.001234",
  "pacientes_alto_riesgo": [ ... ]
}
```

---

## 7. Dashboard Power BI (4 visualizaciones requeridas)

Conecta Power BI a la API Flask usando el conector **Web** (`http://localhost:5000/stats`, etc.) o directamente a MongoDB vía ODBC.

| # | Visualización | Fuente | Tipo de gráfico |
|---|---|---|---|
| 1 | Distribución de riesgo | `/stats` → `distribucion_riesgo` | Gráfico de dona |
| 2 | Evolución temporal de alertas | `/predictions` → `timestamp` + `prediction_label` | Gráfico de línea |
| 3 | Variables correlacionadas (edad, colesterol, FC máx por grupo de riesgo) | `/patients` | Gráfico de dispersión / matriz |
| 4 | Tabla de alertas activas (alto riesgo) | `/risk-summary` → `pacientes_alto_riesgo` | Tabla con formato condicional |

**Paso a paso para conectar Power BI:**
1. Abre Power BI Desktop → **Obtener datos** → **Web**.
2. URL: `http://<TU_IP_LOCAL>:5000/patients?riesgo=alto&limit=500`
3. Expande la columna `pacientes` → **A la tabla** → normaliza columnas.
4. Repite para `/stats` y `/predictions`.
5. Crea relaciones entre tablas usando `_id` o `timestamp`.

---

## 8. Comandos útiles

```bash
# Ver logs en tiempo real por servicio
docker compose logs -f producer
docker compose logs -f spark
docker compose logs -f api

# Verificar que Kafka recibe mensajes
docker exec -it kafkamed_kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic heart-records \
  --from-beginning \
  --max-messages 5

# Consultar MongoDB directamente
docker exec -it kafkamed_mongo mongosh
use kafkamed
db.predictions.countDocuments()
db.predictions.find({ prediction_label: "riesgo" }).limit(3).pretty()

# Detener y limpiar (conserva datos)
docker compose down

# Limpiar completamente (borra datos de Kafka y MongoDB)
docker compose down -v
```

---

## 9. Diferencias clave vs Actividad 1 (SentimentStream → KafkaMed)

| Componente | Actividad 1 (SentimentStream) | Actividad 2 (KafkaMed) |
|---|---|---|
| Fuente de datos | Socket TCP simple | **Broker Kafka** (topic `heart-records`) |
| Dominio | Análisis de sentimientos (texto) | **Detección de riesgo cardíaco** (datos tabulares) |
| Modelo ML | Naive Bayes + TF-IDF | **RandomForest / LogisticRegression** |
| Colección MongoDB | `sentiments` | `predictions` |
| Endpoints Flask | `/sentiments`, `/stats`, `/predict` | `/patients`, `/predictions`, `/stats`, `/risk-summary` |
| Broker | Socket (frágil, 1:1) | **Kafka KRaft** (escalable, persistente, tolerante a fallos) |
| Garantías de entrega | Ninguna | **at-least-once** (acks=all + checkpoint) |
| Escalabilidad | Un productor, un consumidor | **N productores, M consumidores** en paralelo |

**Por qué Kafka es mejor que un socket simple:**
- **Persistencia:** los mensajes sobreviven si el consumidor cae; el socket los pierde.
- **Desacoplamiento:** el productor no necesita saber si el consumidor está activo.
- **Escalabilidad horizontal:** múltiples particiones permiten consumidores paralelos.
- **Tolerancia a fallos:** el checkpoint de Spark + el log de Kafka garantizan exactitud.

---

## 10. Variables de entorno de referencia

| Variable | Valor por defecto | Descripción |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `kafka:29092` | Broker Kafka (listener interno Docker) |
| `KAFKA_TOPIC` | `heart-records` | Topic de registros cardíacos |
| `CSV_PATH` | `/app/data/heart.csv` | Ruta del dataset |
| `INTERVAL` | `1.0` | Segundos entre mensajes del productor |
| `MONGO_URI` | `mongodb://mongo:27017` | Conexión MongoDB |
| `MONGO_DB` | `kafkamed` | Base de datos |
| `MONGO_COLLECTION` | `predictions` | Colección de predicciones |
| `MODEL_PATH` | `/app/data/model_rf` | Ruta del modelo Spark ML |
| `CHECKPOINT_DIR` | `/app/data/checkpoint` | Checkpoint de Structured Streaming |

---

## 11. Descarga automática del dataset (opcional)

Si quieres automatizar la descarga de `heart.csv` desde Kaggle, coloca tu `kaggle.json` en `~/.kaggle/kaggle.json` (permiso `600`) y ejecuta el helper:

```bash
# instalar la dependencia de ayuda (solo para el script local)
pip install -r requirements.txt

# descargar y descomprimir en ./data/
python scripts/download_dataset.py
```

El script usa la API oficial `kaggle` y guardará `heart.csv` dentro de la carpeta `data/`.

Si prefieres usar la CLI directamente:

```bash
# descarga directa con la CLI de Kaggle
kaggle datasets download -d fedesoriano/heart-failure-prediction -p data/ --unzip
```
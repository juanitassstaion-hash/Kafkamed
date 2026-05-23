# KafkaMed — Plataforma de monitoreo cardíaco en streaming

**Pipeline:** Apache Kafka → PySpark Structured Streaming → MongoDB → Flask API → Power BI

| Campo | Detalle |
|---|---|
| Curso | Big Data — Institución Universitaria de Envigado |
| Periodo | 2026-1 |
| Actividad | 2 — Proyecto integrador (35% nota final) |
| Dataset | [Heart Failure Prediction — Kaggle](https://www.kaggle.com/datasets/fedesoriano/heart-failure-prediction) |
| Autores | Cristian David Ocampo Uribe | Juanita Solórzano Salazar

---

## ⚡ GUÍA RÁPIDA DE EJECUCIÓN (Paso a Paso)

### Paso 0: Verificar requisitos
```bash
# Verificar que Docker y Docker Compose están instalados
docker --version
docker compose --version
```

### Paso 1: Descargar el Dataset

El dataset **no está incluido** en el repositorio. Debes descargarlo antes de ejecutar. Elige una opción:

#### **Opción A — Descarga manual (RECOMENDADO - más fácil)**
1. Ve a: https://www.kaggle.com/datasets/fedesoriano/heart-failure-prediction
2. Haz clic en **Download** (requiere crear cuenta Kaggle gratuita si no la tienes)
3. Descomprime el ZIP descargado
4. Copia el archivo **`heart.csv`** a la carpeta **`data/`** del proyecto
5. Verifica:
   ```bash
   ls -la data/heart.csv
   ```

#### Opción B — Usando CLI de Kaggle
```bash
# 1. Instalar Kaggle CLI
pip install kaggle

# 2. Generar token: https://www.kaggle.com/settings/account
#    Descargar kaggle.json y copiarlo a ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json

# 3. Descargar dataset
kaggle datasets download -d fedesoriano/heart-failure-prediction -p data/ --unzip

# 4. Verificar
ls data/heart.csv
```

#### Opción C — Script automático
```bash
pip install -r requirements.txt
python scripts/download_dataset.py
```

**✓ Verificación final:**
```bash
head -3 data/heart.csv
# Debe mostrar las columnas: Age,Sex,ChestPainType,RestingBP,...
```

---

### Paso 2: Levantar los servicios (Primera ejecución)

**En la terminal, en la raíz del proyecto:**

```bash
# Navegar al directorio
cd /ruta/al/Kafkamed

# Construir imágenes y levantar todos los servicios
docker compose -f infra/docker-compose.yml up --build
```

**¿Qué sucede automáticamente?**
- ✅ Kafka inicia en modo KRaft (30 segundos aprox)
- ✅ MongoDB se levanta y está listo
- ✅ Producer comienza a publicar registros cada 1 segundo
- ✅ Spark lee del topic, aplica modelo ML y guarda en MongoDB
- ✅ API Flask queda disponible en `http://localhost:5000`

---

### Paso 3: Verificar que todo funciona

**Abre otra terminal y ejecuta:**

```bash
# 1. Ver logs del producer en vivo
docker compose -f infra/docker-compose.yml logs -f producer

# 2. En otra terminal, probar la API (esperar 30 segundos después de iniciar)
curl http://localhost:5000/stats

# 3. Obtener lista de pacientes procesados
curl "http://localhost:5000/patients?limit=5"

# 4. Resumen de alertas
curl http://localhost:5000/risk-summary
```

**Si ves datos en respuesta → ¡Todo funciona! ✓**

---

### Paso 4: (Opcional) Conectar Power BI

Ver sección **8. Dashboard Power BI** más abajo.

---

### Paso 5: Detener los servicios

```bash
# Detener sin perder datos
docker compose -f infra/docker-compose.yml down

# O limpiar completamente (borra datos de Kafka y MongoDB)
docker compose -f infra/docker-compose.yml down -v
```

---

## 🔄 Para ejecuciones posteriores

Una vez verificado que funciona, **no necesitas reconstruir las imágenes:**

```bash
# Solo levantar servicios existentes
docker compose -f infra/docker-compose.yml up

# Ver logs de un servicio específico
docker compose -f infra/docker-compose.yml logs -f spark
docker compose -f infra/docker-compose.yml logs -f api
```

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

**Flujo de datos:**
1. **Producer** lee `heart.csv` y publica registros en Kafka
2. **Spark** consume del topic en tiempo real
3. **ML Model** predice riesgo cardíaco para cada paciente
4. **MongoDB** almacena predicciones con timestamp
5. **API Flask** expone endpoints para consultas
6. **Power BI** visualiza métricas y alertas

---

## 2. Estructura del repositorio

```
Kafkamed/
├── producer/
│   ├── producer.py             # Publica registros en Kafka
│   ├── Dockerfile              # Imagen Docker
│   └── requirements.txt        # kafka-python
│
├── spark/
│   ├── consumer_spark.py       # Consume Kafka + ML + MongoDB
│   ├── train_and_evaluate.py   # Entrena modelo RandomForest
│   ├── Dockerfile              # Imagen Docker
│   └── requirements.txt        # pyspark, pymongo
│
├── api/
│   ├── app.py                  # API REST Flask (4 endpoints)
│   ├── Dockerfile              # Imagen Docker
│   └── requirements.txt        # flask, pymongo
│
├── data/
│   ├── heart.csv               # ← Dataset (debes descargarlo)
│   ├── checkpoint/             # Checkpoints de Spark Streaming
│   ├── model_rf/               # Modelo RandomForest entrenado
│   └── model_nb/               # (Opcional) Naive Bayes
│
├── infra/
│   ├── docker-compose.yml      # Orquestación de servicios
│   └── Jenkinsfile             # CI/CD
│
├── scripts/
│   └── download_dataset.py     # Helper para descargar dataset
│
└── docs/
    ├── README.md               # Este archivo
    └── metrics.json            # Métricas del modelo
```

---

## 3. Variables de entorno de referencia

| Variable | Valor por defecto | Descripción |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `kafka:29092` | Broker Kafka (listener Docker) |
| `KAFKA_TOPIC` | `heart-records` | Topic de registros |
| `CSV_PATH` | `/app/data/heart.csv` | Ruta del dataset |
| `INTERVAL` | `1.0` | Segundos entre mensajes |
| `MONGO_URI` | `mongodb://mongo:27017` | Conexión MongoDB |
| `MONGO_DB` | `kafkamed` | Base de datos |
| `MONGO_COLLECTION` | `predictions` | Colección de predicciones |
| `MODEL_PATH` | `/app/data/model_rf` | Ruta modelo ML |
| `CHECKPOINT_DIR` | `/app/data/checkpoint` | Checkpoint Streaming |

---

## 4. Endpoints de la API

### GET `/patients` — Listado completo de pacientes procesados

```bash
# Todos los pacientes
curl http://localhost:5000/patients

# Filtrar por riesgo y sexo
curl "http://localhost:5000/patients?riesgo=alto&sex=M&limit=10"
```

**Respuesta:**
```json
{
  "total": 10,
  "pacientes": [
    {
      "_id": "507f1f77bcf86cd799439011",
      "age": 63,
      "sex": "M",
      "chest_pain_type": "ASY",
      "resting_bp": 145,
      "cholesterol": 233,
      "prediction_label": "riesgo",
      "probability": 0.91,
      "timestamp": "2026-05-18T14:32:01.123456"
    },
    ...
  ]
}
```

### GET `/predictions` — Solo campos de predicción

```bash
# Todas las predicciones
curl http://localhost:5000/predictions

# Filtrar por riesgo
curl "http://localhost:5000/predictions?riesgo=alto&limit=50"
```

### GET `/stats` — Métricas globales agregadas

```bash
curl http://localhost:5000/stats
```

**Respuesta:**
```json
{
  "total_pacientes": 918,
  "distribucion_riesgo": {
    "riesgo": 508,
    "sin_riesgo": 410
  },
  "probabilidad_promedio": 0.7341,
  "distribucion_por_sexo": {
    "M": {
      "riesgo": 437,
      "sin_riesgo": 189
    },
    "F": {
      "riesgo": 71,
      "sin_riesgo": 221
    }
  }
}
```

### GET `/risk-summary` — Resumen ejecutivo para equipo médico

```bash
# Resumen general
curl http://localhost:5000/risk-summary

# Últimas N alertas
curl "http://localhost:5000/risk-summary?limit=5"
```

**Respuesta:**
```json
{
  "alertas_activas": 20,
  "promedios_clinicos": {
    "age": 58.3,
    "resting_bp": 141.2,
    "cholesterol": 221.7,
    "max_hr": 126.4,
    "oldpeak": 1.8
  },
  "ultimo_procesado": "2026-05-18T14:35:22.001234",
  "pacientes_alto_riesgo": [
    {
      "age": 45,
      "resting_bp": 160,
      "probability": 0.95,
      "timestamp": "2026-05-18T14:35:20.123456"
    },
    ...
  ]
}
```

---

## 5. Dashboard Power BI (4 visualizaciones requeridas)

### Paso a paso para conectar Power BI a la API

1. **Abre Power BI Desktop**
2. **Obtener datos** → **Web**
3. **Ingresa URL:** `http://<TU_IP_LOCAL>:5000/patients?riesgo=alto&limit=500`
4. **Carga los datos**
5. **Expande la columna** `pacientes` → **A la tabla**
6. **Normaliza columnas** según sea necesario

### Las 4 visualizaciones requeridas

| # | Visualización | Fuente | Tipo de gráfico |
|---|---|---|---|
| 1 | Distribución de riesgo | `/stats` → `distribucion_riesgo` | **Gráfico de dona** |
| 2 | Evolución temporal de alertas | `/predictions` → `timestamp` + `prediction_label` | **Línea temporal** |
| 3 | Correlaciones clínicas | `/patients` → edad/colesterol/FC máx vs riesgo | **Dispersión / Matriz** |
| 4 | Tabla de alertas activas | `/risk-summary` → `pacientes_alto_riesgo` | **Tabla con formato condicional** |

**Recomendaciones:**
- Puedes conectar directamente a MongoDB con ODBC para mayor flexibilidad
- Usa filtros dinámicos por sexo, rango de edad, tipo de dolor
- Actualiza los datos cada 5 minutos (opción de Power BI)

---

## 6. Comandos útiles para debugging

### Ver logs en tiempo real
```bash
# Producer (publica mensajes Kafka)
docker compose -f infra/docker-compose.yml logs -f producer

# Spark (procesa y aplica ML)
docker compose -f infra/docker-compose.yml logs -f spark

# API Flask
docker compose -f infra/docker-compose.yml logs -f api

# Base de datos
docker compose -f infra/docker-compose.yml logs -f mongo
```

### Verificar que Kafka recibe mensajes
```bash
docker exec -it Kafkamed-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic heart-records \
  --from-beginning \
  --max-messages 5
```

### Consultar MongoDB directamente
```bash
docker exec -it Kafkamed-mongo-1 mongosh

# Dentro de mongosh:
use kafkamed
db.predictions.countDocuments()
db.predictions.find({ prediction_label: "riesgo" }).limit(3).pretty()
db.predictions.aggregate([
  { $group: { _id: "$prediction_label", count: { $sum: 1 } } }
])
```

### Detener y limpiar
```bash
# Detener sin borrar datos
docker compose -f infra/docker-compose.yml down

# Limpiar completamente (borra bases de datos)
docker compose -f infra/docker-compose.yml down -v

# Forzar detención
docker compose -f infra/docker-compose.yml kill
```

---

## 7. Diferencias clave vs Actividad 1 (SentimentStream → KafkaMed)

| Aspecto | Actividad 1 (SentimentStream) | Actividad 2 (KafkaMed) |
|---|---|---|
| Fuente de datos | Socket TCP simple | **Broker Kafka con persistencia** |
| Dominio | Análisis de sentimientos (texto) | **Diag. cardíaco (datos tabulares)** |
| Modelo ML | Naive Bayes + TF-IDF | **RandomForest / LogisticRegression** |
| Colección | `sentiments` | `predictions` |
| Endpoints | `/sentiments`, `/stats` | `/patients`, `/predictions`, `/stats`, `/risk-summary` |
| Broker | Socket 1:1 (frágil) | **Kafka KRaft (escalable)** |
| Garantías | Ninguna | **at-least-once** |
| Scalabilidad | 1 productor ↔ 1 consumidor | **N productores ↔ M consumidores** |

**¿Por qué Kafka es mejor?**
- ✅ **Persistencia:** mensajes sobreviven si el consumidor cae
- ✅ **Desacoplamiento:** productor y consumidor actúan independientemente
- ✅ **Escalabilidad horizontal:** múltiples particiones para paralelismo
- ✅ **Garantías de entrega:** checkpoint + offset = exactitud

---

## 8. Solución de problemas

### El API no responde
```bash
# Esperar 30-40 segundos después de iniciar (Kafka tarda en arrancar)
# Ver logs de Spark:
docker compose -f infra/docker-compose.yml logs spark | tail -50
```

### No hay datos en MongoDB
```bash
# Verificar que el producer está publicando
docker compose -f infra/docker-compose.yml logs producer | head -20

# Verificar que Spark está leyendo
docker compose -f infra/docker-compose.yml logs spark | grep "Batch"
```

### Error de conexión a Kafka
```bash
# El nombre del contenedor puede variar, revisar:
docker ps

# Ajustar variables de entorno en docker-compose.yml si es necesario
```

### Dataset no encontrado
```bash
# Verificar que el archivo existe
ls -la data/heart.csv

# Si no existe, seguir Paso 1 de la guía rápida
```

---

## 9. Próximos pasos (mejoras futuras)

- [ ] Agregar autenticación a la API (JWT)
- [ ] CI/CD con Jenkins (Jenkinsfile ya existe)
- [ ] Alertas en tiempo real (webhook/email)
- [ ] Dashboard web integrado (vs Power BI externo)
- [ ] Versionado del modelo ML con MLflow
- [ ] Tests automatizados (pytest)
- [ ] Métricas Prometheus para monitoreo

---

## 📞 Contacto y referencias

- **Institución:** Institución Universitaria de Envigado
- **Curso:** Big Data - 2026-1
- **Dataset:** [Kaggle - Heart Failure Prediction](https://www.kaggle.com/datasets/fedesoriano/heart-failure-prediction)
- **Tecnologías:** Kafka, Spark, MongoDB, Flask, Power BI

"""
KafkaMed — API REST (Flask)
---------------------------
Expone los resultados del pipeline de detección de riesgo cardíaco
almacenados en MongoDB. El equipo médico consume estos endpoints
desde Power BI o cualquier cliente HTTP.

Endpoints:
    GET  /patients      -> listado de pacientes procesados (filtros opcionales)
    GET  /predictions   -> predicciones con campos de riesgo y probabilidad
    GET  /stats         -> métricas globales y distribución de riesgo
    GET  /risk-summary  -> resumen ejecutivo: alertas activas y promedios clínicos

Curso: Big Data — Institución Universitaria de Envigado (2026-1)
"""

import os
from datetime import datetime

from flask import Flask, jsonify, request
from pymongo import MongoClient

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
MONGO_URI        = os.environ.get("MONGO_URI",        "mongodb://mongo:27017")
MONGO_DB         = os.environ.get("MONGO_DB",         "kafkamed")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "predictions")

app = Flask(__name__)

_mongo_client = MongoClient(MONGO_URI)
_coleccion    = _mongo_client[MONGO_DB][MONGO_COLLECTION]


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
def _serializar(doc: dict) -> dict:
    """Convierte un documento Mongo a un dict JSON-serializable."""
    doc["_id"] = str(doc["_id"])
    ts = doc.get("timestamp")
    if isinstance(ts, datetime):
        doc["timestamp"] = ts.isoformat()
    return doc


def _filtro_riesgo(request_args) -> dict:
    """Construye el filtro MongoDB a partir de query params comunes."""
    filtro = {}
    riesgo = request_args.get("riesgo")          # "alto" | "bajo"
    sexo   = request_args.get("sex")             # "M" | "F"
    if riesgo == "alto":
        filtro["prediction_label"] = "riesgo"
    elif riesgo == "bajo":
        filtro["prediction_label"] = "sin_riesgo"
    if sexo:
        filtro["sex"] = sexo.upper()
    return filtro


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

# ── 1. /patients ────────────────────────────────────────────────────────────
@app.route("/patients", methods=["GET"])
def listar_pacientes():
    """
    Lista todos los pacientes procesados, ordenados por timestamp desc.

    Query params opcionales:
        riesgo  — 'alto' | 'bajo'
        sex     — 'M' | 'F'
        limit   — número máximo de resultados (default: 100)

    Respuesta:
        {
          "total": <int>,
          "pacientes": [ { campos del paciente + predicción } ]
        }
    """
    filtro = _filtro_riesgo(request.args)
    limit  = min(int(request.args.get("limit", 100)), 500)

    cursor = _coleccion.find(filtro).sort("timestamp", -1).limit(limit)
    pacientes = [_serializar(d) for d in cursor]

    return jsonify({"total": len(pacientes), "pacientes": pacientes})


# ── 2. /predictions ─────────────────────────────────────────────────────────
@app.route("/predictions", methods=["GET"])
def listar_predicciones():
    """
    Devuelve solo los campos de predicción (sin datos clínicos completos).
    Útil para tablas resumen en Power BI.

    Query params opcionales:
        riesgo  — 'alto' | 'bajo'
        limit   — default: 200

    Respuesta:
        {
          "total": <int>,
          "predicciones": [
            {
              "_id": "...",
              "prediction_label": "riesgo" | "sin_riesgo",
              "probability": 0.87,
              "timestamp": "2026-..."
            }
          ]
        }
    """
    filtro = _filtro_riesgo(request.args)
    limit  = min(int(request.args.get("limit", 200)), 1000)

    proyeccion = {
        "prediction_label": 1,
        "probability": 1,
        "timestamp": 1,
        "age": 1,
        "sex": 1,
    }

    cursor = _coleccion.find(filtro, proyeccion).sort("timestamp", -1).limit(limit)
    predicciones = [_serializar(d) for d in cursor]

    return jsonify({"total": len(predicciones), "predicciones": predicciones})


# ── 3. /stats ────────────────────────────────────────────────────────────────
@app.route("/stats", methods=["GET"])
def estadisticas():
    """
    Métricas globales del sistema:
        - Total de pacientes procesados
        - Distribución de riesgo (alto / bajo)
        - Probabilidad promedio de riesgo
        - Desglose por sexo

    Respuesta:
        {
          "total_pacientes": 918,
          "distribucion_riesgo": { "riesgo": 508, "sin_riesgo": 410 },
          "probabilidad_promedio": 0.73,
          "distribucion_por_sexo": { "M": { "riesgo": 450, "sin_riesgo": 180 }, "F": {...} }
        }
    """
    total = _coleccion.count_documents({})

    # Distribución por etiqueta de predicción
    pipeline_dist = [
        {"$group": {"_id": "$prediction_label", "cantidad": {"$sum": 1}}}
    ]
    distribucion = {
        d["_id"]: d["cantidad"]
        for d in _coleccion.aggregate(pipeline_dist)
        if d["_id"] is not None
    }

    # Probabilidad promedio de riesgo
    pipeline_prob = [
        {"$group": {"_id": None, "promedio": {"$avg": "$probability"}}}
    ]
    prob_result = list(_coleccion.aggregate(pipeline_prob))
    prob_promedio = 0.0
    if prob_result and prob_result[0].get("promedio") is not None:
        prob_promedio = round(prob_result[0]["promedio"], 4)

    # Desglose por sexo y etiqueta
    pipeline_sexo = [
        {"$group": {
            "_id": {"sex": "$sex", "label": "$prediction_label"},
            "cantidad": {"$sum": 1}
        }}
    ]
    dist_sexo: dict = {}
    for d in _coleccion.aggregate(pipeline_sexo):
        if not d["_id"].get("sex") or not d["_id"].get("label"):
            continue
        sex   = d["_id"]["sex"]
        label = d["_id"]["label"]
        dist_sexo.setdefault(sex, {})[label] = d["cantidad"]

    return jsonify({
        "total_pacientes":        total,
        "distribucion_riesgo":    distribucion,
        "probabilidad_promedio":  prob_promedio,
        "distribucion_por_sexo":  dist_sexo,
    })


# ── 4. /risk-summary ─────────────────────────────────────────────────────────
@app.route("/risk-summary", methods=["GET"])
def resumen_riesgo():
    """
    Resumen ejecutivo para el equipo médico:
        - Pacientes de alto riesgo activos (últimos N procesados)
        - Promedios clínicos de ese grupo (edad, colesterol, FC máx, oldpeak)
        - Timestamp del último registro procesado

    Query params opcionales:
        limit — cuántos pacientes de alto riesgo incluir (default: 20)

    Respuesta:
        {
          "alertas_activas": 20,
          "promedios_clinicos": { "age": 58.3, "cholesterol": 234.1, ... },
          "ultimo_procesado": "2026-05-18T...",
          "pacientes_alto_riesgo": [ {...}, ... ]
        }
    """
    limit = min(int(request.args.get("limit", 20)), 100)

    filtro_alto = {"prediction_label": "riesgo"}
    cursor = (
        _coleccion.find(filtro_alto)
        .sort("timestamp", -1)
        .limit(limit)
    )
    pacientes_alto = [_serializar(d) for d in cursor]

    # Promedios clínicos del grupo de alto riesgo
    campos_numericos = ["age", "resting_bp", "cholesterol", "max_hr", "oldpeak"]
    promedios: dict = {}
    if pacientes_alto:
        for campo in campos_numericos:
            valores = [p[campo] for p in pacientes_alto if isinstance(p.get(campo), (int, float))]
            promedios[campo] = round(sum(valores) / len(valores), 2) if valores else None

    # Timestamp del registro más reciente (cualquier etiqueta)
    ultimo = _coleccion.find_one({}, sort=[("timestamp", -1)])
    ultimo_ts = None
    if ultimo:
        ts = ultimo.get("timestamp")
        ultimo_ts = ts.isoformat() if isinstance(ts, datetime) else str(ts)

    return jsonify({
        "alertas_activas":       len(pacientes_alto),
        "promedios_clinicos":    promedios,
        "ultimo_procesado":      ultimo_ts,
        "pacientes_alto_riesgo": pacientes_alto,
    })


# ---------------------------------------------------------------------------
# Arranque
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
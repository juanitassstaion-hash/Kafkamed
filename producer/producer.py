"""
KafkaMed — Productor Kafka
--------------------------
Lee el dataset heart_failure_prediction.csv fila a fila y publica cada
registro como mensaje JSON en el topic 'heart-records', simulando el
ingreso de pacientes en tiempo real desde equipos de diagnóstico.

Curso: Big Data — Institución Universitaria de Envigado (2026-1)

Uso:
    python producer.py                        # intervalo por defecto: 1 s
    python producer.py --interval 0.5         # medio segundo por registro
    python producer.py --loop                 # repite el CSV indefinidamente
"""

import argparse
import csv
import json
import os
import time

from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Configuración (sobreescribible con variables de entorno)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC           = os.environ.get("KAFKA_TOPIC",     "heart-records")
CSV_PATH        = os.environ.get("CSV_PATH",        "/app/data/heart.csv")
DEFAULT_INTERVAL = float(os.environ.get("INTERVAL", "1.0"))   # segundos


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
def crear_productor(bootstrap: str, reintentos: int = 10, espera: int = 5) -> KafkaProducer:
    """
    Intenta conectar al broker Kafka. Reintenta hasta `reintentos` veces
    con pausa de `espera` segundos entre intentos (Kafka puede tardar en
    arrancar en Docker).
    """
    for intento in range(1, reintentos + 1):
        try:
            productor = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",           # confirmación de todas las réplicas
                retries=3,
            )
            print(f"[KafkaMed-Producer] Conectado a {bootstrap}")
            return productor
        except Exception as exc:
            print(f"[KafkaMed-Producer] Intento {intento}/{reintentos} fallido: {exc}")
            if intento < reintentos:
                time.sleep(espera)
    raise RuntimeError(f"No se pudo conectar a Kafka en {bootstrap} tras {reintentos} intentos.")


def normalizar_registro(row: dict) -> dict:
    """
    Convierte los valores del CSV a los tipos correctos y estandariza
    los nombres de columnas a snake_case para facilitar el consumo en Spark.

    Columnas del dataset fedesoriano/heart-failure-prediction:
        Age, Sex, ChestPainType, RestingBP, Cholesterol, FastingBS,
        RestingECG, MaxHR, ExerciseAngina, Oldpeak, ST_Slope,
        HeartDisease (target: 0 = sin riesgo, 1 = riesgo)
    """
    try:
        return {
            "age":              int(row.get("Age", 0)),
            "sex":              row.get("Sex", ""),
            "chest_pain_type":  row.get("ChestPainType", ""),
            "resting_bp":       int(row.get("RestingBP", 0)),
            "cholesterol":      int(row.get("Cholesterol", 0)),
            "fasting_bs":       int(row.get("FastingBS", 0)),
            "resting_ecg":      row.get("RestingECG", ""),
            "max_hr":           int(row.get("MaxHR", 0)),
            "exercise_angina":  row.get("ExerciseAngina", ""),
            "oldpeak":          float(row.get("Oldpeak", 0.0)),
            "st_slope":         row.get("ST_Slope", ""),
            "heart_disease":    int(row.get("HeartDisease", -1)),  # etiqueta real (para validación posterior)
        }
    except (ValueError, TypeError) as exc:
        print(f"[KafkaMed-Producer] Error normalizando fila: {exc}  |  raw={row}")
        return {}


# ---------------------------------------------------------------------------
# Publicación
# ---------------------------------------------------------------------------
def publicar(csv_path: str, intervalo: float, loop: bool) -> None:
    productor = crear_productor(KAFKA_BOOTSTRAP)

    iteracion = 0
    continuar = True

    while continuar:
        iteracion += 1
        enviados = 0

        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                registro = normalizar_registro(row)
                if not registro:
                    continue

                future = productor.send(TOPIC, value=registro)
                try:
                    meta = future.get(timeout=10)
                    enviados += 1
                    print(
                        f"[KafkaMed-Producer] [{iteracion}] Paciente #{enviados:04d} → "
                        f"partition={meta.partition} offset={meta.offset} | "
                        f"age={registro['age']} sex={registro['sex']} "
                        f"heart_disease={registro['heart_disease']}"
                    )
                except Exception as exc:
                    print(f"[KafkaMed-Producer] Error enviando mensaje: {exc}")

                time.sleep(intervalo)

        print(f"[KafkaMed-Producer] Iteración {iteracion} completada — {enviados} registros enviados.")

        if not loop:
            continuar = False
        else:
            print("[KafkaMed-Producer] Modo loop activo. Recomenzando en 3 s...")
            time.sleep(3)

    productor.flush()
    productor.close()
    print("[KafkaMed-Producer] Productor cerrado.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="KafkaMed — Productor Kafka de registros cardíacos")
    parser.add_argument(
        "--interval", type=float, default=DEFAULT_INTERVAL,
        help="Segundos entre mensajes (default: 1.0)"
    )
    parser.add_argument(
        "--loop", action="store_true",
        help="Repetir el CSV indefinidamente al terminar"
    )
    args = parser.parse_args()

    print(f"[KafkaMed-Producer] Iniciando | broker={KAFKA_BOOTSTRAP} | topic={TOPIC} | "
          f"csv={CSV_PATH} | interval={args.interval}s | loop={args.loop}")
    publicar(CSV_PATH, args.interval, args.loop)


if __name__ == "__main__":
    main()
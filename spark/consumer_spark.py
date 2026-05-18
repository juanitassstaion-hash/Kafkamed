"""Spark Structured Streaming consumer.

Lee registros JSON desde Kafka (topic `heart-records`), aplica un modelo
Spark ML (PipelineModel) y escribe las predicciones en MongoDB.

Uso: ejecutar dentro de un contenedor Spark (ver README). Las variables
de entorno controlan conectores y rutas.
"""

import os

import json
import traceback

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml import PipelineModel

from pymongo import MongoClient


# Config (sobreescribible vía env)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "heart-records")
MODEL_PATH = os.environ.get("MODEL_PATH", "/app/data/model_rf")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "kafkamed")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "predictions")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/app/data/checkpoint")


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("KafkaMedConsumerSpark")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    return spark


def mongo_write(batch_df, epoch_id):
    # Convierte a dicts y escribe en MongoDB usando pymongo
    if batch_df.rdd.isEmpty():
        return

    records = [row.asDict(recursive=True) for row in batch_df.collect()]

    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        coll = db[MONGO_COLLECTION]
        if records:
            coll.insert_many(records, ordered=False)
        client.close()
    except Exception:
        print("[consumer_spark] Error escribiendo en MongoDB:")
        traceback.print_exc()


def main():
    spark = create_spark_session()

    # Schema esperado (coincide con normalizar_registro del productor)
    schema = T.StructType([
        T.StructField("age", T.IntegerType()),
        T.StructField("sex", T.StringType()),
        T.StructField("chest_pain_type", T.StringType()),
        T.StructField("resting_bp", T.IntegerType()),
        T.StructField("cholesterol", T.IntegerType()),
        T.StructField("fasting_bs", T.IntegerType()),
        T.StructField("resting_ecg", T.StringType()),
        T.StructField("max_hr", T.IntegerType()),
        T.StructField("exercise_angina", T.StringType()),
        T.StructField("oldpeak", T.DoubleType()),
        T.StructField("st_slope", T.StringType()),
        T.StructField("heart_disease", T.IntegerType()),
    ])

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        kafka_df.selectExpr("CAST(value AS STRING) as json_str")
        .select(F.from_json(F.col("json_str"), schema).alias("data"))
        .select("data.*")
    )

    # Cargar modelo si existe
    model = None
    try:
        model = PipelineModel.load(MODEL_PATH)
        print(f"[consumer_spark] Modelo cargado desde {MODEL_PATH}")
    except Exception:
        print(f"[consumer_spark] No se pudo cargar el modelo en {MODEL_PATH}. Se continuará usando heart_disease como predicción.")

    to_process = parsed
    if model is not None:
        to_process = model.transform(parsed)

    # Normalizar columnas de salida: prediction, probability (si existen)
    cols = list(to_process.columns)
    out = to_process

    if "probability" in cols:
        out = out.withColumn("probability", F.col("probability").getItem(1))
    else:
        # Si no hay modelo, usar heart_disease como probabilidad (0 o 1)
        out = out.withColumn("probability", F.col("heart_disease").cast(T.DoubleType()))

    if "prediction" in cols:
        out = out.withColumn(
            "prediction_label",
            F.when(F.col("prediction") == 1, F.lit("riesgo")).otherwise(F.lit("sin_riesgo"))
        )
    else:
        # Si no hay modelo, usar heart_disease como predicción
        out = out.withColumn(
            "prediction_label",
            F.when(F.col("heart_disease") == 1, F.lit("riesgo")).otherwise(F.lit("sin_riesgo"))
        )

    out = out.withColumn("timestamp", F.current_timestamp())

    query = (
        out.writeStream
        .foreachBatch(mongo_write)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    print("[consumer_spark] Streaming iniciado. Esperando datos...")
    query.awaitTermination()


if __name__ == "__main__":
    main()

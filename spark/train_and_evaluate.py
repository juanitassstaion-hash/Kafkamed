"""
KafkaMed — Entrenamiento y evaluación (Spark ML)

Entrena un PipelineModel (StringIndexer + OneHot + VectorAssembler + RandomForest),
calcula métricas: accuracy, precision, recall, F1, AUC-ROC,
y guarda:
- Modelo en /app/data/model_rf   (compatible con consumer_spark.py)
- Métricas en /app/docs/metrics.json (para el informe técnico)

Ejecutar (en contenedor Spark):
/opt/spark/bin/spark-submit --master local[2] train_and_evaluate.py
"""

import os
import json

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator


DATA_PATH   = os.environ.get("DATA_PATH",   "/app/data/heart.csv")
MODEL_PATH  = os.environ.get("MODEL_PATH",  "/app/data/model_rf")
DOCS_DIR    = os.environ.get("DOCS_DIR",    "/app/docs")
METRICS_OUT = os.path.join(DOCS_DIR, "metrics.json")


def main():
    spark = (
        SparkSession.builder
        .appName("KafkaMed-Train-Eval")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    # 1) Cargar dataset
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(DATA_PATH)
    )

    # Normalizar columnas a snake_case como usa el producer/consumer
    # (Producer manda: age, sex, chest_pain_type, resting_bp, ...)
    df = (df
          .withColumnRenamed("Age", "age")
          .withColumnRenamed("Sex", "sex")
          .withColumnRenamed("ChestPainType", "chest_pain_type")
          .withColumnRenamed("RestingBP", "resting_bp")
          .withColumnRenamed("Cholesterol", "cholesterol")
          .withColumnRenamed("FastingBS", "fasting_bs")
          .withColumnRenamed("RestingECG", "resting_ecg")
          .withColumnRenamed("MaxHR", "max_hr")
          .withColumnRenamed("ExerciseAngina", "exercise_angina")
          .withColumnRenamed("Oldpeak", "oldpeak")
          .withColumnRenamed("ST_Slope", "st_slope")
          .withColumnRenamed("HeartDisease", "heart_disease")
    )

    # 2) Features
    label_col = "heart_disease"

    cat_cols = ["sex", "chest_pain_type", "resting_ecg", "exercise_angina", "st_slope"]
    num_cols = ["age", "resting_bp", "cholesterol", "fasting_bs", "max_hr", "oldpeak"]

    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in cat_cols
    ]

    encoder = OneHotEncoder(
        inputCols=[f"{c}_idx" for c in cat_cols],
        outputCols=[f"{c}_ohe" for c in cat_cols],
        handleInvalid="keep"
    )

    assembler = VectorAssembler(
        inputCols=num_cols + [f"{c}_ohe" for c in cat_cols],
        outputCol="features",
        handleInvalid="keep"
    )

    clf = RandomForestClassifier(
        labelCol=label_col,
        featuresCol="features",
        predictionCol="prediction",
        probabilityCol="probability",
        rawPredictionCol="rawPrediction",
        numTrees=150,
        maxDepth=8,
        seed=42
    )

    pipeline = Pipeline(stages=indexers + [encoder, assembler, clf])

    # 3) Split
    train, test = df.randomSplit([0.8, 0.2], seed=42)

    # 4) Train
    model = pipeline.fit(train)

    # 5) Predict
    pred = model.transform(test).select(label_col, "prediction", "rawPrediction", "probability")

    # 6) Métricas
    # Confusion matrix para clase positiva (1)
    cm = (pred
          .withColumn("tp", F.when((F.col(label_col) == 1) & (F.col("prediction") == 1), 1).otherwise(0))
          .withColumn("tn", F.when((F.col(label_col) == 0) & (F.col("prediction") == 0), 1).otherwise(0))
          .withColumn("fp", F.when((F.col(label_col) == 0) & (F.col("prediction") == 1), 1).otherwise(0))
          .withColumn("fn", F.when((F.col(label_col) == 1) & (F.col("prediction") == 0), 1).otherwise(0))
          .agg(
              F.sum("tp").alias("tp"),
              F.sum("tn").alias("tn"),
              F.sum("fp").alias("fp"),
              F.sum("fn").alias("fn"),
              F.count("*").alias("total")
          )
          .collect()[0]
    )

    tp, tn, fp, fn, total = cm["tp"], cm["tn"], cm["fp"], cm["fn"], cm["total"]

    accuracy = (tp + tn) / total if total else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0

    evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    auc_roc = evaluator.evaluate(pred)

    metrics = {
        "accuracy": round(float(accuracy), 4),
        "precision": round(float(precision), 4),
        "recall": round(float(recall), 4),
        "f1": round(float(f1), 4),
        "auc_roc": round(float(auc_roc), 4),
        "confusion_matrix": {"tp": int(tp), "tn": int(tn), "fp": int(fp), "fn": int(fn)},
        "test_rows": int(total)
    }

    # 7) Guardar modelo (compatible con consumer_spark.py)
    model.write().overwrite().save(MODEL_PATH)

    # 8) Guardar métricas para informe
    os.makedirs(DOCS_DIR, exist_ok=True)
    with open(METRICS_OUT, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)

    print("✅ Modelo guardado en:", MODEL_PATH)
    print("✅ Métricas guardadas en:", METRICS_OUT)
    print("✅ Métricas:", metrics)

    spark.stop()


if __name__ == "__main__":
    main()
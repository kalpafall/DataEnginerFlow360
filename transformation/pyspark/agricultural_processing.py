#!/usr/bin/env python3
"""
Transformations PySpark pour les données agricoles
Traite les données brutes et génère des analyses agrégées
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
import json

# Initialiser Spark
spark = SparkSession.builder \
    .appName("Agricultural Data Processing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=" * 70)
print("🌾 TRANSFORMATIONS PYSPARK - DONNÉES AGRICOLES")
print("=" * 70)
print(f"⏰ Heure de début: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# Chemins
RAW_PATH = "data_lake/raw/agriculture"
PROCESSED_PATH = "data_lake/processed/agriculture"

# ============================================================================
# 1. AGRÉGATION DES CAPTEURS IOT
# ============================================================================
print("📊 TRANSFORMATION 1: Agrégation des capteurs IoT")
print("-" * 70)

# Charger les données capteurs
sensors_df = spark.read.json(f"{RAW_PATH}/sensors.json")

print(f"   ✅ Chargé {sensors_df.count()} lectures de capteurs")

# Convertir timestamp en date
sensors_df = sensors_df.withColumn("reading_date", to_date(col("timestamp")))

# Agrégation par parcelle, date et type de capteur
sensors_agg = sensors_df.groupBy("field_id", "reading_date", "sensor_type") \
    .agg(
        count("*").alias("num_readings"),
        avg("value").alias("avg_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        stddev("value").alias("stddev_value"),
        first("unit").alias("unit")
    ) \
    .orderBy("field_id", "reading_date", "sensor_type")

# Sauvegarder
output_path = f"{PROCESSED_PATH}/sensors_aggregated"
sensors_agg.write.mode("overwrite").parquet(output_path)
print(f"   ✅ Sauvegardé {sensors_agg.count()} agrégations → {output_path}")

# Afficher un échantillon
print("\n   📋 Échantillon des données agrégées:")
sensors_agg.show(5, truncate=False)

# ============================================================================
# 2. CALCUL DES RENDEMENTS
# ============================================================================
print("\n📊 TRANSFORMATION 2: Calcul des rendements")
print("-" * 70)

# Charger les données
crops_df = spark.read.json(f"{RAW_PATH}/crops.json")
harvests_df = spark.read.json(f"{RAW_PATH}/harvests.json")
fields_df = spark.read.json(f"{RAW_PATH}/fields.json")

print(f"   ✅ Chargé {crops_df.count()} cultures")
print(f"   ✅ Chargé {harvests_df.count()} récoltes")

# Joindre cultures et récoltes
yield_df = harvests_df.join(crops_df, "crop_id") \
    .join(fields_df, "field_id")

# Calculer les rendements
yield_df = yield_df.withColumn(
    "actual_yield_t_ha",
    col("quantity_tonnes") / col("area_ha")
).withColumn(
    "yield_variance_percent",
    ((col("actual_yield_t_ha") - col("estimated_yield_t_ha")) / col("estimated_yield_t_ha") * 100)
)

# Agrégation par type de culture
yield_summary = yield_df.groupBy("crop_type", "category", "season") \
    .agg(
        count("*").alias("num_harvests"),
        sum("quantity_tonnes").alias("total_production_tonnes"),
        sum("area_ha").alias("total_area_ha"),
        avg("actual_yield_t_ha").alias("avg_yield_t_ha"),
        min("actual_yield_t_ha").alias("min_yield_t_ha"),
        max("actual_yield_t_ha").alias("max_yield_t_ha"),
        avg("yield_variance_percent").alias("avg_variance_percent")
    ) \
    .orderBy(desc("total_production_tonnes"))

# Sauvegarder
output_path = f"{PROCESSED_PATH}/yield_analysis"
yield_summary.write.mode("overwrite").parquet(output_path)
print(f"   ✅ Sauvegardé l'analyse des rendements → {output_path}")

print("\n   📋 Rendements par culture:")
yield_summary.show(10, truncate=False)

# ============================================================================
# 3. CORRÉLATION MÉTÉO / RENDEMENT
# ============================================================================
print("\n📊 TRANSFORMATION 3: Corrélation météo/rendement")
print("-" * 70)

# Charger les données météo
weather_df = spark.read.json(f"{RAW_PATH}/weather.json")

print(f"   ✅ Chargé {weather_df.count()} enregistrements météo")

# Extraire les températures de la structure JSON
weather_df = weather_df.withColumn("temp_min_c", col("temperature.min_c")) \
    .withColumn("temp_max_c", col("temperature.max_c")) \
    .withColumn("temp_avg_c", col("temperature.avg_c"))

# Agrégation météo par mois
weather_monthly = weather_df.withColumn("year_month", date_format(col("date"), "yyyy-MM")) \
    .groupBy("year_month", "location.name") \
    .agg(
        avg("temp_avg_c").alias("avg_temperature"),
        sum("precipitation_mm").alias("total_precipitation"),
        avg("humidity_percent").alias("avg_humidity"),
        avg("sunshine_hours").alias("avg_sunshine")
    )

# Joindre avec les rendements (par période de culture)
# Pour simplifier, on fait une agrégation globale
weather_impact = weather_monthly.join(
    yield_summary.select("crop_type", "avg_yield_t_ha"),
    how="cross"
).select(
    "crop_type",
    "year_month",
    "avg_temperature",
    "total_precipitation",
    "avg_humidity",
    "avg_sunshine",
    "avg_yield_t_ha"
)

# Sauvegarder
output_path = f"{PROCESSED_PATH}/weather_impact"
weather_impact.write.mode("overwrite").parquet(output_path)
print(f"   ✅ Sauvegardé l'analyse météo → {output_path}")

print("\n   📋 Impact météo (échantillon):")
weather_impact.show(5, truncate=False)

# ============================================================================
# 4. DÉTECTION D'ANOMALIES
# ============================================================================
print("\n📊 TRANSFORMATION 4: Détection d'anomalies")
print("-" * 70)

# Anomalies dans les capteurs
# Calculer les statistiques globales par type de capteur
sensor_stats = sensors_df.groupBy("sensor_type") \
    .agg(
        avg("value").alias("global_avg"),
        stddev("value").alias("global_stddev")
    )

# Joindre et détecter les valeurs aberrantes (> 3 écarts-types)
sensors_with_stats = sensors_df.join(sensor_stats, "sensor_type")

anomalies_sensors = sensors_with_stats.withColumn(
    "z_score",
    (col("value") - col("global_avg")) / col("global_stddev")
).filter(
    abs(col("z_score")) > 3
).select(
    "sensor_id",
    "field_id",
    "sensor_type",
    "timestamp",
    "value",
    "unit",
    "z_score"
).orderBy(desc("z_score"))

print(f"   ⚠️  Détecté {anomalies_sensors.count()} anomalies dans les capteurs")

# Anomalies dans les rendements (rendement < 50% de la moyenne)
avg_yields = yield_df.groupBy("crop_type") \
    .agg(avg("actual_yield_t_ha").alias("avg_yield"))

anomalies_yield = yield_df.join(avg_yields, "crop_type") \
    .filter(col("actual_yield_t_ha") < col("avg_yield") * 0.5) \
    .select(
        "crop_id",
        "field_id",
        "crop_type",
        "actual_yield_t_ha",
        "avg_yield",
        ((col("actual_yield_t_ha") / col("avg_yield") - 1) * 100).alias("deviation_percent")
    ) \
    .orderBy("deviation_percent")

print(f"   ⚠️  Détecté {anomalies_yield.count()} rendements anormalement bas")

# Sauvegarder les anomalies
output_path = f"{PROCESSED_PATH}/anomalies_sensors"
anomalies_sensors.write.mode("overwrite").parquet(output_path)
print(f"   ✅ Sauvegardé anomalies capteurs → {output_path}")

output_path = f"{PROCESSED_PATH}/anomalies_yield"
anomalies_yield.write.mode("overwrite").parquet(output_path)
print(f"   ✅ Sauvegardé anomalies rendements → {output_path}")

if anomalies_sensors.count() > 0:
    print("\n   📋 Exemples d'anomalies capteurs:")
    anomalies_sensors.show(5, truncate=False)

if anomalies_yield.count() > 0:
    print("\n   📋 Exemples d'anomalies rendements:")
    anomalies_yield.show(5, truncate=False)

# ============================================================================
# 5. GÉNÉRATION DE RAPPORTS SYNTHÉTIQUES
# ============================================================================
print("\n📊 TRANSFORMATION 5: Rapports synthétiques")
print("-" * 70)

# Rapport global
farms_df = spark.read.json(f"{RAW_PATH}/farms.json")
sales_df = spark.read.json(f"{RAW_PATH}/sales.json")

# Statistiques globales
total_farms = farms_df.count()
total_area = farms_df.agg(sum("total_area_ha")).collect()[0][0]
total_production = harvests_df.agg(sum("quantity_tonnes")).collect()[0][0]
total_revenue = sales_df.agg(sum("total_amount")).collect()[0][0]

report = {
    "generated_at": datetime.now().isoformat(),
    "summary": {
        "total_farms": int(total_farms),
        "total_area_ha": float(total_area),
        "total_production_tonnes": float(total_production),
        "total_revenue_fcfa": float(total_revenue),
        "avg_yield_t_ha": float(total_production / total_area) if total_area > 0 else 0
    },
    "top_crops": yield_summary.limit(5).toPandas().to_dict('records'),
    "anomalies": {
        "sensor_anomalies": int(anomalies_sensors.count()),
        "yield_anomalies": int(anomalies_yield.count())
    }
}

# Sauvegarder le rapport
report_path = f"{PROCESSED_PATH}/summary_report.json"
with open(report_path, 'w', encoding='utf-8') as f:
    json.dump(report, f, indent=2, ensure_ascii=False)

print(f"   ✅ Rapport synthétique généré → {report_path}")

# ============================================================================
# RÉSUMÉ
# ============================================================================
print("\n" + "=" * 70)
print("✅ TRANSFORMATIONS TERMINÉES")
print("=" * 70)
print("\n📊 Résumé des transformations:")
print(f"   1. Capteurs agrégés: {sensors_agg.count()} enregistrements")
print(f"   2. Analyse rendements: {yield_summary.count()} cultures")
print(f"   3. Impact météo: {weather_impact.count()} enregistrements")
print(f"   4. Anomalies capteurs: {anomalies_sensors.count()}")
print(f"   5. Anomalies rendements: {anomalies_yield.count()}")
print(f"\n💾 Données sauvegardées dans: {PROCESSED_PATH}/")
print(f"⏰ Heure de fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# Arrêter Spark
spark.stop()

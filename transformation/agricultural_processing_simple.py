#!/usr/bin/env python3
"""
Transformations simplifiées pour les données agricoles
Version utilisant pandas pour plus de simplicité
"""
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import numpy as np

print("=" * 70)
print("🌾 TRANSFORMATIONS - DONNÉES AGRICOLES")
print("=" * 70)
print(f"⏰ Heure de début: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# Chemins
RAW_PATH = Path("data_lake/raw/agriculture")
PROCESSED_PATH = Path("data_lake/processed/agriculture")
PROCESSED_PATH.mkdir(parents=True, exist_ok=True)

# ============================================================================
# 1. AGRÉGATION DES CAPTEURS IOT
# ============================================================================
print("📊 TRANSFORMATION 1: Agrégation des capteurs IoT")
print("-" * 70)

# Charger les données capteurs
with open(RAW_PATH / "sensors.json", 'r') as f:
    sensors_data = json.load(f)

sensors_df = pd.DataFrame(sensors_data)
print(f"   ✅ Chargé {len(sensors_df)} lectures de capteurs")

# Convertir timestamp en date
sensors_df['timestamp'] = pd.to_datetime(sensors_df['timestamp'])
sensors_df['reading_date'] = sensors_df['timestamp'].dt.date

# Agrégation par parcelle, date et type de capteur
sensors_agg = sensors_df.groupby(['field_id', 'reading_date', 'sensor_type']).agg({
    'value': ['count', 'mean', 'min', 'max', 'std'],
    'unit': 'first'
}).reset_index()

# Aplatir les colonnes multi-niveaux
sensors_agg.columns = ['field_id', 'reading_date', 'sensor_type', 
                        'num_readings', 'avg_value', 'min_value', 'max_value', 'stddev_value', 'unit']

# Sauvegarder
output_file = PROCESSED_PATH / "sensors_aggregated.json"
sensors_agg.to_json(output_file, orient='records', date_format='iso', indent=2)
print(f"   ✅ Sauvegardé {len(sensors_agg)} agrégations → {output_file}")

print("\n   📋 Échantillon des données agrégées:")
print(sensors_agg.head())

# ============================================================================
# 2. CALCUL DES RENDEMENTS
# ============================================================================
print("\n📊 TRANSFORMATION 2: Calcul des rendements")
print("-" * 70)

# Charger les données
with open(RAW_PATH / "crops.json", 'r') as f:
    crops_data = json.load(f)
with open(RAW_PATH / "harvests.json", 'r') as f:
    harvests_data = json.load(f)
with open(RAW_PATH / "fields.json", 'r') as f:
    fields_data = json.load(f)

crops_df = pd.DataFrame(crops_data)
harvests_df = pd.DataFrame(harvests_data)
fields_df = pd.DataFrame(fields_data)

print(f"   ✅ Chargé {len(crops_df)} cultures")
print(f"   ✅ Chargé {len(harvests_df)} récoltes")

# Joindre les données
yield_df = harvests_df.merge(crops_df, on='crop_id') \
                      .merge(fields_df, on='field_id')

# Calculer les rendements
yield_df['actual_yield_t_ha'] = yield_df['quantity_tonnes'] / yield_df['area_ha']
yield_df['yield_variance_percent'] = ((yield_df['actual_yield_t_ha'] - yield_df['estimated_yield_t_ha']) / 
                                       yield_df['estimated_yield_t_ha'] * 100)

# Agrégation par type de culture
yield_summary = yield_df.groupby(['crop_type', 'category', 'season']).agg({
    'harvest_id': 'count',
    'quantity_tonnes': 'sum',
    'area_ha': 'sum',
    'actual_yield_t_ha': ['mean', 'min', 'max'],
    'yield_variance_percent': 'mean'
}).reset_index()

# Aplatir les colonnes
yield_summary.columns = ['crop_type', 'category', 'season', 'num_harvests', 
                         'total_production_tonnes', 'total_area_ha',
                         'avg_yield_t_ha', 'min_yield_t_ha', 'max_yield_t_ha', 
                         'avg_variance_percent']

# Trier par production
yield_summary = yield_summary.sort_values('total_production_tonnes', ascending=False)

# Sauvegarder
output_file = PROCESSED_PATH / "yield_analysis.json"
yield_summary.to_json(output_file, orient='records', indent=2)
print(f"   ✅ Sauvegardé l'analyse des rendements → {output_file}")

print("\n   📋 Rendements par culture:")
print(yield_summary.head(10))

# ============================================================================
# 3. DÉTECTION D'ANOMALIES
# ============================================================================
print("\n📊 TRANSFORMATION 3: Détection d'anomalies")
print("-" * 70)

# Anomalies dans les capteurs (z-score > 3)
sensor_stats = sensors_df.groupby('sensor_type')['value'].agg(['mean', 'std']).reset_index()
sensors_with_stats = sensors_df.merge(sensor_stats, on='sensor_type')
sensors_with_stats['z_score'] = (sensors_with_stats['value'] - sensors_with_stats['mean']) / sensors_with_stats['std']

anomalies_sensors = sensors_with_stats[abs(sensors_with_stats['z_score']) > 3][
    ['sensor_id', 'field_id', 'sensor_type', 'timestamp', 'value', 'unit', 'z_score']
].sort_values('z_score', ascending=False)

print(f"   ⚠️  Détecté {len(anomalies_sensors)} anomalies dans les capteurs")

# Anomalies dans les rendements (< 50% de la moyenne)
avg_yields = yield_df.groupby('crop_type')['actual_yield_t_ha'].mean().reset_index()
avg_yields.columns = ['crop_type', 'avg_yield']

yield_with_avg = yield_df.merge(avg_yields, on='crop_type')
anomalies_yield = yield_with_avg[yield_with_avg['actual_yield_t_ha'] < yield_with_avg['avg_yield'] * 0.5][
    ['crop_id', 'field_id', 'crop_type', 'actual_yield_t_ha', 'avg_yield']
].copy()
anomalies_yield['deviation_percent'] = ((anomalies_yield['actual_yield_t_ha'] / anomalies_yield['avg_yield'] - 1) * 100)
anomalies_yield = anomalies_yield.sort_values('deviation_percent')

print(f"   ⚠️  Détecté {len(anomalies_yield)} rendements anormalement bas")

# Sauvegarder
output_file = PROCESSED_PATH / "anomalies_sensors.json"
anomalies_sensors.to_json(output_file, orient='records', date_format='iso', indent=2)
print(f"   ✅ Sauvegardé anomalies capteurs → {output_file}")

output_file = PROCESSED_PATH / "anomalies_yield.json"
anomalies_yield.to_json(output_file, orient='records', indent=2)
print(f"   ✅ Sauvegardé anomalies rendements → {output_file}")

if len(anomalies_sensors) > 0:
    print("\n   📋 Exemples d'anomalies capteurs:")
    print(anomalies_sensors.head())

if len(anomalies_yield) > 0:
    print("\n   📋 Exemples d'anomalies rendements:")
    print(anomalies_yield.head())

# ============================================================================
# 4. RAPPORT SYNTHÉTIQUE
# ============================================================================
print("\n📊 TRANSFORMATION 4: Rapport synthétique")
print("-" * 70)

# Charger les données manquantes
with open(RAW_PATH / "farms.json", 'r') as f:
    farms_data = json.load(f)
with open(RAW_PATH / "sales.json", 'r') as f:
    sales_data = json.load(f)

farms_df = pd.DataFrame(farms_data)
sales_df = pd.DataFrame(sales_data)

# Statistiques globales
report = {
    "generated_at": datetime.now().isoformat(),
    "summary": {
        "total_farms": int(len(farms_df)),
        "total_area_ha": float(farms_df['total_area_ha'].sum()),
        "total_production_tonnes": float(harvests_df['quantity_tonnes'].sum()),
        "total_revenue_fcfa": float(sales_df['total_amount'].sum()),
        "avg_yield_t_ha": float(yield_df['actual_yield_t_ha'].mean())
    },
    "top_crops": yield_summary.head(5).to_dict('records'),
    "anomalies": {
        "sensor_anomalies": int(len(anomalies_sensors)),
        "yield_anomalies": int(len(anomalies_yield))
    },
    "by_category": yield_summary.groupby('category').agg({
        'total_production_tonnes': 'sum',
        'avg_yield_t_ha': 'mean'
    }).to_dict('index')
}

# Sauvegarder
report_path = PROCESSED_PATH / "summary_report.json"
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
print(f"   1. Capteurs agrégés: {len(sensors_agg)} enregistrements")
print(f"   2. Analyse rendements: {len(yield_summary)} cultures")
print(f"   3. Anomalies capteurs: {len(anomalies_sensors)}")
print(f"   4. Anomalies rendements: {len(anomalies_yield)}")
print(f"\n💾 Données sauvegardées dans: {PROCESSED_PATH}/")
print(f"⏰ Heure de fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

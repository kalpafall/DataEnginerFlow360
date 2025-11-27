#!/usr/bin/env python3
"""
Script de simulation pour le streaming continu de données capteurs.
Permet de tester les alertes Prometheus en générant différents scénarios.

Usage:
  python3 continuous_sensor_stream.py --scenario normal
  python3 continuous_sensor_stream.py --scenario drought  (Déclenche SoilMoistureCritical)
  python3 continuous_sensor_stream.py --scenario heatwave (Déclenche HighTemperatureWarning)
"""
import time
import json
import random
import argparse
from datetime import datetime
from kafka import KafkaProducer

# Configuration Kafka
KAFKA_BOOTSTRAP = 'localhost:9093'
TOPIC = 'sensor_data'

def get_sensor_data(scenario='normal'):
    """Génère des données capteurs selon le scénario"""
    timestamp = datetime.now().isoformat()
    
    data = []
    
    # 1. Humidité du sol (Soil Moisture)
    if scenario == 'drought':
        # Valeur critique < 20%
        moisture = round(random.uniform(5.0, 18.0), 2)
    else:
        # Valeur normale > 30%
        moisture = round(random.uniform(30.0, 60.0), 2)
        
    data.append({
        "sensor_id": f"SENSOR-MOIST-{random.randint(1, 5)}",
        "sensor_type": "soil_moisture",
        "value": moisture,
        "unit": "percent",
        "location": "Field-A",
        "timestamp": timestamp
    })
    
    # 2. Température de l'air
    if scenario == 'heatwave':
        # Valeur critique > 35°C
        temp = round(random.uniform(36.0, 42.0), 2)
    else:
        # Valeur normale 20-30°C
        temp = round(random.uniform(20.0, 30.0), 2)
        
    data.append({
        "sensor_id": f"SENSOR-TEMP-{random.randint(1, 5)}",
        "sensor_type": "air_temperature",
        "value": temp,
        "unit": "celsius",
        "location": "Field-A",
        "timestamp": timestamp
    })
    
    return data

def main():
    parser = argparse.ArgumentParser(description='Simulateur de flux capteurs IoT')
    parser.add_argument('--scenario', choices=['normal', 'drought', 'heatwave'], 
                        default='normal', help='Scénario de données à générer')
    parser.add_argument('--interval', type=float, default=1.0, 
                        help='Intervalle entre les envois (secondes)')
    args = parser.parse_args()
    
    print(f"🚀 Démarrage du simulateur - Scénario: {args.scenario.upper()}")
    print(f"📡 Connexion à Kafka ({KAFKA_BOOTSTRAP})...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Connecté! Envoi des données (Ctrl+C pour arrêter)...")
        print("-" * 50)
        
        count = 0
        while True:
            sensors = get_sensor_data(args.scenario)
            
            for sensor in sensors:
                producer.send(TOPIC, value=sensor)
                
                # Affichage stylé
                status = "🟢"
                if args.scenario == 'drought' and sensor['sensor_type'] == 'soil_moisture':
                    status = "🔴 DRY"
                elif args.scenario == 'heatwave' and sensor['sensor_type'] == 'air_temperature':
                    status = "🔴 HOT"
                
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {status} {sensor['sensor_type']}: {sensor['value']} {sensor['unit']}")
            
            count += 1
            if count % 10 == 0:
                producer.flush()
                
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print("\n🛑 Arrêt du simulateur.")
    except Exception as e:
        print(f"\n❌ Erreur: {e}")

if __name__ == "__main__":
    main()

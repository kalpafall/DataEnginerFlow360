#!/usr/bin/env python3
"""
Générateur de données agricoles pour le pipeline DataEnginerFlow360
Données adaptées pour l'Afrique de l'Ouest (Sénégal, Mali, Burkina Faso, etc.)
"""
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker
from typing import List, Dict, Any

# Configuration
fake = Faker('fr_FR')
Faker.seed(42)
random.seed(42)

# Cultures typiques d'Afrique de l'Ouest
CROPS = {
    'cereals': {
        'mil': {'season': 'hivernage', 'yield_range': (0.8, 1.5), 'price_range': (150, 250)},
        'sorgho': {'season': 'hivernage', 'yield_range': (1.0, 2.0), 'price_range': (140, 220)},
        'maïs': {'season': 'hivernage', 'yield_range': (1.5, 3.0), 'price_range': (160, 280)},
        'riz': {'season': 'hivernage', 'yield_range': (2.0, 4.5), 'price_range': (200, 350)},
        'fonio': {'season': 'hivernage', 'yield_range': (0.6, 1.2), 'price_range': (300, 500)},
    },
    'legumes': {
        'arachide': {'season': 'hivernage', 'yield_range': (1.0, 2.5), 'price_range': (250, 400)},
        'niébé': {'season': 'hivernage', 'yield_range': (0.5, 1.2), 'price_range': (300, 500)},
        'sésame': {'season': 'hivernage', 'yield_range': (0.4, 0.8), 'price_range': (400, 700)},
    },
    'maraichage': {
        'tomate': {'season': 'contre-saison', 'yield_range': (15, 35), 'price_range': (100, 300)},
        'oignon': {'season': 'contre-saison', 'yield_range': (20, 40), 'price_range': (150, 350)},
        'chou': {'season': 'contre-saison', 'yield_range': (25, 45), 'price_range': (80, 200)},
        'carotte': {'season': 'contre-saison', 'yield_range': (18, 35), 'price_range': (120, 250)},
        'laitue': {'season': 'contre-saison', 'yield_range': (12, 25), 'price_range': (100, 220)},
    },
    'arboriculture': {
        'mangue': {'season': 'saison-seche', 'yield_range': (5, 15), 'price_range': (200, 500)},
        'anacarde': {'season': 'saison-seche', 'yield_range': (0.8, 2.0), 'price_range': (500, 1200)},
        'agrumes': {'season': 'saison-seche', 'yield_range': (10, 25), 'price_range': (150, 400)},
    }
}

# Régions du Sénégal
REGIONS = [
    {'name': 'Dakar', 'lat': 14.7167, 'lon': -17.4677},
    {'name': 'Thiès', 'lat': 14.7886, 'lon': -16.9260},
    {'name': 'Saint-Louis', 'lat': 16.0179, 'lon': -16.4897},
    {'name': 'Kaolack', 'lat': 14.1500, 'lon': -16.0833},
    {'name': 'Tambacounda', 'lat': 13.7708, 'lon': -13.6681},
    {'name': 'Kolda', 'lat': 12.8833, 'lon': -14.9500},
    {'name': 'Ziguinchor', 'lat': 12.5833, 'lon': -16.2667},
    {'name': 'Louga', 'lat': 15.6167, 'lon': -16.2167},
    {'name': 'Fatick', 'lat': 14.3333, 'lon': -16.4167},
    {'name': 'Diourbel', 'lat': 14.6500, 'lon': -16.2333},
]

# Types de sol
SOIL_TYPES = ['argileux', 'sableux', 'limoneux', 'argilo-sableux', 'latéritique']

# Types d'irrigation
IRRIGATION_TYPES = ['pluvial', 'goutte-à-goutte', 'aspersion', 'gravitaire', 'pompage']

# Types de capteurs
SENSOR_TYPES = ['soil_moisture', 'soil_temperature', 'soil_ph', 'irrigation_level', 'air_temperature']


class AgriculturalDataGenerator:
    """Générateur de données agricoles réalistes"""
    
    def __init__(self, output_dir: str = "data_lake/raw/agriculture"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.farms = []
        self.fields = []
        self.crops = []
        self.harvests = []
        self.weather_data = []
        self.sensor_data = []
        self.sales = []
    
    def generate_farms(self, num_farms: int = 20) -> List[Dict[str, Any]]:
        """Générer des exploitations agricoles"""
        print(f"   Génération de {num_farms} exploitations agricoles...")
        
        farming_types = ['familiale', 'coopérative', 'agro-industrielle', 'bio']
        
        for i in range(num_farms):
            region = random.choice(REGIONS)
            farm = {
                'farm_id': f'FARM-{i+1:04d}',
                'name': f'Exploitation {fake.last_name()}',
                'owner': fake.name(),
                'location': {
                    'region': region['name'],
                    'latitude': region['lat'] + random.uniform(-0.5, 0.5),
                    'longitude': region['lon'] + random.uniform(-0.5, 0.5),
                    'village': fake.city(),
                },
                'total_area_ha': round(random.uniform(5, 200), 2),
                'farming_type': random.choice(farming_types),
                'created_date': fake.date_between(start_date='-20y', end_date='-1y').isoformat(),
                'contact': {
                    'phone': fake.phone_number(),
                    'email': fake.email() if random.random() > 0.3 else None,
                },
                'certifications': random.sample(['bio', 'GlobalGAP', 'commerce_equitable'], 
                                               k=random.randint(0, 2)),
            }
            self.farms.append(farm)
        
        return self.farms
    
    def generate_fields(self, fields_per_farm: int = 5) -> List[Dict[str, Any]]:
        """Générer des parcelles"""
        print(f"   Génération de parcelles ({fields_per_farm} par exploitation)...")
        
        field_id = 1
        for farm in self.farms:
            num_fields = random.randint(2, fields_per_farm)
            remaining_area = farm['total_area_ha']
            
            for _ in range(num_fields):
                area = round(random.uniform(0.5, remaining_area / (num_fields - _ + 1)), 2)
                remaining_area -= area
                
                field = {
                    'field_id': f'FIELD-{field_id:05d}',
                    'farm_id': farm['farm_id'],
                    'name': f'Parcelle {fake.word().capitalize()}',
                    'area_ha': area,
                    'soil_type': random.choice(SOIL_TYPES),
                    'location': {
                        'latitude': farm['location']['latitude'] + random.uniform(-0.01, 0.01),
                        'longitude': farm['location']['longitude'] + random.uniform(-0.01, 0.01),
                    },
                    'slope_percent': round(random.uniform(0, 15), 1),
                    'irrigation_type': random.choice(IRRIGATION_TYPES),
                    'has_sensors': random.random() > 0.4,
                }
                self.fields.append(field)
                field_id += 1
        
        return self.fields
    
    def generate_crops(self, crops_per_field: int = 2) -> List[Dict[str, Any]]:
        """Générer des cultures"""
        print(f"   Génération de cultures...")
        
        crop_id = 1
        current_year = datetime.now().year
        
        for field in self.fields:
            # Générer des cultures pour les 2 dernières années
            for year in [current_year - 1, current_year]:
                num_crops = random.randint(1, crops_per_field)
                
                for _ in range(num_crops):
                    # Choisir une catégorie et une culture
                    category = random.choice(list(CROPS.keys()))
                    crop_name = random.choice(list(CROPS[category].keys()))
                    crop_info = CROPS[category][crop_name]
                    
                    # Dates de semis et récolte selon la saison
                    if crop_info['season'] == 'hivernage':
                        sowing_date = datetime(year, random.randint(6, 7), random.randint(1, 28))
                        harvest_date = sowing_date + timedelta(days=random.randint(90, 150))
                    elif crop_info['season'] == 'contre-saison':
                        sowing_date = datetime(year, random.randint(11, 12), random.randint(1, 28))
                        harvest_date = sowing_date + timedelta(days=random.randint(60, 120))
                    else:  # saison-seche
                        sowing_date = datetime(year, random.randint(1, 3), random.randint(1, 28))
                        harvest_date = sowing_date + timedelta(days=random.randint(120, 180))
                    
                    crop = {
                        'crop_id': f'CROP-{crop_id:06d}',
                        'field_id': field['field_id'],
                        'crop_type': crop_name,
                        'category': category,
                        'variety': f'{crop_name.capitalize()} {random.choice(["locale", "améliorée", "hybride"])}',
                        'sowing_date': sowing_date.isoformat(),
                        'expected_harvest_date': harvest_date.isoformat(),
                        'area_ha': field['area_ha'],
                        'estimated_yield_t_ha': round(random.uniform(*crop_info['yield_range']), 2),
                        'season': crop_info['season'],
                        'year': year,
                    }
                    self.crops.append(crop)
                    crop_id += 1
        
        return self.crops
    
    def generate_harvests(self) -> List[Dict[str, Any]]:
        """Générer des données de récolte"""
        print(f"   Génération de données de récolte...")
        
        harvest_id = 1
        quality_grades = ['A', 'B', 'C']
        
        for crop in self.crops:
            # Seulement pour les cultures déjà récoltées
            harvest_date = datetime.fromisoformat(crop['expected_harvest_date'])
            if harvest_date > datetime.now():
                continue
            
            # Obtenir les infos de prix
            category = crop['category']
            crop_type = crop['crop_type']
            price_range = CROPS[category][crop_type]['price_range']
            
            # Rendement réel avec variation
            actual_yield = crop['estimated_yield_t_ha'] * random.uniform(0.7, 1.3)
            total_quantity = round(actual_yield * crop['area_ha'], 2)
            
            harvest = {
                'harvest_id': f'HARVEST-{harvest_id:06d}',
                'crop_id': crop['crop_id'],
                'harvest_date': (harvest_date + timedelta(days=random.randint(-7, 7))).isoformat(),
                'quantity_tonnes': total_quantity,
                'quality_grade': random.choice(quality_grades),
                'moisture_percent': round(random.uniform(10, 18), 1),
                'storage_location': random.choice(['ferme', 'coopérative', 'entrepôt_privé']),
                'estimated_price_per_tonne': round(random.uniform(*price_range), 2),
            }
            self.harvests.append(harvest)
            harvest_id += 1
        
        return self.harvests
    
    def generate_weather_data(self, days: int = 90) -> List[Dict[str, Any]]:
        """Générer des données météorologiques"""
        print(f"   Génération de données météo ({days} jours)...")
        
        # Créer une station météo par région
        stations = {}
        for i, region in enumerate(REGIONS[:5]):  # 5 stations principales
            stations[f'STATION-{i+1:03d}'] = region
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        for station_id, region in stations.items():
            current_date = start_date
            
            while current_date <= end_date:
                # Températures selon la saison
                month = current_date.month
                if month in [11, 12, 1, 2]:  # Saison sèche froide
                    temp_base = 25
                elif month in [3, 4, 5]:  # Saison sèche chaude
                    temp_base = 35
                else:  # Hivernage
                    temp_base = 28
                
                weather = {
                    'station_id': station_id,
                    'location': region,
                    'date': current_date.date().isoformat(),
                    'temperature': {
                        'min_c': round(temp_base - random.uniform(5, 10), 1),
                        'max_c': round(temp_base + random.uniform(5, 10), 1),
                        'avg_c': round(temp_base + random.uniform(-2, 2), 1),
                    },
                    'precipitation_mm': round(random.uniform(0, 50) if month in [6, 7, 8, 9] else random.uniform(0, 5), 1),
                    'humidity_percent': round(random.uniform(30, 90), 1),
                    'wind_speed_kmh': round(random.uniform(5, 25), 1),
                    'sunshine_hours': round(random.uniform(6, 12), 1),
                }
                self.weather_data.append(weather)
                current_date += timedelta(days=1)
        
        return self.weather_data
    
    def generate_sensor_data(self, readings_per_sensor: int = 100) -> List[Dict[str, Any]]:
        """Générer des données de capteurs IoT"""
        print(f"   Génération de données capteurs IoT...")
        
        # Sélectionner les parcelles avec capteurs
        fields_with_sensors = [f for f in self.fields if f.get('has_sensors')]
        
        sensor_id = 1
        for field in fields_with_sensors:
            # 2-3 capteurs par parcelle
            num_sensors = random.randint(2, 3)
            
            for _ in range(num_sensors):
                sensor_type = random.choice(SENSOR_TYPES)
                current_sensor_id = f'SENSOR-{sensor_id:05d}'
                
                # Générer des lectures sur les 30 derniers jours
                end_time = datetime.now()
                start_time = end_time - timedelta(days=30)
                
                for i in range(readings_per_sensor):
                    timestamp = start_time + timedelta(
                        minutes=random.randint(0, int((end_time - start_time).total_seconds() / 60))
                    )
                    
                    reading = {
                        'sensor_id': current_sensor_id,
                        'field_id': field['field_id'],
                        'sensor_type': sensor_type,
                        'timestamp': timestamp.isoformat(),
                    }
                    
                    # Valeurs selon le type de capteur
                    if sensor_type == 'soil_moisture':
                        reading['value'] = round(random.uniform(20, 80), 1)
                        reading['unit'] = 'percent'
                    elif sensor_type == 'soil_temperature':
                        reading['value'] = round(random.uniform(18, 35), 1)
                        reading['unit'] = 'celsius'
                    elif sensor_type == 'soil_ph':
                        reading['value'] = round(random.uniform(5.5, 8.0), 1)
                        reading['unit'] = 'ph'
                    elif sensor_type == 'irrigation_level':
                        reading['value'] = round(random.uniform(0, 100), 1)
                        reading['unit'] = 'percent'
                    elif sensor_type == 'air_temperature':
                        reading['value'] = round(random.uniform(20, 40), 1)
                        reading['unit'] = 'celsius'
                    
                    self.sensor_data.append(reading)
                
                sensor_id += 1
        
        return self.sensor_data
    
    def generate_sales(self) -> List[Dict[str, Any]]:
        """Générer des transactions commerciales"""
        print(f"   Génération de transactions commerciales...")
        
        sale_id = 1
        buyers = ['Marché local', 'Coopérative', 'Exportateur', 'Grossiste', 'Transformateur']
        sale_types = ['direct', 'coopérative', 'export', 'contrat']
        
        for harvest in self.harvests:
            # Certaines récoltes sont vendues en plusieurs lots
            num_sales = random.randint(1, 3)
            remaining_quantity = harvest['quantity_tonnes']
            
            for _ in range(num_sales):
                if remaining_quantity <= 0:
                    break
                
                quantity_sold = round(random.uniform(0.3, 0.8) * remaining_quantity, 2)
                remaining_quantity -= quantity_sold
                
                # Prix avec variation selon la qualité
                base_price = harvest['estimated_price_per_tonne']
                if harvest['quality_grade'] == 'A':
                    price = base_price * random.uniform(1.0, 1.2)
                elif harvest['quality_grade'] == 'B':
                    price = base_price * random.uniform(0.9, 1.0)
                else:
                    price = base_price * random.uniform(0.7, 0.9)
                
                sale_date = datetime.fromisoformat(harvest['harvest_date']) + timedelta(days=random.randint(1, 30))
                
                sale = {
                    'sale_id': f'SALE-{sale_id:06d}',
                    'harvest_id': harvest['harvest_id'],
                    'sale_date': sale_date.isoformat(),
                    'quantity_tonnes': quantity_sold,
                    'price_per_tonne': round(price, 2),
                    'total_amount': round(quantity_sold * price, 2),
                    'buyer': random.choice(buyers),
                    'sale_type': random.choice(sale_types),
                    'payment_status': random.choice(['paid', 'pending', 'partial']),
                }
                self.sales.append(sale)
                sale_id += 1
        
        return self.sales
    
    def save_all_data(self):
        """Sauvegarder toutes les données générées"""
        print(f"\n   💾 Sauvegarde des données...")
        
        datasets = {
            'farms': self.farms,
            'fields': self.fields,
            'crops': self.crops,
            'harvests': self.harvests,
            'weather': self.weather_data,
            'sensors': self.sensor_data,
            'sales': self.sales,
        }
        
        for name, data in datasets.items():
            if data:
                filepath = self.output_dir / f'{name}.json'
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                print(f"   ✅ {name}: {len(data)} enregistrements → {filepath}")
    
    def generate_all(self, num_farms: int = 20, fields_per_farm: int = 5, 
                     weather_days: int = 90, sensor_readings: int = 100):
        """Générer toutes les données"""
        print("\n" + "=" * 70)
        print("🌾 GÉNÉRATION DE DONNÉES AGRICOLES")
        print("=" * 70)
        
        self.generate_farms(num_farms)
        self.generate_fields(fields_per_farm)
        self.generate_crops()
        self.generate_harvests()
        self.generate_weather_data(weather_days)
        self.generate_sensor_data(sensor_readings)
        self.generate_sales()
        self.save_all_data()
        
        print("\n" + "=" * 70)
        print("✅ GÉNÉRATION TERMINÉE")
        print("=" * 70)
        print(f"\n📊 Résumé:")
        print(f"   - Exploitations: {len(self.farms)}")
        print(f"   - Parcelles: {len(self.fields)}")
        print(f"   - Cultures: {len(self.crops)}")
        print(f"   - Récoltes: {len(self.harvests)}")
        print(f"   - Données météo: {len(self.weather_data)} jours")
        print(f"   - Lectures capteurs: {len(self.sensor_data)}")
        print(f"   - Ventes: {len(self.sales)}")
        
        # Statistiques
        total_area = sum(f['total_area_ha'] for f in self.farms)
        total_harvest = sum(h['quantity_tonnes'] for h in self.harvests)
        total_revenue = sum(s['total_amount'] for s in self.sales)
        
        print(f"\n💰 Statistiques:")
        print(f"   - Surface totale: {total_area:.2f} hectares")
        print(f"   - Production totale: {total_harvest:.2f} tonnes")
        print(f"   - Revenus totaux: {total_revenue:,.2f} FCFA")
        print("=" * 70)


if __name__ == '__main__':
    generator = AgriculturalDataGenerator()
    generator.generate_all(
        num_farms=20,
        fields_per_farm=5,
        weather_days=90,
        sensor_readings=100
    )

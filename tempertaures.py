import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
import json
import os
from datetime import datetime

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def download_kaggle_data():
    """Descarga solo los archivos circuits.csv y races.csv de Kaggle"""
    print("Descargando datos de Kaggle...")
    
    # Crear directorio para datos si no existe
    os.makedirs('f1_data', exist_ok=True)
    
    # Descargar solo races.csv y circuits.csv
    if not os.path.exists('f1_data/races.csv'):
        print("Descargando races.csv...")
        os.system('kaggle datasets download -d jtrotman/formula-1-race-data -f races.csv -p f1_data --unzip')
    
    if not os.path.exists('f1_data/circuits.csv'):
        print("Descargando circuits.csv...")
        os.system('kaggle datasets download -d jtrotman/formula-1-race-data -f circuits.csv -p f1_data --unzip')
    
    if os.path.exists('f1_data/races.csv') and os.path.exists('f1_data/circuits.csv'):
        print("✓ Archivos descargados exitosamente")
    else:
        print("Los datos ya existen localmente")

def load_race_data():
    """Carga las carreras filtradas por raceId"""
    print("\nCargando datos de carreras...")
    races_df = pd.read_csv('f1_data/races.csv')
    
    # Filtrar carreras con raceId entre 948 y 1167
    filtered_races = races_df[(races_df['raceId'] >= 948) & (races_df['raceId'] <= 1167)]
    
    print(f"Carreras encontradas: {len(filtered_races)}")
    return filtered_races[['raceId', 'circuitId', 'date', 'name']]

def load_circuit_data():
    """Carga datos de circuitos"""
    print("\nCargando datos de circuitos...")
    circuits_df = pd.read_csv('f1_data/circuits.csv')
    return circuits_df[['circuitId', 'lat', 'lng', 'name', 'location', 'country']]

def get_weather_data(latitude, longitude, date):
    """Obtiene datos meteorológicos de Open-Meteo para una fecha y ubicación específica"""
    try:
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": date,
            "end_date": date,
            "hourly": ["wind_speed_100m", "temperature_2m", "relative_humidity_2m", 
                      "precipitation", "rain", "pressure_msl", "surface_pressure"],
        }
        
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        
        # Process hourly data
        hourly = response.Hourly()
        hourly_wind_speed_100m = hourly.Variables(0).ValuesAsNumpy()
        hourly_temperature_2m = hourly.Variables(1).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(2).ValuesAsNumpy()
        hourly_precipitation = hourly.Variables(3).ValuesAsNumpy()
        hourly_rain = hourly.Variables(4).ValuesAsNumpy()
        hourly_pressure_msl = hourly.Variables(5).ValuesAsNumpy()
        hourly_surface_pressure = hourly.Variables(6).ValuesAsNumpy()
        
        # Calcular promedios diarios
        weather_summary = {
            'avg_wind_speed_100m': float(hourly_wind_speed_100m.mean()) if len(hourly_wind_speed_100m) > 0 else None,
            'max_wind_speed_100m': float(hourly_wind_speed_100m.max()) if len(hourly_wind_speed_100m) > 0 else None,
            'avg_temperature_2m': float(hourly_temperature_2m.mean()) if len(hourly_temperature_2m) > 0 else None,
            'max_temperature_2m': float(hourly_temperature_2m.max()) if len(hourly_temperature_2m) > 0 else None,
            'min_temperature_2m': float(hourly_temperature_2m.min()) if len(hourly_temperature_2m) > 0 else None,
            'avg_humidity': float(hourly_relative_humidity_2m.mean()) if len(hourly_relative_humidity_2m) > 0 else None,
            'total_precipitation': float(hourly_precipitation.sum()) if len(hourly_precipitation) > 0 else None,
            'total_rain': float(hourly_rain.sum()) if len(hourly_rain) > 0 else None,
            'avg_pressure_msl': float(hourly_pressure_msl.mean()) if len(hourly_pressure_msl) > 0 else None,
            'avg_surface_pressure': float(hourly_surface_pressure.mean()) if len(hourly_surface_pressure) > 0 else None,
        }
        
        return weather_summary
    
    except Exception as e:
        print(f"Error obteniendo datos meteorológicos: {e}")
        return None

def main():
    """Función principal que ejecuta todo el proceso"""
    print("=== Iniciando proceso de recolección de datos F1 ===\n")
    
    # Paso 1: Descargar datos de Kaggle
    download_kaggle_data()
    
    # Paso 2: Cargar carreras filtradas
    races = load_race_data()
    
    # Paso 3: Cargar datos de circuitos
    circuits = load_circuit_data()
    
    # Paso 4: Combinar datos de carreras y circuitos
    print("\nCombinando datos de carreras y circuitos...")
    merged_data = races.merge(circuits, on='circuitId', suffixes=('_race', '_circuit'))
    
    # Paso 5: Obtener datos meteorológicos para cada carrera
    print("\nObteniendo datos meteorológicos...")
    all_race_data = []
    
    for idx, row in merged_data.iterrows():
        print(f"Procesando carrera {idx+1}/{len(merged_data)}: {row['name_race']} - {row['date']}")
        
        weather = get_weather_data(row['lat'], row['lng'], row['date'])
        
        race_info = {
            'raceId': int(row['raceId']),
            'circuitId': int(row['circuitId']),
            'latitude': float(row['lat']),
            'longitude': float(row['lng']),
        }
        
        if weather:
            race_info.update(weather)
        
        all_race_data.append(race_info)
    
    # Paso 6: Crear DataFrame y exportar
    print("\nGenerando archivos de salida...")
    results_df = pd.DataFrame(all_race_data)
    
    # Exportar a CSV
    results_df.to_csv('f1_weather_data.csv', index=False, encoding='utf-8')
    print("✓ Datos guardados en 'f1_weather_data.csv'")
    
    # Exportar a JSON
    results_df.to_json('f1_weather_data.json', orient='records', indent=2)
    print("✓ Datos guardados en 'f1_weather_data.json'")
    
    print(f"\n=== Proceso completado ===")
    print(f"Total de carreras procesadas: {len(all_race_data)}")

if __name__ == "__main__":
    main()
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

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
                      "precipitation", "pressure_msl", "surface_pressure"],
        }
        
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        
        # Process hourly data
        hourly = response.Hourly()
        hourly_wind_speed_100m = hourly.Variables(0).ValuesAsNumpy()
        hourly_temperature_2m = hourly.Variables(1).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(2).ValuesAsNumpy()
        hourly_precipitation = hourly.Variables(3).ValuesAsNumpy()
        hourly_pressure_msl = hourly.Variables(4).ValuesAsNumpy()
        hourly_surface_pressure = hourly.Variables(5).ValuesAsNumpy()
        
        # Calcular promedios diarios
        weather_summary = {
            'AVG WIND SPEED': round(float(hourly_wind_speed_100m.mean()), 2) if len(hourly_wind_speed_100m) > 0 else None,
            'MAX WIND SPEED': round(float(hourly_wind_speed_100m.max()), 2) if len(hourly_wind_speed_100m) > 0 else None,
            'AVG TEMPERATURE': round(float(hourly_temperature_2m.mean()), 2) if len(hourly_temperature_2m) > 0 else None,
            'MAX TEMPERATURE': round(float(hourly_temperature_2m.max()), 2) if len(hourly_temperature_2m) > 0 else None,
            'MIN TEMPERATURE': round(float(hourly_temperature_2m.min()), 2) if len(hourly_temperature_2m) > 0 else None,
            'AVG HUMIDITY': round(float(hourly_relative_humidity_2m.mean()), 2) if len(hourly_relative_humidity_2m) > 0 else None,
            'PRECIPITATION': round(float(hourly_precipitation.sum()), 2) if len(hourly_precipitation) > 0 else None,
            'AVG PRESSURE MSL': round(float(hourly_pressure_msl.mean()), 2) if len(hourly_pressure_msl) > 0 else None,
            'AVG SURFACE PRESSURE': round(float(hourly_surface_pressure.mean()), 2) if len(hourly_surface_pressure) > 0 else None,
        }
        
        return weather_summary
    
    except Exception as e:
        print(f"Error obteniendo datos meteorológicos: {e}")
        return None

def generar_dataset_carrera_1168():
    """
    Genera el dataset para la carrera 1168 (última carrera de la temporada) con datos meteorológicos.
    """
    
    # --- 1. Definición de Archivos y Delimitadores ---
    RESULTS_FILE = 'f1_data/results.csv'
    SPRINT_RESULTS_FILE = 'f1_data/sprint_results.csv'
    QUALIFYING_FILE = 'f1_data/qualifying.csv'
    RACES_FILE = 'f1_data/races.csv'
    CIRCUITS_FILE = 'f1_data/circuits.csv'
    DRIVERS_FILE = 'f1_data/drivers.csv'
    DRIVER_STANDINGS_FILE = 'f1_data/driver_standings.csv'
    CONSTRUCTOR_STANDINGS_FILE = 'f1_data/constructor_standings.csv'
    STATUS_FILE = 'f1_data/status.csv'

    COMMON_DELIMITER = ','
    RACE_ID = 1168
    PROCESS_FROM_YEAR = 2001
    
    OUTPUT_FILE = f'f1_race_{RACE_ID}_data.csv'

    # Columnas finales requeridas
    COLUMNAS_FINALES = [
        'RACEID', 'DRIVERID', 'CONSTRUCTORID', 'CIRCUITID', 'ROUND', 'YEAR', 'LAP DISTANCE KM', 'LAPS RACE', 'URBAN',
        'AVG WIND SPEED', 'MAX WIND SPEED', 'AVG TEMPERATURE', 'MIN TEMPERATURE', 'MAX TEMPERATURE',
        'AVG HUMIDITY', 'PRECIPITATION', 'AVG PRESSURE MSL', 'AVG SURFACE PRESSURE',
        'DRIVER LAST POSITION', 'WINS SEASON', 'WINS CAREER', 'POINTS BEFORE GP', 'YEARS OF EXPERIENCE', 'AGE', 
        'MATE LAST POSITION',
        'CONSTRUCTOR POINTS BEFORE GP', 'CONSTRUCTOR WINS SEASON',
        'Q1', 'Q2', 'Q3', 'BEST Q', 'GRID',
        'Q1 VALID', 'Q2 VALID', 'Q3 VALID', 'RACE VALID',
        'SPRINT Y/N'
    ]

    print(f"=== Iniciando generación de datos para carrera {RACE_ID} ===\n")

    # --- 2. Cargar información de la carrera 1168 ---
    races_df = pd.read_csv(RACES_FILE, sep=COMMON_DELIMITER)
    
    race_1168 = races_df[races_df['raceId'] == RACE_ID].iloc[0]
    
    circuit_id = race_1168['circuitId']
    race_date = race_1168['date']
    race_year = race_1168['year']
    race_round = race_1168['round']
    
    print(f"Carrera {RACE_ID}: {race_1168['name']}")
    print(f"Circuito ID: {circuit_id}, Fecha: {race_date}, Año: {race_year}, Ronda: {race_round}\n")
    
    # --- 3. Obtener datos meteorológicos ---
    circuits_df = pd.read_csv(CIRCUITS_FILE, sep=COMMON_DELIMITER)
    circuit_info = circuits_df[circuits_df['circuitId'] == circuit_id].iloc[0]
    
    lat = circuit_info['lat']
    lng = circuit_info['lng']
    lap_distance = circuit_info['lap_distance_km']
    urban = circuit_info['urban']
    
    print(f"Obteniendo datos meteorológicos para {circuit_info['name']}...")
    print(f"Coordenadas: {lat}, {lng}")
    
    weather_data = get_weather_data(lat, lng, race_date)
    
    if weather_data:
        print("✓ Datos meteorológicos obtenidos exitosamente\n")
    else:
        print("⚠ No se pudieron obtener datos meteorológicos\n")
        weather_data = {}
    
    # --- 4. Cargar y Preparar DataFrames ---
    print("Cargando datos de F1...")
    
    # a) results.csv (Carrera Principal)
    results_df = pd.read_csv(RESULTS_FILE, sep=COMMON_DELIMITER)
    results_df = results_df[['raceId', 'driverId', 'constructorId', 'grid', 'milliseconds', 'statusId', 'position', 'laps']].copy()
    results_df.rename(columns={
        'grid': 'GRID',
        'milliseconds': 'MS RACE',
        'statusId': 'STATUS RACE'
    }, inplace=True)

    # b) sprint_results.csv (Carrera Sprint)
    sprint_df = pd.read_csv(SPRINT_RESULTS_FILE, sep=COMMON_DELIMITER)
    sprint_df = sprint_df[['raceId', 'driverId', 'constructorId', 'milliseconds', 'statusId']].copy()
    sprint_df.rename(columns={
        'milliseconds': 'MS SPRINT',
        'statusId': 'STATUS SPRINT'
    }, inplace=True)

    # c) qualifying.csv (Calificación)
    qualifying_df = pd.read_csv(QUALIFYING_FILE, sep=COMMON_DELIMITER)
    qualifying_df = qualifying_df[['raceId', 'driverId', 'constructorId', 'q1', 'q2', 'q3']].copy()

    # d) races.csv (Información de carreras)
    races_df['DATE'] = pd.to_datetime(races_df['date'])
    races_df = races_df[races_df['year'] >= PROCESS_FROM_YEAR].copy()
    recent_race_ids = set(races_df['raceId'].unique())

    # e) drivers.csv (Información de pilotos)
    drivers_df = pd.read_csv(DRIVERS_FILE, sep=COMMON_DELIMITER)
    drivers_df = drivers_df[['driverId', 'dob']].copy()
    drivers_df.rename(columns={'dob': 'DOB'}, inplace=True)
    drivers_df['DOB'] = pd.to_datetime(drivers_df['DOB'])
    
    # f) driver_standings.csv (Campeonato de pilotos)
    driver_standings_df = pd.read_csv(DRIVER_STANDINGS_FILE, sep=COMMON_DELIMITER, na_values=['\\N'])
    driver_standings_df = driver_standings_df[driver_standings_df['raceId'].isin(recent_race_ids)]
    driver_standings_df = driver_standings_df[['raceId', 'driverId', 'points', 'position']].copy()
    driver_standings_df.rename(columns={
        'points': 'POINTS STANDINGS',
        'position': 'POSITION STANDINGS'
    }, inplace=True)

    # g) constructor_standings.csv
    constructor_standings_df = pd.read_csv(CONSTRUCTOR_STANDINGS_FILE, sep=COMMON_DELIMITER, na_values=['\\N'])
    constructor_standings_df = constructor_standings_df[constructor_standings_df['raceId'].isin(recent_race_ids)]
    constructor_standings_df = constructor_standings_df[['raceId', 'constructorId', 'points']].copy()
    constructor_standings_df.rename(columns={'points': 'CONSTRUCTOR POINTS'}, inplace=True)

    # h) status.csv
    status_df = pd.read_csv(STATUS_FILE, sep=COMMON_DELIMITER)
    status_dict = status_df.set_index('statusId')['status'].to_dict()

    # --- 5. Fusiones para la carrera 1168 (sin resultados) ---
    print("Procesando datos de la carrera...")
    
    # Como la carrera 1168 no tiene resultados, solo usamos qualifying
    # Cargar pilotos que participarán (desde qualifying)
    merged_df = qualifying_df[qualifying_df['raceId'] == RACE_ID].copy()

    # Fusión con Información de Pilotos
    merged_df = pd.merge(
        merged_df,
        drivers_df,
        on='driverId',
        how='left'
    )
    
    # Fusión con Driver Standings de la carrera ANTERIOR (1167)
    merged_df = pd.merge(
        merged_df,
        driver_standings_df[driver_standings_df['raceId'] == RACE_ID - 1],
        on='driverId',
        how='left',
        suffixes=('', '_prev')
    )
    
    # Fusión con Constructor Standings de la carrera ANTERIOR (1167)
    merged_df = pd.merge(
        merged_df,
        constructor_standings_df[constructor_standings_df['raceId'] == RACE_ID - 1],
        on='constructorId',
        how='left',
        suffixes=('', '_const')
    )

    # --- 6. Transformaciones ---
    
    # Verificar si hubo sprint (aunque probablemente no haya datos)
    sprint_check = sprint_df[sprint_df['raceId'] == RACE_ID]
    merged_df['SPRINT Y/N'] = 1 if not sprint_check.empty else 0
    
    # Función para convertir tiempo MM:SS.SSS a milisegundos
    def time_to_milliseconds(time_str):
        if pd.isna(time_str):
            return None
        try:
            parts = str(time_str).split(':')
            minutes = int(parts[0])
            seconds = float(parts[1])
            return int(minutes * 60 * 1000 + seconds * 1000)
        except:
            return None
    
    # Convertir Q1, Q2, Q3 a milisegundos
    merged_df['q1'] = merged_df['q1'].apply(time_to_milliseconds)
    merged_df['q2'] = merged_df['q2'].apply(time_to_milliseconds)
    merged_df['q3'] = merged_df['q3'].apply(time_to_milliseconds)
    
    # Sustituir valores nulos por 300000 (penalización por no clasificar)
    merged_df['q1'] = merged_df['q1'].fillna(300000)
    merged_df['q2'] = merged_df['q2'].fillna(300000)
    merged_df['q3'] = merged_df['q3'].fillna(300000)
    
    # Renombrar IDs y Qs a mayúsculas
    merged_df.rename(columns={
        'raceId': 'RACEID',
        'driverId': 'DRIVERID',
        'constructorId': 'CONSTRUCTORID',
        'q1': 'Q1',
        'q2': 'Q2',
        'q3': 'Q3'
    }, inplace=True)
    
    # Calcular BEST Q (el menor tiempo entre Q1, Q2, Q3)
    merged_df['BEST Q'] = merged_df[['Q1', 'Q2', 'Q3']].min(axis=1)
    
    # Calcular GRID desde qualifying (ordenar por BEST Q)
    merged_df['GRID'] = merged_df['BEST Q'].rank(method='min').astype(int)
    merged_df['GRID'] = merged_df['GRID'].replace(0, 20)
    
    # --- 7. Calcular columnas sin necesitar resultados de la carrera 1168 ---
    
    # Cargar resultados históricos (excluyendo la carrera 1168)
    results_full = pd.read_csv(RESULTS_FILE, sep=COMMON_DELIMITER)
    results_full = results_full[results_full['raceId'].isin(recent_race_ids)]
    results_full = results_full[results_full['raceId'] < RACE_ID]  # Solo carreras anteriores
    results_full['position'] = pd.to_numeric(results_full['position'], errors='coerce')

    # LAPS RACE: Obtener de una carrera reciente en el mismo circuito o usar promedio
    previous_race_same_circuit = results_full[results_full['raceId'] == RACE_ID - 1]
    if not previous_race_same_circuit.empty:
        winner_prev = previous_race_same_circuit[previous_race_same_circuit['position'] == 1]
        if not winner_prev.empty:
            laps_race = winner_prev['laps'].iloc[0]
        else:
            laps_race = 58  # Valor por defecto típico
    else:
        laps_race = 58  # Valor por defecto típico
    
    merged_df['LAPS RACE'] = laps_race
    
    # Columnas relacionadas con resultados: se rellenan con valores por defecto
    # ya que no tenemos resultados de esta carrera (es para predicción)
    merged_df['RACE VALID'] = 1
    
    # Crear columnas binarias para validez
    merged_df['Q1 VALID'] = (merged_df['Q1'] != 300000).astype(int)
    merged_df['Q2 VALID'] = (merged_df['Q2'] != 300000).astype(int)
    merged_df['Q3 VALID'] = (merged_df['Q3'] != 300000).astype(int)
    
    # Crear tabla de información de carreras
    race_info = pd.read_csv(RACES_FILE, sep=COMMON_DELIMITER)[['raceId', 'year']].copy()
    race_info = race_info[race_info['year'] >= PROCESS_FROM_YEAR]
    race_info = race_info.sort_values('raceId').reset_index(drop=True)
    race_info['RACE_ORDER'] = range(len(race_info))
    
    # AGE: Calcular edad del piloto en la fecha de la carrera
    race_date_dt = pd.to_datetime(race_date)
    merged_df['AGE'] = ((race_date_dt - merged_df['DOB']).dt.days / 365.25).astype(int)
    
    # Renombrar columnas
    merged_df.rename(columns={
        'raceId': 'RACEID',
        'driverId': 'DRIVERID',
        'constructorId': 'CONSTRUCTORID'
    }, inplace=True)
    
    # DRIVER LAST POSITION: Posición del piloto en la carrera anterior (1167)
    previous_race_results = results_full[results_full['raceId'] == RACE_ID - 1][['driverId', 'position']].copy()
    previous_race_results.rename(columns={'position': 'DRIVER LAST POSITION'}, inplace=True)
    previous_race_results['DRIVER LAST POSITION'] = pd.to_numeric(previous_race_results['DRIVER LAST POSITION'], errors='coerce')
    
    merged_df = merged_df.merge(previous_race_results, left_on='DRIVERID', right_on='driverId', how='left', suffixes=('', '_drop'))
    merged_df['DRIVER LAST POSITION'] = merged_df['DRIVER LAST POSITION'].fillna(21).astype(int)
    
    # POINTS BEFORE GP: Usar los standings de la carrera anterior ya fusionados
    merged_df['POINTS BEFORE GP'] = merged_df['POINTS STANDINGS'].fillna(0)
    
    # YEARS OF EXPERIENCE: Años desde el debut
    first_race = results_full[['raceId', 'driverId']].copy()
    first_race = first_race.merge(race_info[['raceId', 'year']], on='raceId', how='left')
    first_race = first_race.drop_duplicates('driverId', keep='first').rename(columns={'year': 'DEBUT_YEAR'})
    
    merged_df = merged_df.merge(first_race[['driverId', 'DEBUT_YEAR']], left_on='DRIVERID', right_on='driverId', how='left', suffixes=('', '_debut'))
    merged_df['YEARS OF EXPERIENCE'] = (race_year - merged_df['DEBUT_YEAR']).fillna(0).astype(int)
    
    # WINS SEASON y WINS CAREER
    wins_by_race = results_full[results_full['position'] == 1][['raceId', 'driverId']].copy()
    wins_by_race = wins_by_race.merge(race_info, on='raceId', how='left')
    
    current_race_order = race_info[race_info['raceId'] == RACE_ID]['RACE_ORDER'].iloc[0]
    
    def calc_wins(driver_id):
        wins_career = wins_by_race[(wins_by_race['driverId'] == driver_id) & (wins_by_race['RACE_ORDER'] < current_race_order)].shape[0]
        wins_season = wins_by_race[(wins_by_race['driverId'] == driver_id) & (wins_by_race['RACE_ORDER'] < current_race_order) & (wins_by_race['year'] == race_year)].shape[0]
        return wins_career, wins_season
    
    wins_data = merged_df['DRIVERID'].apply(lambda x: calc_wins(x)).apply(pd.Series)
    merged_df['WINS CAREER'] = wins_data[0].fillna(0).astype(int)
    merged_df['WINS SEASON'] = wins_data[1].fillna(0).astype(int)
    
    # CONSTRUCTOR POINTS BEFORE GP
    previous_constructor_standings = constructor_standings_df[constructor_standings_df['raceId'] == RACE_ID - 1][['constructorId', 'CONSTRUCTOR POINTS']].copy()
    previous_constructor_standings.rename(columns={'CONSTRUCTOR POINTS': 'CONSTRUCTOR POINTS BEFORE GP'}, inplace=True)
    
    merged_df = merged_df.merge(previous_constructor_standings, left_on='CONSTRUCTORID', right_on='constructorId', how='left', suffixes=('', '_const'))
    merged_df['CONSTRUCTOR POINTS BEFORE GP'] = merged_df['CONSTRUCTOR POINTS BEFORE GP'].fillna(0)
    
    # CONSTRUCTOR WINS SEASON
    constructor_wins = results_full[results_full['position'] == 1][['raceId', 'constructorId']].copy()
    constructor_wins = constructor_wins.merge(race_info, on='raceId', how='left')
    
    def calc_constructor_wins_season(constructor_id):
        wins_season = constructor_wins[(constructor_wins['constructorId'] == constructor_id) & 
                                      (constructor_wins['RACE_ORDER'] < current_race_order) & 
                                      (constructor_wins['year'] == race_year)].shape[0]
        return wins_season
    
    merged_df['CONSTRUCTOR WINS SEASON'] = merged_df['CONSTRUCTORID'].apply(calc_constructor_wins_season).fillna(0).astype(int)
    
    # MATE LAST POSITION
    mate_positions = []
    
    for idx, row in merged_df.iterrows():
        driver_id = row['DRIVERID']
        constructor_id = row['CONSTRUCTORID']
        
        # Buscar al compañero de equipo en la carrera anterior (1167)
        mate_prev = results_full[
            (results_full['raceId'] == RACE_ID - 1) &
            (results_full['constructorId'] == constructor_id) &
            (results_full['driverId'] != driver_id)
        ]
        
        if mate_prev.empty:
            mate_positions.append(21)
        else:
            mate_last_pos = mate_prev['position'].iloc[0]
            mate_positions.append(int(mate_last_pos) if pd.notna(mate_last_pos) else 21)
    
    merged_df['MATE LAST POSITION'] = mate_positions
    
    # --- 8. Añadir información constante de la carrera ---
    merged_df['CIRCUITID'] = circuit_id
    merged_df['ROUND'] = race_round
    merged_df['YEAR'] = race_year - 2025  # Transformar año
    merged_df['LAP DISTANCE KM'] = lap_distance
    merged_df['URBAN'] = urban
    
    # Añadir datos meteorológicos
    for key, value in weather_data.items():
        merged_df[key] = value if value is not None else 0
    
    # Sustituir GRID = 0 por 20
    merged_df['GRID'] = merged_df['GRID'].replace(0, 20)
    
    # --- 9. Seleccionar y reordenar las columnas finales ---
    final_df = merged_df[COLUMNAS_FINALES].copy()
    
    # --- 10. Guardar el Resultado Final ---
    print(f"\nGuardando el dataset en {OUTPUT_FILE}...")
    final_df.to_csv(OUTPUT_FILE, index=False)

    print("\n✅ ¡Proceso completado!")
    print(f"Dataset generado con {final_df.shape[0]} filas y {final_df.shape[1]} columnas.")

# Ejecutar la función principal
if __name__ == "__main__":
    generar_dataset_carrera_1168()

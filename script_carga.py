import pandas as pd

def generar_dataset_f1_completo(min_year=2016):
    """
    Carga, fusiona, reorganiza y filtra los datos de F1 a partir de un año específico.

    Parámetros:
        min_year (int): El año mínimo (inclusive) para el filtrado de carreras.
    """
    # --- 1. Definición de Archivos y Delimitadores ---
    RESULTS_FILE = 'results.csv'
    SPRINT_RESULTS_FILE = 'sprint_results.csv'
    QUALIFYING_FILE = 'qualifying.csv'
    RACES_FILE = 'races.csv'

    COMMON_DELIMITER = ','
    # Año mínimo de datos que se cargan para todos los cálculos (reduce volumen)
    PROCESS_FROM_YEAR = 2001
    
    OUTPUT_FILE = f'f1_training_data_{min_year}_onwards.csv'

    # Columnas finales requeridas
    COLUMNAS_FINALES = [
        'RACEID', 'DRIVERID', 'CONSTRUCTORID', 'CIRCUITID', 'ROUND', 'YEAR', 'LAP DISTANCE KM', 'LAPS RACE', 
        'AVG WIND SPEED', 'MAX WIND SPEED', 'AVG TEMPERATURE', 'MIN TEMPERATURE', 'MAX TEMPERATURE',
        'AVG HUMIDITY', 'PRECIPITATION', 'AVG PRESSURE MSL', 'AVG SURFACE PRESSURE',
        'DRIVER LAST POSITION', 'WINS SEASON', 'WINS CAREER', 'POINTS BEFORE GP', 'YEARS OF EXPERIENCE', 'AGE', 
        'MATE LAST POSITION',
        'CONSTRUCTOR POINTS BEFORE GP', 'CONSTRUCTOR WINS SEASON',
        'Q1', 'Q2', 'Q3', 'BEST Q', 'GRID',
        'SPRINT Y/N', 'MS RACE'
    ]

    # --- 2. Cargar y Preparar DataFrames ---

    print("Iniciando la carga y preparación de datos...")

    # a) results.csv (Carrera Principal)
    results_df = pd.read_csv(RESULTS_FILE, sep=COMMON_DELIMITER)
    results_df = results_df[['raceId', 'driverId', 'constructorId', 'grid', 'milliseconds', 'statusId', 'position']].copy()
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
    races_df = pd.read_csv(RACES_FILE, sep=COMMON_DELIMITER)
    races_df = races_df[['raceId', 'circuitId', 'round', 'year', 'date']].copy()
    races_df.rename(columns={'round': 'ROUND', 'year': 'YEAR', 'date': 'DATE', 'circuitId': 'CIRCUITID'}, inplace=True)
    # Convertir DATE a datetime para cálculos de edad
    races_df['DATE'] = pd.to_datetime(races_df['DATE'])
    # Filtrar lo más pronto posible
    races_df = races_df[races_df['YEAR'] >= PROCESS_FROM_YEAR].copy()
    recent_race_ids = set(races_df['raceId'].unique())

    # g) circuits.csv (añadir distancia por vuelta)
    circuits_df = pd.read_csv('circuits.csv', sep=COMMON_DELIMITER)
    circuits_df = circuits_df[['circuitId', 'lap_distance_km']].copy()
    circuits_df.rename(columns={'circuitId': 'CIRCUITID', 'lap_distance_km': 'LAP DISTANCE KM'}, inplace=True)
    
    # h) f1_weather_data.csv (añadir datos meteorológicos)
    weather_df = pd.read_csv('f1_weather_data.csv', sep=COMMON_DELIMITER)
    weather_df = weather_df[[
        'raceId', 'avg_wind_speed_100m', 'max_wind_speed_100m', 'avg_temperature_2m', 
        'min_temperature_2m', 'max_temperature_2m', 'avg_humidity', 'total_precipitation',
        'avg_pressure_msl', 'avg_surface_pressure'
    ]].copy()
    # Redondear a 2 decimales
    weather_df['avg_wind_speed_100m'] = weather_df['avg_wind_speed_100m'].round(2)
    weather_df['max_wind_speed_100m'] = weather_df['max_wind_speed_100m'].round(2)
    weather_df['avg_temperature_2m'] = weather_df['avg_temperature_2m'].round(2)
    weather_df['min_temperature_2m'] = weather_df['min_temperature_2m'].round(2)
    weather_df['max_temperature_2m'] = weather_df['max_temperature_2m'].round(2)
    weather_df['avg_humidity'] = weather_df['avg_humidity'].round(2)
    weather_df['total_precipitation'] = weather_df['total_precipitation'].round(2)
    weather_df['avg_pressure_msl'] = weather_df['avg_pressure_msl'].round(2)
    weather_df['avg_surface_pressure'] = weather_df['avg_surface_pressure'].round(2)
    weather_df.rename(columns={
        'avg_wind_speed_100m': 'AVG WIND SPEED',
        'max_wind_speed_100m': 'MAX WIND SPEED',
        'avg_temperature_2m': 'AVG TEMPERATURE',
        'min_temperature_2m': 'MIN TEMPERATURE',
        'max_temperature_2m': 'MAX TEMPERATURE',
        'avg_humidity': 'AVG HUMIDITY',
        'total_precipitation': 'PRECIPITATION',
        'avg_pressure_msl': 'AVG PRESSURE MSL',
        'avg_surface_pressure': 'AVG SURFACE PRESSURE'
    }, inplace=True)
    
    # e) drivers.csv (Información de pilotos)
    drivers_df = pd.read_csv('drivers.csv', sep=COMMON_DELIMITER)
    drivers_df = drivers_df[['driverId', 'dob']].copy()
    drivers_df.rename(columns={'dob': 'DOB'}, inplace=True)
    drivers_df['DOB'] = pd.to_datetime(drivers_df['DOB'])
    
    # f) driver_standings.csv (Campeonato de pilotos)
    driver_standings_df = pd.read_csv('driver_standings.csv', sep=COMMON_DELIMITER, na_values=['\\N'])
    driver_standings_df = driver_standings_df[driver_standings_df['raceId'].isin(recent_race_ids)]
    driver_standings_df = driver_standings_df[['raceId', 'driverId', 'points', 'position']].copy()
    driver_standings_df.rename(columns={
        'points': 'POINTS STANDINGS',
        'position': 'POSITION STANDINGS'
    }, inplace=True)


    # --- 3. Realizar las Fusiones (JOINs) ---

    print("Realizando fusiones de datos...")

    # Fusión 1: Resultados de Carrera y Sprint (Full Outer Join)
    merged_df = pd.merge(
        results_df[results_df['raceId'].isin(recent_race_ids)],
        sprint_df[sprint_df['raceId'].isin(recent_race_ids)],
        on=['raceId', 'driverId', 'constructorId'],
        how='outer'
    )

    # Fusión 2: con Calificación (Left Join)
    merged_df = pd.merge(
        merged_df,
        qualifying_df[qualifying_df['raceId'].isin(recent_race_ids)],
        on=['raceId', 'driverId', 'constructorId'],
        how='left'
    )

    # Fusión 3: con Fechas (Left Join)
    merged_df = pd.merge(
        merged_df,
        races_df,
        on='raceId',
        how='left'
    )

    # Añadir distancia de vuelta por circuito
    merged_df = pd.merge(
        merged_df,
        circuits_df,
        on='CIRCUITID',
        how='left'
    )
    
    # Fusión 6: con Datos Meteorológicos (Left Join)
    merged_df = pd.merge(
        merged_df,
        weather_df,
        on='raceId',
        how='left'
    )
    
    # Fusión 4: con Información de Pilotos (Left Join)
    merged_df = pd.merge(
        merged_df,
        drivers_df,
        on='driverId',
        how='left'
    )
    
    # Fusión 5: con Driver Standings (Left Join)
    merged_df = pd.merge(
        merged_df,
        driver_standings_df,
        on=['raceId', 'driverId'],
        how='left'
    )

    # --- 4. Transformaciones y Filtrado Finales ---

    # Crear la columna 'SPRINT Y/N' (1 si hubo sprint, 0 si no)
    merged_df['SPRINT Y/N'] = merged_df['MS SPRINT'].notna().astype(int)
    
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
    
    # Convertir 'DATE' a datetime
    merged_df['DATE'] = pd.to_datetime(merged_df['DATE'])
    
    # Renombrar IDs y Qs a mayúsculas primero
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
    
    # --- 5. Calcular nuevas columnas ---
    
    # Cargar todas las carreras con resultados para cálculos históricos
    results_full = pd.read_csv(RESULTS_FILE, sep=COMMON_DELIMITER)
    results_full = results_full[results_full['raceId'].isin(recent_race_ids)]
    results_full['position'] = pd.to_numeric(results_full['position'], errors='coerce')

    # LAPS RACE: número de vueltas completadas por el ganador (referencia de la carrera)
    laps_per_race = results_full[results_full['position'] == 1][['raceId', 'laps']].copy()
    laps_per_race.rename(columns={'raceId': 'RACEID', 'laps': 'LAPS RACE'}, inplace=True)

    # Añadir vueltas totales de la carrera (ganador)
    merged_df = merged_df.merge(laps_per_race, on='RACEID', how='left')
    
    # Cargar status para identificar pilotos con +N laps
    status_df = pd.read_csv('status.csv', sep=COMMON_DELIMITER)
    status_dict = status_df.set_index('statusId')['status'].to_dict()
    
    # Obtener tiempos del ganador por carrera
    winner_times = results_full[results_full['position'] == 1][['raceId', 'milliseconds']].copy()
    winner_times['milliseconds'] = pd.to_numeric(winner_times['milliseconds'], errors='coerce')
    winner_times.rename(columns={'raceId': 'RACEID', 'milliseconds': 'WINNER_TIME'}, inplace=True)
    merged_df = merged_df.merge(winner_times, on='RACEID', how='left')
    
    # Convertir MS RACE a numérico
    merged_df['MS RACE'] = pd.to_numeric(merged_df['MS RACE'], errors='coerce')
    
    # Ajustar MS RACE para +1, +2, +3, +4 laps
    def adjust_race_time(row):
        if pd.isna(row['MS RACE']):
            status_id = row['STATUS RACE']
            status_text = status_dict.get(status_id, '')
            
            # Detectar +N Laps (N entre 1 y 4)
            if status_text in ['+1 Lap', '+2 Laps', '+3 Laps', '+4 Laps']:
                # Extraer número de vueltas
                laps_behind = int(status_text.split()[0].replace('+', ''))
                
                if laps_behind <= 4 and pd.notna(row['WINNER_TIME']) and pd.notna(row['BEST Q']):
                    # Calcular tiempo estimado: tiempo_ganador + (mejor_Q + 7s) * vueltas_de_más
                    estimated_time = row['WINNER_TIME'] + (row['BEST Q'] + 7000) * laps_behind
                    return estimated_time
        
        return row['MS RACE']
    
    merged_df['MS RACE'] = merged_df.apply(adjust_race_time, axis=1)
    
    # Rellenar valores NA restantes en MS RACE con valor arbitrariamente alto
    merged_df['MS RACE'] = merged_df['MS RACE'].fillna(10000000)
    
    # Eliminar columna auxiliar
    merged_df.drop(columns=['WINNER_TIME'], inplace=True)
    
    # Crear tabla de información de carreras
    race_info = pd.read_csv(RACES_FILE, sep=COMMON_DELIMITER)[['raceId', 'year']].copy()
    race_info = race_info[race_info['year'] >= PROCESS_FROM_YEAR]
    race_info = race_info.sort_values('raceId').reset_index(drop=True)
    race_info['RACE_ORDER'] = range(len(race_info))
    
    # Crear diccionario de año por raceId para lookup rápido
    races_dict = race_info.set_index('raceId')['year'].to_dict()
    
    # AGE: Calcular edad del piloto en la fecha de la carrera
    merged_df['AGE'] = ((merged_df['DATE'] - merged_df['DOB']).dt.days / 365.25).astype(int)
    
    # Ordenar por DRIVERID y RACEID para asegurar el orden correcto en shift
    merged_df = merged_df.sort_values(['DRIVERID', 'RACEID']).reset_index(drop=True)
    
    # DRIVER LAST POSITION: Posición del piloto en la carrera anterior (no en el campeonato)
    # Convertir position a numérico si no lo es
    merged_df['position'] = pd.to_numeric(merged_df['position'], errors='coerce')
    merged_df['DRIVER LAST POSITION'] = merged_df.groupby('DRIVERID')['position'].shift(1).fillna(0).astype(int)
    
    # POINTS BEFORE GP: Puntos en el campeonato antes de esta carrera
    merged_df['POINTS BEFORE GP'] = merged_df.groupby('DRIVERID')['POINTS STANDINGS'].shift(1).fillna(0)
    
    # YEARS OF EXPERIENCE: Años desde el debut
    # Crear tabla con el año del primer debut de cada piloto
    first_race = pd.read_csv(RESULTS_FILE, sep=COMMON_DELIMITER)[['raceId', 'driverId']].copy()
    first_race = first_race.merge(race_info, on='raceId', how='left')[['driverId', 'year']].drop_duplicates('driverId', keep='first').rename(columns={'year': 'DEBUT_YEAR'})
    
    merged_df = merged_df.merge(first_race, left_on='DRIVERID', right_on='driverId', how='left')
    merged_df['YEARS OF EXPERIENCE'] = (merged_df['YEAR'] - merged_df['DEBUT_YEAR']).fillna(0).astype(int)
    
    # WINS SEASON y WINS CAREER: Usar cálculo eficiente sin apply
    # Crear tabla de victorias para cada carrera
    wins_by_race = results_full[results_full['position'] == 1][['raceId', 'driverId']].copy()
    
    # Agregar información de año a las victorias
    wins_by_race = wins_by_race.merge(race_info, on='raceId', how='left')
    
    # Agregar RACE_ORDER a merged_df para comparaciones
    merged_df = merged_df.merge(race_info[['raceId', 'RACE_ORDER']], left_on='RACEID', right_on='raceId', how='left', validate='m:1')
    
    # Calcular WINS CAREER y WINS SEASON usando groupby + merge eficiente
    def calc_wins(driver_id, current_race_order, current_year):
        # WINS CAREER: todas las victorias antes de esta carrera
        wins_career = wins_by_race[(wins_by_race['driverId'] == driver_id) & (wins_by_race['RACE_ORDER'] < current_race_order)].shape[0]
        # WINS SEASON: victorias en la temporada antes de esta carrera
        wins_season = wins_by_race[(wins_by_race['driverId'] == driver_id) & (wins_by_race['RACE_ORDER'] < current_race_order) & (wins_by_race['year'] == current_year)].shape[0]
        return wins_career, wins_season
    
    # Aplicar cálculo vectorizado
    wins_data = merged_df.apply(lambda row: calc_wins(row['DRIVERID'], row['RACE_ORDER'], row['YEAR']), axis=1, result_type='expand')
    merged_df['WINS CAREER'] = wins_data[0].fillna(0).astype(int)
    merged_df['WINS SEASON'] = wins_data[1].fillna(0).astype(int)
    
    # --- Columnas para Constructores ---
    
    # CONSTRUCTOR POINTS BEFORE GP: Puntos del constructor antes de esta carrera
    # Cargar constructor standings
    constructor_standings_df = pd.read_csv('constructor_standings.csv', sep=COMMON_DELIMITER, na_values=['\\N'])
    constructor_standings_df = constructor_standings_df[constructor_standings_df['raceId'].isin(recent_race_ids)]
    constructor_standings_df = constructor_standings_df[['raceId', 'constructorId', 'points']].copy()
    constructor_standings_df.rename(columns={'points': 'CONSTRUCTOR POINTS'}, inplace=True)
    
    # Merge con constructor standings
    merged_df = merged_df.merge(
        constructor_standings_df,
        left_on=['RACEID', 'CONSTRUCTORID'],
        right_on=['raceId', 'constructorId'],
        how='left'
    )
    
    # Ordenar temporalmente por CONSTRUCTORID y RACEID para calcular CONSTRUCTOR POINTS BEFORE GP
    merged_df_temp = merged_df.sort_values(['CONSTRUCTORID', 'RACEID']).reset_index(drop=True)
    
    # Calcular CONSTRUCTOR POINTS BEFORE GP usando shift
    merged_df_temp['CONSTRUCTOR POINTS BEFORE GP'] = merged_df_temp.groupby('CONSTRUCTORID')['CONSTRUCTOR POINTS'].shift(1).fillna(0)
    
    # Restaurar el orden original por DRIVERID y RACEID
    merged_df = merged_df_temp.sort_values(['DRIVERID', 'RACEID']).reset_index(drop=True)
    
    # CONSTRUCTOR WINS SEASON: Victorias del constructor en la temporada actual antes de esta carrera
    constructor_wins = results_full[results_full['position'] == 1][['raceId', 'constructorId']].copy()
    constructor_wins = constructor_wins.merge(race_info, on='raceId', how='left')
    
    def calc_constructor_wins_season(constructor_id, current_race_order, current_year):
        # CONSTRUCTOR WINS SEASON: victorias en la temporada antes de esta carrera
        wins_season = constructor_wins[(constructor_wins['constructorId'] == constructor_id) & 
                                      (constructor_wins['RACE_ORDER'] < current_race_order) & 
                                      (constructor_wins['year'] == current_year)].shape[0]
        return wins_season
    
    merged_df['CONSTRUCTOR WINS SEASON'] = merged_df.apply(
        lambda row: calc_constructor_wins_season(row['CONSTRUCTORID'], row['RACE_ORDER'], row['YEAR']), 
        axis=1
    ).fillna(0).astype(int)
    
    # MATE LAST POSITION: Posición del compañero de equipo en la carrera anterior
    # Primero, crear un dataframe auxiliar con todos los pilotos por carrera
    # Ordenar por RACEID y DRIVERID para procesamiento secuencial
    merged_df = merged_df.sort_values(['RACEID', 'DRIVERID']).reset_index(drop=True)
    
    # Convertir position a numérico
    merged_df['position'] = pd.to_numeric(merged_df['position'], errors='coerce')
    
    # Crear una lista para almacenar las posiciones del compañero
    mate_positions = []
    
    # Para cada fila, buscar al compañero de equipo en la carrera anterior
    for idx, row in merged_df.iterrows():
        driver_id = row['DRIVERID']
        current_race_id = row['RACEID']
        constructor_id = row['CONSTRUCTORID']
        
        # Buscar la carrera anterior de este piloto
        prev_races = merged_df[
            (merged_df['DRIVERID'] == driver_id) & 
            (merged_df['RACEID'] < current_race_id)
        ]
        
        if prev_races.empty:
            mate_positions.append(None)
            continue
        
        # Obtener la última carrera anterior
        prev_race_id = prev_races['RACEID'].max()
        
        # Buscar el constructor en esa carrera anterior
        prev_constructor = merged_df[
            (merged_df['DRIVERID'] == driver_id) & 
            (merged_df['RACEID'] == prev_race_id)
        ]['CONSTRUCTORID'].iloc[0]
        
        # Buscar al compañero de equipo en esa misma carrera anterior
        mate = merged_df[
            (merged_df['RACEID'] == prev_race_id) &
            (merged_df['CONSTRUCTORID'] == prev_constructor) &
            (merged_df['DRIVERID'] != driver_id)
        ]
        
        if mate.empty:
            mate_positions.append(None)
        else:
            mate_pos = mate['position'].iloc[0]
            mate_positions.append(int(mate_pos) if pd.notna(mate_pos) else None)
    
    merged_df['MATE LAST POSITION'] = mate_positions
    
    # --- 6. Filtrado Final ---
    print(f"Filtrando datos para incluir solo carreras a partir del año {min_year}...")
    
    # Aplicar el filtro: solo años >= min_year
    final_df = merged_df[merged_df['YEAR'] >= min_year].copy()
    
    # Seleccionar y reordenar las columnas finales
    final_df = final_df[COLUMNAS_FINALES]
    
    # Limpiar valores NaN en columnas numéricas
    final_df['DRIVER LAST POSITION'] = final_df['DRIVER LAST POSITION'].fillna(0).astype(int)
    final_df['POINTS BEFORE GP'] = final_df['POINTS BEFORE GP'].fillna(0)


    # --- 7. Guardar el Resultado Final ---
    print(f"Guardando el dataset final completado en {OUTPUT_FILE}...")
    final_df.to_csv(OUTPUT_FILE, index=False)

    print("\n✅ ¡Proceso de generación de dataset completado!")
    print(f"Dataset generado con {final_df.shape[0]} filas y {final_df.shape[1]} columnas.")


# Ejecutar la función principal para generar el dataset
generar_dataset_f1_completo(min_year=2016)
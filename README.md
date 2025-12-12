# PROCESAMIENTO Y TRANSFORMACIÓN DEL DATASET DE F1

## Tabla de Contenidos
1. [Introducción: Datos Originales](#1-introducción-datos-originales)
2. [Fusión de Datos](#2-fusión-de-datos-data-merging)
3. [Transformaciones de Datos](#3-transformaciones-de-datos)
4. [Imputación de Tiempos de Carrera](#4-imputación-de-tiempos-de-carrera-ms-race)
5. [Variables Binarias de Validez](#5-creación-de-variables-binarias-de-validez)
6. [Features Avanzadas](#6-creación-de-features-avanzadas)
7. [Transformación Temporal](#7-transformación-temporal-de-year)
8. [Limpieza y Valores por Defecto](#8-limpieza-y-valores-por-defecto)
9. [Filtrado y Dataset Final](#9-filtrado-temporal-y-dataset-final)
10. [Resultado Final](#10-resultado-final)
11. [Resumen de Mejoras](#11-resumen-de-mejoras-clave)

---

## 1. INTRODUCCIÓN: DATOS ORIGINALES

### Objetivo
Presentar las fuentes de datos utilizadas para construir el dataset unificado de F1.

### Fuentes de Datos
Dataset original proveniente de **Kaggle F1 Database** que incluye múltiples archivos CSV:

| Archivo | Descripción | Columnas Clave |
|---------|-------------|----------------|
| `results.csv` | Resultados de carreras principales | raceId, driverId, constructorId, position, milliseconds |
| `sprint_results.csv` | Resultados de carreras sprint | raceId, driverId, milliseconds |
| `qualifying.csv` | Tiempos de clasificación | raceId, driverId, q1, q2, q3 |
| `races.csv` | Información de carreras | raceId, circuitId, round, year, date |
| `circuits.csv` | Características de circuitos | circuitId, lap_distance_km, urban |
| `f1_weather_data.csv` | Datos meteorológicos | raceId, temperatura, viento, humedad, precipitación |
| `drivers.csv` | Información de pilotos | driverId, dob (fecha nacimiento) |
| `driver_standings.csv` | Puntos en campeonato | raceId, driverId, points, position |
| `constructor_standings.csv` | Puntos de constructores | raceId, constructorId, points |
| `status.csv` | Estados de finalización | statusId, status (DNF, +N Laps, etc.) |

**Código de referencia**: Líneas 11-14 del script `script_carga.py`

---

## 2. FUSIÓN DE DATOS (DATA MERGING)

### Objetivo
Combinar múltiples fuentes de datos en un único dataset coherente mediante operaciones JOIN.

### 2.1. Fusión Principal: Results + Sprint
- **Tipo de Join**: Full Outer Join
- **Claves**: `raceId`, `driverId`, `constructorId`
- **Razón**: Algunos pilotos solo participan en carrera principal, otros en ambas
- **Código**: Líneas 131-136

```python
merged_df = pd.merge(
    results_df, sprint_df,
    on=['raceId', 'driverId', 'constructorId'],
    how='outer'
)
```

### 2.2. Añadir Clasificación (Qualifying)
- **Tipo de Join**: Left Join
- **Claves**: `raceId`, `driverId`, `constructorId`
- **Columnas añadidas**: `q1`, `q2`, `q3`
- **Código**: Líneas 138-144

### 2.3. Añadir Información de Carreras
- **Tipo de Join**: Left Join
- **Clave**: `raceId`
- **Columnas añadidas**: `circuitId`, `round`, `year`, `date`
- **Código**: Líneas 146-151

### 2.4. Añadir Características de Circuitos ⭐
- **Tipo de Join**: Left Join
- **Clave**: `CIRCUITID`
- **Columnas añadidas**: 
  - **`LAP DISTANCE KM`**: Kilómetros por vuelta del circuito
  - **`URBAN`**: Variable binaria (1=circuito urbano, 0=circuito permanente)
- **Código**: Líneas 153-158

**Impacto**: Permite al modelo considerar la naturaleza del circuito (urbano vs permanente) y la distancia de vuelta como factores predictivos.

### 2.5. Añadir Datos Meteorológicos
- **Tipo de Join**: Left Join
- **Clave**: `raceId`
- **Columnas añadidas**: 
  - Velocidad del viento (promedio y máxima)
  - Temperatura (promedio, mínima, máxima)
  - Humedad promedio
  - Precipitación total
  - Presión atmosférica (MSL y superficie)
- **Valores redondeados**: 2 decimales para todas las variables
- **Código**: Líneas 160-165

### 2.6. Añadir Información de Pilotos y Campeonato
- **Pilotos**: `DOB` (fecha de nacimiento) para calcular edad
- **Standings**: Puntos y posición en el campeonato
- **Código**: Líneas 167-178

---

## 3. TRANSFORMACIONES DE DATOS

### 3.1. Creación de Columna SPRINT Y/N
- **Tipo**: Variable binaria (0/1)
- **Lógica**: `1` si `MS SPRINT` tiene valor, `0` si es nulo
- **Objetivo**: Indicar si hubo carrera sprint ese fin de semana
- **Código**: Línea 183

```python
merged_df['SPRINT Y/N'] = merged_df['MS SPRINT'].notna().astype(int)
```

### 3.2. Conversión de Tiempos de Clasificación
**Problema**: Los tiempos vienen en formato texto `MM:SS.SSS`

**Solución**: Convertir a milisegundos (numérico)

```python
def time_to_milliseconds(time_str):
    if pd.isna(time_str):
        return None
    parts = str(time_str).split(':')
    minutes = int(parts[0])
    seconds = float(parts[1])
    return int(minutes * 60 * 1000 + seconds * 1000)
```

**Penalización**: `300,000 ms` para pilotos que no clasificaron
- **Código**: Líneas 185-203

### 3.3. Creación de Columna BEST Q
- **Lógica**: Mínimo entre Q1, Q2, Q3
- **Objetivo**: Mejor tiempo de clasificación del piloto
- **Código**: Línea 219

```python
merged_df['BEST Q'] = merged_df[['Q1', 'Q2', 'Q3']].min(axis=1)
```

### 3.4. Cálculo de LAPS RACE
- **Fuente**: Número de vueltas completadas por el ganador
- **Objetivo**: Referencia del número total de vueltas de la carrera
- **Código**: Líneas 224-228

---

## 4. IMPUTACIÓN DE TIEMPOS DE CARRERA (MS RACE)

### 4.1. Problema: Pilotos con +N Laps
**Situación**: Pilotos que terminan la carrera pero con vueltas de menos respecto al ganador.

**Ejemplo**: 
- Estado "+2 Laps" = piloto terminó 2 vueltas por detrás del ganador
- Dataset original: estos casos tienen `MS RACE = NaN`

### 4.2. Solución: Estimación Inteligente

**Fórmula implementada**:
```
MS RACE = WINNER_TIME + (BEST_Q + 7000) × N_LAPS_BEHIND
```

**Componentes**:
- `WINNER_TIME`: Tiempo del ganador en milisegundos
- `BEST_Q`: Mejor vuelta del piloto en clasificación
- `+7000 ms`: Penalización adicional por tráfico/condiciones de carrera
- `N_LAPS_BEHIND`: Número de vueltas de retraso (detecta +1, +2, +3, +4 Laps)

**Código**: Líneas 238-253

```python
def adjust_race_time(row):
    if pd.isna(row['MS RACE']):
        status_text = status_dict.get(row['STATUS RACE'], '')
        if status_text in ['+1 Lap', '+2 Laps', '+3 Laps', '+4 Laps']:
            laps_behind = int(status_text.split()[0].replace('+', ''))
            if laps_behind <= 4 and pd.notna(row['WINNER_TIME']) and pd.notna(row['BEST Q']):
                estimated_time = row['WINNER_TIME'] + (row['BEST Q'] + 7000) * laps_behind
                return estimated_time
    return row['MS RACE']
```

### 4.3. Penalización para No Finalizadores
- **Valor asignado**: `10,000,000 ms` (~2.7 horas)
- **Aplicación**: DNF (Did Not Finish), accidentes, descalificaciones, retiros
- **Objetivo**: Indicar que el piloto no completó la carrera
- **Código**: Línea 256

---

## 5. CREACIÓN DE VARIABLES BINARIAS DE VALIDEZ

### 5.1. Columnas Creadas ⭐
Cuatro nuevas variables binarias que indican la validez de los datos:

| Variable | Condición | Significado |
|----------|-----------|-------------|
| `Q1 VALID` | `Q1 ≠ 300,000` | Piloto clasificó y tiene tiempo en Q1 |
| `Q2 VALID` | `Q2 ≠ 300,000` | Piloto pasó a Q2 y tiene tiempo |
| `Q3 VALID` | `Q3 ≠ 300,000` | Piloto pasó a Q3 y tiene tiempo |
| `RACE VALID` | `MS RACE ≠ 10,000,000` | Piloto terminó la carrera |

### 5.2. Objetivo
- Indicar explícitamente si el piloto tiene datos válidos en cada fase
- Facilitar filtrado y análisis por modelos ML
- Permite detectar patrones de abandono o clasificación

**Código**: Líneas 258-265

```python
merged_df['Q1 VALID'] = (merged_df['Q1'] != 300000).astype(int)
merged_df['Q2 VALID'] = (merged_df['Q2'] != 300000).astype(int)
merged_df['Q3 VALID'] = (merged_df['Q3'] != 300000).astype(int)
merged_df['RACE VALID'] = (merged_df['MS RACE'] != 10000000).astype(int)
```

---

## 6. CREACIÓN DE FEATURES AVANZADAS

### 6.1. AGE (Edad del Piloto)
- **Cálculo**: Diferencia entre fecha de carrera y fecha de nacimiento
- **Unidad**: Años enteros
- **Código**: Línea 276

```python
merged_df['AGE'] = ((merged_df['DATE'] - merged_df['DOB']).dt.days / 365.25).astype(int)
```

### 6.2. DRIVER LAST POSITION
- **Definición**: Posición del piloto en su carrera anterior (no en el campeonato)
- **Uso**: Indicador de forma reciente del piloto
- **Valor por defecto**: `21` si es su primera carrera
- **Implementación**: Usa `groupby` + `shift` para obtener la posición anterior
- **Código**: Línea 282

### 6.3. POINTS BEFORE GP
- **Definición**: Puntos acumulados en el campeonato antes de esta carrera
- **Uso**: Estado actual en el campeonato de pilotos
- **Valor por defecto**: `0` para primera carrera de la temporada
- **Código**: Línea 285

### 6.4. YEARS OF EXPERIENCE
- **Cálculo**: Años transcurridos desde el debut del piloto en F1
- **Implementación**: 
  1. Identifica el año de debut (primera aparición en `results.csv`)
  2. Resta año de debut al año actual de la carrera
- **Código**: Líneas 288-292

### 6.5. WINS SEASON y WINS CAREER
**WINS SEASON**: 
- Número de victorias en la temporada actual **antes** de esta carrera
- Reinicia a 0 cada temporada

**WINS CAREER**: 
- Número total de victorias en toda la carrera del piloto **antes** de esta carrera
- Acumulativo a lo largo de los años

**Importancia**: Indicadores clave de rendimiento histórico y forma actual

**Código**: Líneas 294-314

```python
def calc_wins(driver_id, current_race_order, current_year):
    wins_career = wins_by_race[
        (wins_by_race['driverId'] == driver_id) & 
        (wins_by_race['RACE_ORDER'] < current_race_order)
    ].shape[0]
    
    wins_season = wins_by_race[
        (wins_by_race['driverId'] == driver_id) & 
        (wins_by_race['RACE_ORDER'] < current_race_order) & 
        (wins_by_race['year'] == current_year)
    ].shape[0]
    
    return wins_career, wins_season
```

### 6.6. Variables de Constructores

#### CONSTRUCTOR POINTS BEFORE GP
- **Definición**: Puntos del constructor en el campeonato antes de esta carrera
- **Uso**: Estado del equipo en el campeonato de constructores
- **Código**: Líneas 316-331

#### CONSTRUCTOR WINS SEASON
- **Definición**: Victorias del constructor en la temporada actual antes de esta carrera
- **Uso**: Indicador de competitividad del equipo
- **Código**: Líneas 333-344

### 6.7. MATE LAST POSITION
- **Definición**: Posición del compañero de equipo en su última carrera
- **Uso**: Permite comparación intra-equipo y evaluar rendimiento relativo
- **Valor por defecto**: `21` si no tiene compañero identificado
- **Implementación**: 
  1. Identifica al compañero en la misma carrera y constructor
  2. Obtiene su `DRIVER LAST POSITION`
- **Código**: Líneas 346-372

---

## 7. TRANSFORMACIÓN TEMPORAL DE YEAR

### 7.1. Transformación Aplicada
**Fórmula**: `YEAR = YEAR - 2025`

**Resultado**:

| Año Original | YEAR Transformado |
|--------------|-------------------|
| 2025 | 0 |
| 2024 | -1 |
| 2023 | -2 |
| 2022 | -3 |
| ... | ... |
| 2014 | -11 |

### 7.2. Ventajas
1. **Reduce magnitud**: Valores más pequeños facilitan el entrenamiento
2. **Más interpretable**: Representa "distancia temporal desde hoy"
3. **Facilita ML**: Evita valores muy grandes que pueden afectar la normalización
4. **Mantiene orden**: El orden cronológico se preserva

**Código**: Línea 381

```python
final_df['YEAR'] = final_df['YEAR'] - 2025
```

---

## 8. LIMPIEZA Y VALORES POR DEFECTO

### 8.1. GRID = 0 → GRID = 20
**Problema**: `GRID = 0` en F1 significa salida desde pit lane (penalización)

**Solución**: Asignar posición `20` (equivalente a salir desde el fondo de la parrilla)

**Código**: Línea 388

```python
final_df['GRID'] = final_df['GRID'].replace(0, 20)
```

### 8.2. Manejo de Valores Faltantes

| Variable | Valor de Relleno | Justificación |
|----------|------------------|---------------|
| `DRIVER LAST POSITION` | 21 | Peor posición posible + 1 |
| `POINTS BEFORE GP` | 0 | Sin puntos previos |
| `MATE LAST POSITION` | 21 | Sin información del compañero |

**Código**: Líneas 384-385

---

## 9. FILTRADO TEMPORAL Y DATASET FINAL

### 9.1. Filtrado por Año Mínimo
- **Parámetro configurable**: `min_year = 2014` (por defecto)
- **Razón**: Datos más relevantes y consistentes desde la era híbrida de F1 (2014+)
- **Beneficio**: Reduce ruido de regulaciones antiguas muy diferentes
- **Código**: Línea 379

```python
final_df = merged_df[merged_df['YEAR'] >= min_year].copy()
```

### 9.2. Selección de Columnas Finales
**Total**: 36 columnas organizadas en categorías:

#### Identificadores (4)
- `RACEID`, `DRIVERID`, `CONSTRUCTORID`, `CIRCUITID`

#### Contexto de Carrera (5)
- `ROUND`, `YEAR`, `LAP DISTANCE KM`, `LAPS RACE`, `URBAN`

#### Datos Meteorológicos (8)
- `AVG WIND SPEED`, `MAX WIND SPEED`
- `AVG TEMPERATURE`, `MIN TEMPERATURE`, `MAX TEMPERATURE`
- `AVG HUMIDITY`, `PRECIPITATION`
- `AVG PRESSURE MSL`, `AVG SURFACE PRESSURE`

#### Variables del Piloto (6)
- `DRIVER LAST POSITION`, `WINS SEASON`, `WINS CAREER`
- `POINTS BEFORE GP`, `YEARS OF EXPERIENCE`, `AGE`

#### Variables del Compañero (1)
- `MATE LAST POSITION`

#### Variables del Constructor (2)
- `CONSTRUCTOR POINTS BEFORE GP`, `CONSTRUCTOR WINS SEASON`

#### Clasificación (9)
- `Q1`, `Q2`, `Q3`, `BEST Q`, `GRID`
- `Q1 VALID`, `Q2 VALID`, `Q3 VALID`

#### Sprint y Target (2)
- `SPRINT Y/N`, `RACE VALID`
- **`MS RACE`** (variable objetivo)

**Código**: Líneas 23-34, 383

---

## 10. RESULTADO FINAL

### 10.1. Estadísticas del Dataset
- **Período temporal**: 2014-2024 (11 temporadas)
- **Número aproximado de filas**: ~4,400 registros
  - ~20 pilotos por carrera
  - ~20-24 carreras por año
  - 11 años de datos
- **Número de columnas**: 36 features + 1 target = 37 total
- **Tamaño estimado**: ~2-3 MB en formato CSV

### 10.2. Archivo Generado
- **Nombre**: `f1_training_data_2014_onwards.csv` (configurable según `min_year`)
- **Formato**: CSV con separador de comas
- **Encoding**: UTF-8
- **Sin índice**: Para facilitar la carga en herramientas de ML

**Código de guardado**: Líneas 391-393

```python
OUTPUT_FILE = f'f1_training_data_{min_year}_onwards.csv'
final_df.to_csv(OUTPUT_FILE, index=False)
```

### 10.3. Mensaje de Confirmación
```
✅ ¡Proceso de generación de dataset completado!
Dataset generado con X filas y 37 columnas.
```

---

## 11. RESUMEN DE MEJORAS CLAVE

### Transformaciones Implementadas

1. ✅ **Integración completa**: 9 archivos CSV → 1 dataset unificado
2. ✅ **Features de circuito**: Distancia por vuelta + Variable binaria Urbano
3. ✅ **Datos meteorológicos**: 8 variables climáticas integradas y redondeadas
4. ✅ **Imputación inteligente**: Estimación de tiempos para pilotos con +N Laps
5. ✅ **Variables de validez**: 4 columnas binarias (Q1/Q2/Q3/RACE VALID)
6. ✅ **Features temporales**: Experiencia, victorias acumuladas, forma reciente
7. ✅ **Contexto de equipo**: Rendimiento del compañero y constructor
8. ✅ **Transformación de YEAR**: Ajustado para facilitar ML (distancia desde 2025)
9. ✅ **Limpieza exhaustiva**: Manejo de valores nulos y casos especiales
10. ✅ **Dataset listo para ML**: Normalizado, estructurado y sin datos faltantes críticos

### Valor Añadido

| Aspecto | Antes | Después |
|---------|-------|---------|
| **Archivos** | 9 CSV dispersos | 1 CSV unificado |
| **Datos faltantes** | ~15% NaN en tiempos | <1% (imputados inteligentemente) |
| **Features** | ~15 básicas | 37 avanzadas |
| **Información contextual** | Limitada | Rica (clima, circuito, histórico) |
| **Usabilidad ML** | Requiere preprocesado | Lista para entrenar |

### Casos de Uso
Este dataset procesado permite entrenar modelos de Machine Learning para:

1. **Predicción de tiempos de carrera** (`MS RACE`)
2. **Clasificación de posiciones finales**
3. **Análisis de factores que influyen en el rendimiento**
4. **Comparación de pilotos y constructores**
5. **Impacto de variables meteorológicas y de circuito**

---

## Ejecución del Script

### Requisitos
```bash
pip install pandas numpy
```

### Uso Básico
```python
# Generar dataset desde 2014 (por defecto)
generar_dataset_f1_completo(min_year=2014)

# Generar dataset desde otro año
generar_dataset_f1_completo(min_year=2016)
```

### Estructura de Carpetas
```
proyecto/
├── f1_data/
│   ├── results.csv
│   ├── sprint_results.csv
│   ├── qualifying.csv
│   ├── races.csv
│   ├── circuits.csv
│   ├── f1_weather_data.csv
│   ├── drivers.csv
│   ├── driver_standings.csv
│   ├── constructor_standings.csv
│   └── status.csv
├── script_carga.py
└── f1_training_data_2014_onwards.csv (generado)
```

---

## Autor y Contacto

**Proyecto**: Predictor F1 2025  
**Repositorio**: [100479095/Predictor_F1_2025](https://github.com/100479095/Predictor_F1_2025)  
**Fecha**: Diciembre 2025

---

## Licencia

Este proyecto procesa datos públicos de F1 disponibles en Kaggle para fines educativos y de investigación.

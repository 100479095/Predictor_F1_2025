import os
import pandas as pd

def download_all_f1_data():
    """Descarga todos los archivos CSV del dataset de F1 de Kaggle"""
    print("=== Descargando todos los datos de F1 desde Kaggle ===\n")
    
    # Crear directorio para datos si no existe
    os.makedirs('f1_data', exist_ok=True)
    
    # Dataset de jtrotman (actualizado con datos más recientes)
    print("Descargando dataset de F1 (actualizado)...")
    os.system('kaggle datasets download -d jtrotman/formula-1-race-data -p f1_data --unzip')
    
    print("\n✓ Descarga completada!")
    
    # Procesar circuits.csv para añadir columna urban
    print("\nProcesando circuits.csv...")
    
    circuits_df = pd.read_csv('f1_data/circuits.csv')
    
    # Circuitos urbanos (basados en street circuits conocidos)
    urban_circuits = {
        6,   # Monaco
        12,  # Valencia
        15,  # Marina Bay (Singapore)
        29,  # Adelaide
        32,  # Mexico City (semi-urbano, pero incluido)
        33,  # Phoenix
        37,  # Detroit
        42,  # Dallas
        43,  # Long Beach
        44,  # Las Vegas (antiguo)
        49,  # Montjuic
        71,  # Sochi (semi-urbano)
        73,  # Baku
        77,  # Jeddah
        79,  # Miami
        80   # Las Vegas Strip (nuevo)
    }
    
    # Añadir columna urban (1 si es urbano, 0 si no)
    circuits_df['urban'] = circuits_df['circuitId'].apply(lambda x: 1 if x in urban_circuits else 0)
    
    # Guardar circuits.csv actualizado en la raíz del proyecto
    circuits_df.to_csv('circuits.csv', index=False)
    
    print(f'✅ Columna "urban" añadida a circuits.csv')
    print(f'Total de circuitos urbanos: {circuits_df["urban"].sum()} de {len(circuits_df)}')
    print('\nArchivos descargados en la carpeta "f1_data/"')
    print('Archivo "circuits.csv" procesado y guardado en la raíz del proyecto')

if __name__ == "__main__":
    download_all_f1_data()

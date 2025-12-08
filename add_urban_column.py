import pandas as pd

# Cargar el CSV
df = pd.read_csv('f1_data/circuits.csv')

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
df['urban'] = df['circuitId'].apply(lambda x: 1 if x in urban_circuits else 0)

# Guardar el CSV actualizado
df.to_csv('circuits.csv', index=False)

print(f'✅ Columna "urban" añadida exitosamente.')
print(f'Total de circuitos urbanos: {df["urban"].sum()} de {len(df)}')
print('\nCircuitos urbanos:')
print(df[df['urban'] == 1][['circuitId', 'name', 'location']].to_string(index=False))

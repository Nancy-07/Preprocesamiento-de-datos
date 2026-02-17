import json
import os
import pandas as pd

def asignar_comisaria(df: pd.DataFrame, columna_municipio: str = 'Municipio') -> pd.DataFrame:
    """
    Asigna una columna 'comisaria' al DataFrame basada en el municipio.
    
    Args:
        df: DataFrame conteniendo la columna de municipios.
        columna_municipio: Nombre de la columna que contiene los municipios.
        
    Returns:
        DataFrame con la nueva columna 'comisaria'.
    """
    # Ruta al archivo json
    json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'comisarias.json')
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            comisarias_data = json.load(f)
    except FileNotFoundError:
        print(f"Advertencia: No se encontró el archivo {json_path}. No se asignarán comisarías.")
        df['comisaria'] = None
        return df

    # Crear diccionario inverso: MUNICIPIO -> id_comisaria
    municipio_to_comisaria = {}
    for comisaria_id, municipios in comisarias_data.items():
        for municipio in municipios:
            # Normalizar a mayúsculas para coincidir con el DataFrame si es necesario
            municipio_to_comisaria[municipio.upper()] = comisaria_id

    # Función auxiliar para mapear
    def get_comisaria(municipio):
        if not isinstance(municipio, str):
            return None
        return municipio_to_comisaria.get(municipio.upper())

    # Aplicar mapeo
    if columna_municipio in df.columns:
        df['comisaria'] = df[columna_municipio].apply(get_comisaria)
    else:
        print(f"Advertencia: Columna '{columna_municipio}' no encontrada en el DataFrame.")
        df['comisaria'] = None
        
    return df

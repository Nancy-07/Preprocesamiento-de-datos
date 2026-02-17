import pandas as pd
import sys
import os

# Al inicio del archivo, antes de los imports de funciones
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if _BASE_DIR not in sys.path:
    sys.path.insert(0, _BASE_DIR)

try:
    from procesamiento_notas import procesar_notas_masivo
    from notas_extraccion import normalizar_texto_es
    from asignar_comisaria import asignar_comisaria
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'funciones'))
    from procesamiento_notas import procesar_notas_masivo
    from notas_extraccion import normalizar_texto_es
    from asignar_comisaria import asignar_comisaria


COLS_MAP = {
    'Folio': 'Folio',
    'Fecha': 'Fecha',
    'Tipo de Incidente': 'Tipo de Incidente',
    'comisaria': 'comisaria',
    'Municipio': 'Municipio',
    'Hora de Recibido': 'Hora de Recibido',
    'HORA_CIERRE': 'HORA_CIERRE',
    'Latitud': 'Latitud',
    'Longitud': 'Longitud',
    'Coordenadas_': 'Coordenadas',
    'Divididos' : 'Divididos',
    'folios_ligados': 'folios_ligados',
    'cancelado': 'cancelados',
    'referencia_folio': 'referencia_folio',
    'nota_limpia': 'Nota'
}

TIPO_INCIDENTE_EXCLUIDOS = {'70104'}


def procesar_reporte(input_file: str, output_file: str) -> pd.DataFrame:
    """
    Lee un CSV de reporte, procesa las notas, asigna comisaría y guarda el resultado.

    Args:
        input_file:  Ruta al CSV de entrada.
        output_file: Ruta donde se guardará el CSV procesado.

    Returns:
        DataFrame final procesado.
    """
    # ── 1. Lectura ──────────────────────────────────────────────────────────
    print(f"Leyendo archivo: {input_file}")
    try:
        df = pd.read_csv(input_file, encoding='utf-8')
    except UnicodeDecodeError:
        print("UTF-8 falló, intentando latin1...")
        df = pd.read_csv(input_file, encoding='latin1')

    print(f"Filas leídas: {len(df)}")

    # ── 2. Filtrado temprano ────────────────────────────────────────────────
    # Se descarta antes de cualquier procesamiento pesado para reducir carga
    antes = len(df)
    df = df[~df['Tipo de Incidente'].astype(str).isin(TIPO_INCIDENTE_EXCLUIDOS)].copy()
    print(f"Filas tras filtrar tipos excluidos: {len(df)} (descartadas: {antes - len(df)})")

    # ── 3. Validación de columna clave ──────────────────────────────────────
    if 'Notas' not in df.columns:
        raise ValueError("Columna 'Notas' no encontrada en el archivo de entrada.")

    # ── 4. Procesamiento de notas ───────────────────────────────────────────
    print("Procesando columna 'Notas'...")
    notas_series = df['Notas'].fillna('')
    df_procesado = procesar_notas_masivo(notas_series)

    print("Normalizando notas procesadas...")
    df_procesado['nota_limpia'] = (
        df_procesado['nota_limpia']
        .astype(str)
        .apply(lambda x: normalizar_texto_es(x, eliminar_stopwords=False))
    )

    # ── 5. Consolidación ────────────────────────────────────────────────────
    df_procesado.index = df.index
    df_final = pd.concat([df, df_procesado], axis=1)

    # ── 6. Asignación de comisaría ──────────────────────────────────────────
    print("Asignando comisarias...")
    df_final = asignar_comisaria(df_final)

    # ── 7. Selección y renombrado de columnas ───────────────────────────────
    cols_to_export, rename_dict = [], {}
    for original, nuevo in COLS_MAP.items():
        if original in df_final.columns:
            cols_to_export.append(original)
            rename_dict[original] = nuevo
        else:
            print(f"Advertencia: Columna '{original}' no encontrada en el DataFrame final.")

    df_out = df_final[cols_to_export].rename(columns=rename_dict)

    # ── 8. Exportación ──────────────────────────────────────────────────────
    print(f"Guardando {len(df_out)} filas en: {output_file}")
    df_out.to_csv(output_file, index=False, encoding='utf-8')
    print("Proceso terminado exitosamente.")

    return df_out


# ── Punto de entrada ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    procesar_reporte(
        input_file=os.path.join(base_dir, 'Limpieza_notas', 'Reporte_enero.csv'),
        output_file=os.path.join(base_dir, 'Reporte_procesado_2.csv'),
    )
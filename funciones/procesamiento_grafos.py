import pandas as pd
import networkx as nx
import ast

def limpiar_foliostr(val):
    """
    Normaliza el folio a string limpio.
    """
    if pd.isna(val):
        return None
    val_str = str(val).strip()
    if val_str == '' or val_str.lower() == 'nan':
        return None
    # Eliminar posibles comillas sobrantes si existen (aunque ast.literal_eval help)
    return val_str

def parsear_lista_string(val):
    """
    Parsea una cadena que representa una lista de python o una cadena separada por comas.
    Ej: "['123', '456']" -> ['123', '456']
    Ej: "123, 456" -> ['123', '456']
    """
    if pd.isna(val):
        return []
    
    val_str = str(val).strip()
    if not val_str or val_str == 'nan' or val_str == '[]':
        return []
    
    try:
        # Intentar parsear como lista python
        if val_str.startswith('[') and val_str.endswith(']'):
            parsed = ast.literal_eval(val_str)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
    except Exception:
        pass
        
    # Si falla o no es lista, intentar separar por comas
    # Esto maneja tanto "123, 456" como casos fallidos de listas mal formadas
    items = [x.strip() for x in val_str.replace('[', '').replace(']', '').replace("'", "").split(',')]
    return [x for x in items if x]

def construir_grafo(df):
    """
    Construye un grafo de relaciones entre folios.
    Retorna el grafo NetworkX y un diccionario de {folio: grupo_id}.
    """
    G = nx.Graph()
    
    # Asegurar que todos los folios existan como nodos
    # (Incluso los que no tienen relaciones)
    if 'Folio' in df.columns:
        for folio in df['Folio'].dropna().unique():
            folio_limpio = limpiar_foliostr(folio)
            if folio_limpio:
                G.add_node(folio_limpio)
    
    # Columnas de relación
    cols_relacion = ['Divididos', 'folios_ligados', 'referencia_folio']
    
    for idx, row in df.iterrows():
        folio_origen = limpiar_foliostr(row.get('Folio'))
        
        if not folio_origen:
            continue
            
        for col in cols_relacion:
            if col not in df.columns:
                continue
                
            relacionados = parsear_lista_string(row[col])
            
            for rel in relacionados:
                folio_destino = limpiar_foliostr(rel)
                if folio_destino and folio_destino != folio_origen:
                    G.add_edge(folio_origen, folio_destino)
                    
    return G

def analizar_componentes(G):
    """
    Analiza las componentes conectadas del grafo.
    Retorna un DataFrame con columnas: [Folio, Grupo_ID, Tamano_Grupo]
    """
    componentes = list(nx.connected_components(G))
    data = []
    
    for i, comp in enumerate(componentes):
        grupo_id = i + 1
        tamano = len(comp)
        for folio in comp:
            data.append({
                'Folio': folio,
                'Grupo_ID': grupo_id,
                'Tamano_Grupo': tamano
            })
            
    return pd.DataFrame(data)

def separar_folios_cancelados(
    df_original,
    df_grupos,
    output_cancelados=None,
    output_limpio=None,
    output_relaciones=None
):
    """
    Separa los folios cancelados que están aislados (Tamano_Grupo == 1).
    Genera los 3 archivos de salida.
    """
    # Unir información de grupos al DF original
    # Asegurar tipos
    df = df_original.copy()
    
    # Normalizar columna Folio para el merge
    df['Folio_Key'] = df['Folio'].apply(limpiar_foliostr)
    df_grupos['Folio_Key'] = df_grupos['Folio'].apply(limpiar_foliostr)
    
    # Merge left para conservar todos los del reporte original
    df_merged = pd.merge(df, df_grupos[['Folio_Key', 'Grupo_ID', 'Tamano_Grupo']], 
                         left_on='Folio_Key', right_on='Folio_Key', how='left')
    
    # Guardar reporte de relaciones completo si se solicita
    if output_relaciones:
        df_grupos.to_csv(output_relaciones, index=False, encoding='utf-8-sig')
        print(f"Reporte de relaciones guardado en: {output_relaciones}")

    # Criterio de separación:
    # 1. Cancelado NO nulo/vacío
    # 2. Tamano_Grupo == 1 (o nulo, si no estaba en el grafo, pero debería estar si venía en el df original)
    #    Nota: Si Tamano_Grupo es NaN, significa que no se procesó en el grafo, asumimos aislado.
    
    def es_cancelado_valido(val):
        parsed = parsear_lista_string(val)
        return len(parsed) > 0

    mask_cancelado = df_merged['cancelados'].apply(es_cancelado_valido)
    mask_aislado = (df_merged['Tamano_Grupo'] == 1) | (df_merged['Tamano_Grupo'].isna())
    
    # Folios a separar (Cancelados Y Aislados)
    folios_a_separar_mask = mask_cancelado & mask_aislado
    
    df_cancelados = df_merged[folios_a_separar_mask].copy()
    df_limpio = df_merged[~folios_a_separar_mask].copy()
    
    # Limpieza de columnas auxiliares antes de guardar
    cols_to_drop = ['Folio_Key', 'Grupo_ID', 'Tamano_Grupo']
    df_cancelados_out = df_cancelados.drop(columns=cols_to_drop, errors='ignore')
    # Para el limpio, tal vez quieran conservar el Grupo? El usuario pidió separar, no modificar el schema del limpio.
    # Mantendremos el schema original para el limpio para evitar problemas downstream, 
    # a menos que se pida explícitamente.
    # El usuario dijo: "Genera una funciòn... sepraralos de los demas". 
    # Generalmente se espera el mismo formato de entrada.
    df_limpio_out = df_limpio.drop(columns=cols_to_drop, errors='ignore')
    
    # Guardar cancelados
    # Solo columnas relevantes o todas? "sepraralos de los demas" sugiere mover la fila entera.
    # El ejemplo anterior del usuario mostraba solo la columna Folio en el csv de salida de cancelados.
    # Vamos a guardar TODO el registro en el archivo de cancelados por seguridad, 
    # y también un archivo solo con folios si es necesario (el previo script hacia eso).
    # Revisando el script anterior 'Folios_cancelados.py', guardaba solo la columna Folio.
    # Haremos eso para cumplir con la expectativa previa de formato si es posible, 
    # pero el usuario pidió "sepraralos", que puede implicar las filas enteras.
    # Ante la duda, guardaré el DF completo en cancelados, ya que contiene la info de por qué se canceló.
    # PERO, el script previo 'Folios_cancelados.py' producia 'folios_cancelados_sin_relacion.csv' con SOLO la columna Folio.
    # Voy a mantener ese comportamiento para la columna Folio, pero añadiré las demás columnas por utilidad,
    # o mejor, si el usuario quiere re-integrar, dejarlo completo.
    # Voy a guardar completo.
    
    if output_cancelados:
        df_cancelados_out.to_csv(output_cancelados, index=False, encoding='utf-8-sig')
    if output_limpio:
        df_limpio_out.to_csv(output_limpio, index=False, encoding='utf-8-sig')
    
    print(f"Total registros originales: {len(df)}")
    print(f"Cancelados aislados separados: {len(df_cancelados)}")
    print(f"Registros restantes limpios: {len(df_limpio)}")
    
    return df_cancelados, df_limpio

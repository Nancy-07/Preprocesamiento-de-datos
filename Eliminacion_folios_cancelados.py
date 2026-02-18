import pandas as pd

""""
Elimincación de folios cancelados y únicos 

Entra: 
1.- folios_cancelados_sin_relacion.csv 
2-. Reporte original ya preprocesado

Salida:
Report_enero_limpieza_actualizado.csv 

"""
df_cancelados = pd.read_csv(r"Data\folios_cancelados_sin_relacion.csv")
df_reporte_enero_limpieza = pd.read_csv(r"Reporte_procesado_2.csv") 


df_cancelados.columns = df_cancelados.columns.str.strip()
df_reporte_enero_limpieza.columns = df_reporte_enero_limpieza.columns.str.strip()


df_cancelados['Folios cancelados sin relación'] = (
    df_cancelados['Folios cancelados sin relación']
    .astype(str)
    .str.strip()
)

df_reporte_enero_limpieza['Folio'] = (
    df_reporte_enero_limpieza['Folio']
    .astype(str)
    .str.strip()
)

folios_a_eliminar = set(
    df_cancelados['Folios cancelados sin relación']
    .dropna()
)

df_re_limpieza_filtrado = df_reporte_enero_limpieza[
    ~df_reporte_enero_limpieza['Folio'].isin(folios_a_eliminar)
]

df_re_limpieza_filtrado.to_csv(
    "Report_enero_limpieza_actualizado_7.csv",
    index=False,
    encoding="utf-8-sig"
)

print("Registros originales:", len(df_reporte_enero_limpieza))
print("Registros eliminados:", len(df_reporte_enero_limpieza) - len(df_re_limpieza_filtrado))
print("Registros finales:", len(df_re_limpieza_filtrado))

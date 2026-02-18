import pandas as pd

""""
Folios que están cancelados y son únicos (Que no están ligados a ningún otro folio)

Salida:
folios_cancelados_sin_relacion.csv

"""
df = pd.read_csv(r"Data\Reporte_enero_procesado.csv")
df.columns = df.columns.str.strip()


df['Folio'] = df['Folio'].astype(str).str.strip()
df['cancelado'] = df['cancelado'].replace('', pd.NA)


df['folios_ligados'] = (
    df['folios_ligados']
    .astype(str)
    .str.strip()
    .replace(['', '[]', 'nan'], pd.NA)
)

folios_referenciados = set()

for val in df['folios_ligados'].dropna():
    ligados = [f.strip() for f in val.split('|') if f.strip() != '']
    folios_referenciados.update(ligados)


cancelados_sin_relacion = df[
    (df['cancelado'].notna()) &                       
    (df['folios_ligados'].isna()) &                   
    (~df['Folio'].isin(folios_referenciados))         
]

df_resultado = cancelados_sin_relacion[['Folio']].copy()
df_resultado.columns = ['Folios cancelados sin relación']

df_resultado.to_csv(
    "folios_cancelados_sin_relacion.csv",
    index=False,
    encoding="utf-8-sig"
)

print("Listo.")

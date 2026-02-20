# Preprocesamiento de datos de incidentes

Pipeline en Python para limpiar y normalizar notas de incidentes, extraer relaciones entre folios y generar un CSV final listo para análisis.

## Objetivo

El proyecto toma un reporte CSV de incidentes (por ejemplo `Limpieza_notas/Reporte_enero.csv`) y ejecuta un flujo de 2 etapas:

1. Procesamiento de notas:
- Extracción de folios relacionados (`Divididos`, `folios_ligados`, `referencia_folio`, `cancelados`).
- Limpieza y normalización del texto de notas.
- Asignación de comisaría por municipio usando `funciones/comisarias.json`.

2. Análisis de relaciones con grafo:
- Construcción de grafo entre folios.
- Detección de componentes conectadas.
- Separación de folios cancelados aislados.

Resultado: un archivo final con registros limpios y filtrados.

## Requisitos

- Python 3.10 o superior (recomendado).
- Paquetes:
  - `pandas`
  - `networkx`
  - `scikit-learn`

Instalación rápida:

```bash
pip install pandas networkx scikit-learn
```

## Uso

### Ejecutar pipeline completo (recomendado)

```bash
python pipeline_preprocesamiento.py \
  --input Limpieza_notas/Reporte_enero.csv \
  --output Reporte_enero_limpieza_final.csv
```

Si no pasas argumentos, usa por defecto:
- Entrada: `Limpieza_notas/Reporte_enero.csv`
- Salida: `Report_enero_limpieza_final.csv`

### Ejecutar solo procesamiento de notas

```bash
python generar_csv_incidentes_procesado.py
```

Genera por defecto `Reporte_procesado_2.csv`.

## Columnas esperadas en el CSV de entrada

Mínimas necesarias para el flujo principal:
- `Folio`
- `Tipo de Incidente`
- `Notas`
- `Municipio`

También se aprovechan otras columnas (por ejemplo `Fecha`, `Hora de Recibido`, `HORA_CIERRE`, `Latitud`, `Longitud`, `Divididos`, `Coordenadas_`) para el archivo final.

## Estructura del proyecto

```text
.
├── pipeline_preprocesamiento.py
├── generar_csv_incidentes_procesado.py
├── Limpieza_notas/
│   └── Reporte_enero.csv
└── funciones/
    ├── procesamiento_notas.py
    ├── notas_extraccion.py
    ├── procesamiento_grafos.py
    ├── asignar_comisaria.py
    ├── abreviaciones.json
    └── comisarias.json
```

## Notas de implementación

- Se excluyen tempranamente los incidentes de tipo `70104`.
- Los folios se consideran válidos para extracción solo si tienen 10 dígitos.
- La codificación de entrada intenta `utf-8` y, si falla, usa `latin1`.

## Próxima mejora sugerida

Agregar `requirements.txt` para fijar dependencias y facilitar despliegue reproducible.

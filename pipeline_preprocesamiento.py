import argparse
import os
import sys


_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if _BASE_DIR not in sys.path:
    sys.path.insert(0, _BASE_DIR)

from generar_csv_incidentes_procesado import procesar_reporte

sys.path.append(os.path.join(_BASE_DIR, "funciones"))
from procesamiento_grafos import construir_grafo, analizar_componentes, separar_folios_cancelados


def ejecutar_pipeline(input_file: str, output_file: str) -> None:
    print(f"Entrada: {input_file}")
    print("Paso 1/2: procesamiento de notas y estructura base...")
    df_procesado = procesar_reporte(input_file=input_file, output_file=None)

    print("Paso 2/2: grafo de relaciones y filtrado de cancelados aislados...")
    grafo = construir_grafo(df_procesado)
    df_componentes = analizar_componentes(grafo)
    _, df_final = separar_folios_cancelados(
        df_original=df_procesado,
        df_grupos=df_componentes,
        output_cancelados=None,
        output_limpio=None,
        output_relaciones=None,
    )

    df_final.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"Archivo final generado: {output_file}")
    print(f"Registros finales: {len(df_final)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline único: un archivo de entrada -> un archivo final sin intermedios."
    )
    parser.add_argument(
        "--input",
        default=os.path.join(_BASE_DIR, "Limpieza_notas", "Reporte_enero.csv"),
        help="Ruta del CSV de entrada.",
    )
    parser.add_argument(
        "--output",
        default=os.path.join(_BASE_DIR, "Report_enero_limpieza_final.csv"),
        help="Ruta del CSV final de salida.",
    )
    args = parser.parse_args()

    ejecutar_pipeline(args.input, args.output)


if __name__ == "__main__":
    main()

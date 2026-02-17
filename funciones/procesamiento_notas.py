import pandas as pd
import re
import json
import os

# 1. Compilación de Regex Global (se hace una sola vez al importar el módulo)

PATRON_DIVIDIO = re.compile(
    r"(?:El incidente |Folio )?(\d+)\s+se dividio\s+(?:a|al folio)\s+(\d+)(?:.*?por ([\w\d() ]+))?",
    re.IGNORECASE
)

PATRON_LIGADO = re.compile(
    r"(?:El incidente |Folio )?(\d+)\s+(?:ha sido |se ha )?ligado\s+(?:al?|con el?)\s*(?:incidente|folio)?\s*(\d+)",
    re.IGNORECASE
)

PATRON_CANCELADO = re.compile(
    r"(?:El incidente |Folio )?(\d+)\s+(?:fue )?cancelado\s+por\s+([\w\d() ]+)",
    re.IGNORECASE
)

PATRON_REFERENCIA = re.compile(
    r"(?:EN )?REFERENCIA\s+(?:A\s+|AL\s+)?(?:FOLIO\s+)?(\d+)",
    re.IGNORECASE
)

PATRON_ESPACIOS = re.compile(r'\s+')


# --- HELPERS DE FORMATO ---

def _es_folio_valido(valor: str) -> bool:
    """
    Valida que un folio tenga exactamente 10 dígitos numéricos.
    """
    s = valor.strip()
    return len(s) == 10 and s.isdigit()


def _lista_str(valores: list) -> list:
    """
    Convierte una lista de valores a List[str], descartando vacíos e inválidos.
    Solo incluye folios de exactamente 10 dígitos numéricos.
    """
    return [str(v).strip() for v in valores if v and _es_folio_valido(str(v))]


def _lista_str_unica(valores: list) -> list:
    """
    Igual que _lista_str pero elimina duplicados manteniendo orden.
    Solo incluye folios de exactamente 10 dígitos numéricos.
    """
    vistos = set()
    resultado = []
    for x in valores:
        if x and _es_folio_valido(str(x)):
            s = str(x).strip()
            if s not in vistos:
                vistos.add(s)
                resultado.append(s)
    return resultado


def _deduplicar_entre_columnas(
    ligados: list,
    referencia: list,
    dividido_a: list,
    dividido_de: list
):
    """
    Elimina folios duplicados entre las cuatro columnas.
    Prioridad: ligados → referencia → dividido_a → dividido_de
    Un folio que ya apareció en una columna anterior se elimina de las siguientes.
    """
    vistos = set()

    def filtrar(lista):
        resultado = []
        for folio in lista:
            if folio not in vistos:
                vistos.add(folio)
                resultado.append(folio)
        return resultado

    return (
        filtrar(ligados),
        filtrar(referencia),
        filtrar(dividido_a),
        filtrar(dividido_de)
    )


# --- CARGA DE ABREVIACIONES ---
RUTA_ABREVIACIONES = os.path.join(os.path.dirname(__file__), 'abreviaciones.json')


def cargar_y_compilar_abreviaciones(ruta_json):
    """
    Carga el JSON de abreviaciones, aplana la estructura y compila una regex optimizada.
    Retorna:
    - mapa_reemplazos: dict { "frase": "reemplazo", ... }
    - patron_regex: objeto re.Pattern para hacer todas las sustituciones
    """
    if not os.path.exists(ruta_json):
        print(f"ADVERTENCIA: No se encontró {ruta_json}. Se omitirá el reemplazo de frases.")
        return {}, None

    try:
        with open(ruta_json, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"ERROR cargando {ruta_json}: {e}")
        return {}, None

    mapa_reemplazos = {}

    for categoria, frases in data.items():
        reemplazo = categoria if categoria.strip() else " "
        for frase in frases:
            mapa_reemplazos[frase.lower()] = reemplazo

    if not mapa_reemplazos:
        return {}, None

    frases_ordenadas = sorted(mapa_reemplazos.keys(), key=len, reverse=True)
    pattern_str = r'\b(' + '|'.join(re.escape(f) for f in frases_ordenadas) + r')\b'
    patron_regex = re.compile(pattern_str, re.IGNORECASE)

    return mapa_reemplazos, patron_regex


MAPA_ABREVIACIONES, PATRON_ABREVIACIONES = cargar_y_compilar_abreviaciones(RUTA_ABREVIACIONES)


def procesar_notas_masivo(series_notas):
    """
    Procesa una Serie de pandas (o lista de strings) conteniendo 'Notas'
    y extrae relaciones (dividio, ligado, cancelado, referencia) de forma masiva y optimizada.

    Regla de extracción:
    - Un número capturado por la regex SOLO se extrae como folio si tiene exactamente
      10 dígitos numéricos. De lo contrario, el match se conserva intacto en la nota limpia.

    Formato de salida garantizado para todas las columnas de listas:
    - Tipo:   List[str]
    - Vacíos: []  (nunca None, nunca strings)

    Los folios extraídos son únicos entre columnas:
    - Prioridad de permanencia: folios_ligados → referencia_folio → dividido_a → dividido_de

    Retorna un DataFrame con las columnas extraídas y la nota limpia.
    """

    valores = series_notas.astype(str).tolist()

    res_dividido_de    = []
    res_dividido_a     = []
    res_cancelado      = []
    res_referencia     = []
    res_folios_ligados = []
    res_nota_limpia    = []

    # Referencias locales para velocidad
    p_div              = PATRON_DIVIDIO
    p_lig              = PATRON_LIGADO
    p_can              = PATRON_CANCELADO
    p_ref              = PATRON_REFERENCIA
    p_esp              = PATRON_ESPACIOS
    p_abreviaciones    = PATRON_ABREVIACIONES
    mapa_abreviaciones = MAPA_ABREVIACIONES

    def repl_abreviaciones(match):
        return mapa_abreviaciones.get(match.group(0).lower(), match.group(0))

    for nota in valores:

        # Vacíos → listas vacías en todas las columnas
        if not nota or nota == 'nan':
            res_dividido_de.append([])
            res_dividido_a.append([])
            res_cancelado.append([])
            res_referencia.append([])
            res_folios_ligados.append([])
            res_nota_limpia.append("")
            continue

        curr_div_de     = []
        curr_div_a      = []
        curr_ligados    = []
        curr_cancelado  = []
        curr_referencia = []

        texto_limpio = nota

        # --- Dividido ---
        # Solo elimina el match si ambos folios (de y a) tienen 10 dígitos
        def repl_div(match):
            folio_de = match.group(1)
            folio_a  = match.group(2)
            if _es_folio_valido(folio_de) and _es_folio_valido(folio_a):
                curr_div_de.append(folio_de)
                curr_div_a.append(folio_a)
                return ""
            return match.group(0)  # conserva en nota si no es válido

        texto_limpio = p_div.sub(repl_div, texto_limpio)

        # --- Ligado ---
        # Solo elimina el match si ambos folios tienen 10 dígitos
        def repl_lig(match):
            folio_1 = match.group(1)
            folio_2 = match.group(2)
            if _es_folio_valido(folio_1) and _es_folio_valido(folio_2):
                curr_ligados.append(folio_1)
                curr_ligados.append(folio_2)
                return ""
            return match.group(0)  # conserva en nota si no es válido

        texto_limpio = p_lig.sub(repl_lig, texto_limpio)

        # --- Cancelado ---
        # Solo elimina el match si el folio tiene 10 dígitos
        def repl_can(match):
            folio = match.group(1)
            if _es_folio_valido(folio):
                curr_cancelado.append(folio)
                return ""
            return match.group(0)  # conserva en nota si no es válido

        texto_limpio = p_can.sub(repl_can, texto_limpio)

        # --- Referencia ---
        # Solo elimina el match si el folio tiene 10 dígitos
        def repl_ref(match):
            folio = match.group(1)
            if _es_folio_valido(folio):
                curr_referencia.append(folio)
                return ""
            return match.group(0)  # conserva en nota si no es válido

        texto_limpio = p_ref.sub(repl_ref, texto_limpio)

        # --- Abreviaciones ---
        if p_abreviaciones:
            texto_limpio = p_abreviaciones.sub(repl_abreviaciones, texto_limpio)

        # --- Limpieza Final ---
        texto_limpio = p_esp.sub(" ", texto_limpio).strip()

        # --- Formato estandarizado antes de deduplicar ---
        ligados_limpios    = _lista_str_unica(curr_ligados)
        referencia_limpia  = _lista_str(curr_referencia)
        dividido_a_limpio  = _lista_str(curr_div_a)
        dividido_de_limpio = _lista_str(curr_div_de)

        # --- Deduplicación entre columnas ---
        # Prioridad: ligados → referencia → dividido_a → dividido_de
        ligados_limpios, referencia_limpia, dividido_a_limpio, dividido_de_limpio = (
            _deduplicar_entre_columnas(
                ligados_limpios,
                referencia_limpia,
                dividido_a_limpio,
                dividido_de_limpio
            )
        )

        # --- Guardar ---
        res_dividido_de.append(dividido_de_limpio)
        res_dividido_a.append(dividido_a_limpio)
        res_cancelado.append(_lista_str(curr_cancelado))
        res_referencia.append(referencia_limpia)
        res_folios_ligados.append(ligados_limpios)
        res_nota_limpia.append(texto_limpio)

    df_resultados = pd.DataFrame({
        "dividido_de":      res_dividido_de,    # List[str], [] si vacío — menor prioridad
        "dividido_a":       res_dividido_a,     # List[str], [] si vacío — sin folios ya en ligados/referencia
        "cancelado":        res_cancelado,      # List[str], [] si vacío
        "referencia_folio": res_referencia,     # List[str], [] si vacío — sin folios ya en ligados
        "folios_ligados":   res_folios_ligados, # List[str] único, [] si vacío — máxima prioridad
        "nota_limpia":      res_nota_limpia,    # str
    })

    return df_resultados
#Se realiza la normalización de las notas obtenidas
import re
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer

MOJIBAKE_RE = re.compile(r'[ÃÂ][\x80-\xBF]')  # típico: Ã± Ã¡ Ã© etc.

# Lista básica de stopwords en español
STOP_WORDS_ES = [
    "de", "la", "que", "el", "en", "y", "a", "los", "del", "se", "las", "por", "un", "para", "con", "no", "una", 
    "su", "al", "lo", "como", "mas", "pero", "sus", "le", "ya", "o", "este", "si", "porque", "esta", "entre", "cuando", 
    "muy", "sin", "sobre", "tambien", "me", "hasta", "hay", "donde", "quien", "desde", "todo", "nos", "durante", 
    "todos", "uno", "les", "ni", "contra", "otros", "ese", "eso", "ante", "ellos", "e", "esto", "mi", "antes", "algunos", 
    "que", "unos", "yo", "otro", "otras", "otra", "el", "tú", "te", "ti"
]

def normalizar_texto_es(texto: object, keep_unk: bool = False, eliminar_stopwords: bool = False) -> str:
    if pd.isna(texto):
        return ""

    s = str(texto)

    # 1) Repara mojibake solo si parece mojibake
    if MOJIBAKE_RE.search(s):
        try:
            s = s.encode("latin1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass

    # 2) Manejo del caracter de reemplazo
    s = s.replace("\ufffd", " <unk> " if keep_unk else " ")

    # 3) Normalización básica y minúsculas
    s = s.lower()

    # 4) Eliminar Fechas (formatos comunes: dd/mm/yyyy, dd-mm-yy)
    #    Busca patrones como 12/05/2023 o 12-05-23
    s = re.sub(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', ' ', s)

    # 5) Eliminar Horas (formatos: HH:MM, HH:MM:SS)
    s = re.sub(r'\b\d{1,2}:\d{2}(:\d{2})?\b', ' ', s)

    # 6) Eliminar puntuación y símbolos, manteniendo números y letras
    #    Reemplazamos por espacio para evitar que "palabra.otra" se convierta en "palabraotra"
    #    o que "10.5" se convierta en "105" (ahora será "10 5")
    s = re.sub(r'[^\w\s]', ' ', s)

    # 7) Eliminar stopwords si se solicita
    if eliminar_stopwords:
        tokens = s.split()
        tokens = [t for t in tokens if t not in STOP_WORDS_ES]
        s = " ".join(tokens)

    # Limpieza final de espacios
    s = re.sub(r"\s+", " ", s).strip()
    return s

def top_ngramas(textos, ngram_range=(2,4), min_df=10, top_k=30, usar_stopwords=True):
    """
    min_df: aparece al menos en min_df notas (no solo frecuencia total)
    usar_stopwords: si True, ignora las stopwords definidas en STOP_WORDS_ES al buscar frases
    """
    stop_words = STOP_WORDS_ES if usar_stopwords else None
    
    vec = CountVectorizer(
        ngram_range=ngram_range, 
        min_df=min_df, 
        stop_words=stop_words
    )
    
    try:
        X = vec.fit_transform(textos)
    except ValueError:
        # Puede pasar si el vocabulario queda vacío
        return []

    frec = X.sum(axis=0).A1
    vocab = vec.get_feature_names_out()

    pares = sorted(zip(vocab, frec), key=lambda x: x[1], reverse=True)[:top_k]
    return pares  # lista de (frase, frecuencia)


def quitar_frases(texto: str, frases: list[str]) -> str:
    s = texto
    # ordenar por longitud para quitar primero las frases largas
    for f in sorted(frases, key=len, reverse=True):
        s = re.sub(rf"\b{re.escape(f)}\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def generar_mapa_sustitucion(pares_ngramas, prefijo="TOKEN"):
    """
    Genera un diccionario mapeando frases a tokens cortos secuenciales.
    Ej: [('frase muy comun', 100)] -> {'frase muy comun': '<<TOKEN_1>>'}
    """ 
    mapa = {}
    for i, (frase, _) in enumerate(pares_ngramas):
        token = f"<<{prefijo}_{i+1}>>"
        mapa[frase] = token
    return mapa

def colapsar_frases_a_tokens(texto: str, mapa: dict[str, str]) -> str:
    s = texto
    # Ordenar claves por longitud descendente es CRÍTICO para evitar reemplazos parciales incorrectos
    for frase, token in sorted(mapa.items(), key=lambda x: len(x[0]), reverse=True):
        # Usamos \b para límites de palabra, asumiendo que las frases son palabras completas
        s = re.sub(rf"\b{re.escape(frase)}\b", f" {token} ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s
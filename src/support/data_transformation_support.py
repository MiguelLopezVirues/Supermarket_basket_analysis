# data processing
import pandas as pd
import numpy as np

# regular expressions
import re 

def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '', filename)


# Function to extract quantity from product name
def extract_quantity_from_product_name(product_name, category_name):
    patterns = {
        "aceite_de_oliva" : r"(\d+(?:[.,]\d+)?\s?(?:l|litros?|ml|mililitros?))",
        "aceite_de_girasol": r"(\d+(?:[.,]\d+)?\s?(?:l|litros?|ml|mililitros?))",
        "leche" : r"(\d+(?:[.,]\d+)?\s?(?:l|litros?|ml|g|gr|cl|g)|\d+\s?(?:uds\.?|botes|x)\s?\d+(?:[.,]\d+)?\s?(?:l|ml|g|gr|cl|g))"
    }

    conversions_magnitude = {'g': 1, 'kg': 1000, 'mg': 0.001, 'l': 1, 'ml': 0.001, 'cl': 0.01}
    conversions_unit = {'g': 'g', 'kg': 'g', 'mg': 'g', 'l': 'l', 'ml': 'l', 'cl': 'l'}

    try:
        quantity_magnitude_unit = re.findall(patterns[category_name], product_name.lower())[0]
        quantity = re.findall(r"(\d+)\s?x", quantity_magnitude_unit)[0]
    except:
        quantity = np.nan

    try:
        units = re.findall(r"\d\s?(\w{1,2})$", quantity_magnitude_unit)[0]
    except:
        units = np.nan

    try:
        magnitude = re.findall(r"(?:\d\s?x\s?)?(\d?\.?\d+)\s?\w{1,2}?", quantity_magnitude_unit.replace(",","."))[0]
    except:
        magnitude = 1

    magnitude = float(magnitude) * conversions_magnitude.get(units, np.nan)
    units = conversions_unit.get(units, np.nan)

    return quantity, magnitude, units


def extract_brand(product_name):
    brands = ["hacendado", "casa juncal", "carrefour", "campomar nature", "la masia", "ybarra", "carbonell", "koipe",
          "la espanola", "natursoy", "dcoop", "k arginano", "oro bailen", "capricho andaluz", "coosur", "de nuestra tierra",
          "oleum", "maestros de hojiblanca", "jaencoop", "guillen", "la laguna", "senorio de segura", "puleva",
          "asturiana", "kaiku", "alcampo", "pascual", "president", "santa teresa", "nivea", "flora", "mustela",
          "babaria", "babybio", "cantero de letur", "ultzama", "movit", "rianxeira", "el buen pastor", "eroski",
          "natura ecologica", "la colmenarena", "larsa", "ram", "vega de oro", "leyma natura", "priegola", "pravia",
          "llet nostra", "mantequilla bujia", "comansi", "montecelio", "caprea", "ecobasic", "artinata", "caprilait",
          "pasqualet", "fageda", "granja noe", "mimosa", "aiguafreda", "lacturale", "el castillo", "rio", "villacorona",
          "arla", "elosol", "diazol", "sveltesse", "ideal", "saha", "etnia", "leyenda", "bove", "valdezarza", "duc",
          "aires de jaen", "cambil", "olea espana", "cuatro esquinas", "quinta aldea", "arroyo de jaen", "mueloliva",
          "finca penamoucho", "coop solera", "beneo", "picualia", "pure bios", "les gallines", "dominus", "cortijo spiritu",
          "al-tabwa", "dos lunas", "la redonda", "quesos casario", "arcos", "aguilar de la frontera", "olivar de segura",
          "tierra de sabor", "coosol", "capicua", "fontasol", "ozolife", "abaco", "aromas del sur", "marques de grinon",
          "nunez de prado", "retama", "ondoliva", "verde segura", "suroliva", "saeta", "oro", "celta", "l.r.", "nestle",
          "president", "lauki", "montbelle", "oleoestepa", "aceites de ardales", "abril", "fuenroble", "olivar del sur",
          "olibeas", "oliva verde", "oleodiel", "oleaurum", "somontano", "oleo cazorla", "mar de olivos", "carbonel",
          "ucasol", "borges", "ondosol", "la masia", "cexasol", "granja noe", "lar", "letona", "lilibet", "lletera",
          "madriz", "unicla", "valles unidos", "auchan", "dia", "hipercor", "danone", "maeva", "santa teresa",
          "ecran sunnique", "nectar of bio", "denenes", "covap", "lanisol", "urzante", "olilan", "palacio de los olivos",
          "nekeas", "carapelli", "hojiblanca", "cazorliva", "arrolan", "saqura", "mil olivas", "don arroniz", "elizondo",
          "beyena", "bomilk", "euskal herria", "bizkaia esnea", "gaza", "el corte ingles", "agus", "alhema de queiles",
          "aljibes", "almaoliva", "amarga y pica", "arboleda", "casas de hualdo", "castillo de canena", "changlot real",
          "conde de benalua", "ester sole", "ferrarini", "flor de arana", "germanor", "go vegg", "hacienda el palo",
          "iznaoliva", "jacoliva", "k arguinano", "l'estornell", "la almazara de canjayar", "la boella", "la organic cuisine",
          "merula", "miro", "molino de olivas de bolea", "pago baldios san carlos", "parqueoliva", "reales almazaras de alcaniz",
          "romanico", "santiveri", "tresces", "unio", "valroble", "venta del baron", "altamira", "ato", "clesa", "ecomil",
          "feiraco", "la yerbera", "el lagar del soto", "el molino d gines", "fruto del sur", "giralda", "karlos arguinano",
          "monegros", "oleocazorla", "laban", "santa gadea", "k. arguinano", "lactebal"]
    
    for brand in brands:
        if brand in product_name:
            return brand
    else:
        return np.nan
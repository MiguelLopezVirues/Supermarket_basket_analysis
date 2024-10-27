# data processing
import pandas as pd
import numpy as np

# regular expressions
import re 

from unidecode import unidecode

from datetime import datetime

def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '', filename)


# Function to extract quantity from product name
def extract_quantity_from_product_name(product_name, category_name):
    patterns = {
        "aceite_de_oliva" : r"(\d+(?:[.,]\d+)?\s?(?:l|litros?|ml|mililitros?))",
        "aceite_de_girasol": r"(\d+(?:[.,]\d+)?\s?(?:l|litros?|ml|mililitros?))",
        "leche" : r"(\d+(?:[.,]\d+)?\s?(?:l|litros?|ml|g|gr|cl|g)|\d+\s?(?:uds\.?|botes|x)\s?\d+(?:[.,]\d+)?\s?(?:l|ml|g|gr|cl|g))"
    }

    conversions_abbr = {
        "gramos": "g",
        "kilogramos": "kg",
        "miligramo": "mg",
        "miligramos": "mg",
        "litros": "l",
        "litro": "l",
        "mililitro": "ml",
        "mililitros": "ml",
        "centilitro": "cl",
        "centilitros": "cl"
        }

    conversions_magnitude = {'g': 1, 'kg': 1000, 'mg': 0.001, 'l': 1, 'ml': 0.001, 'cl': 0.01}
    conversions_unit = {'g': 'g', 'kg': 'g', 'mg': 'g', 'l': 'l', 'ml': 'l', 'cl': 'l'}

    try:
        quantity_magnitude_unit = re.findall(patterns[category_name], product_name.lower())[0]
        quantity = re.findall(r"(\d+)\s?x", quantity_magnitude_unit)[0]
    except:
        quantity = 1

    try:
        units = re.findall(r"\d\s?(\w{1,2})$", quantity_magnitude_unit)[0]
        units = conversions_abbr.get(units,units)
    except:
        units = None

    try:
        magnitude = re.findall(r"(?:\d\s?x\s?)?(\d?\.?\d+)\s?\w{1,2}?", quantity_magnitude_unit.replace(",","."))[0]
    except:
        magnitude = 1

    magnitude = float(magnitude) * conversions_magnitude.get(units, 1)
    units = conversions_unit.get(units, None)

    return quantity, magnitude, units


def extract_brand(product_name):
    brands = ['primer dia de cosecha', 'mendia',
    'natursoy', 'l.r.', 'nunez de prado', 'laban', 'ram', 'hojiblanca', 'oleodiel', "l'estornell", 'president', 
    'la masia', 'la laguna', 'aromas del sur', 'carbonel', 'feiraco', 'carrefour', 'kaiku', 'suroliva', 'ferrarini', 
    'el buen pastor', 'de nuestra tierra', 'aceites de ardales', 'priegola', 'montbelle', 'alhema de queiles', 
    'ecran sunnique', 'almaoliva', 'amarga y pica', 'capricho andaluz', 'carapelli', 'ondosol', 'tierra de sabor', 
    'verde segura', 'eroski', 'larsa', 'ideal', 'jacoliva', 'go vegg', 'lanisol', 'saqura', 'saha', 'oliva verde', 
    'merula', 'oro', 'arrolan', 'la boella', 'reales almazaras de alcaniz', 'carbonell', 'marques de grinon', 
    'finca penamoucho', 'la almazara de canjayar', 'elizondo', 'bomilk', 'mueloliva', 'hacienda el palo', 'fontasol', 
    'euskal herria', 'oro bailen', 'jaencoop', 'cantero de letur', 'miro', 'flor de arana', 'covap', 'cexasol', 
    'babaria', 'parqueoliva', 'gaza', 'pago baldios san carlos', 'lacturale', 'mar de olivos', 'agus', 'lauki', 
    'palacio de los olivos', 'casas de hualdo', 'madriz', 'don arroniz', 'oleoestepa', 'dcoop', 'leyma natura', 
    'borges', 'alcampo', 'giralda', 'duc', 'coosur', 'nekeas', 'santa teresa', 'ucasol', 'dominus', 'lar', 'abril', 
    'beyena', 'romanico', 'ester sole', 'koipe', 'capicua', 'lletera', 'puleva', 'babybio', 'retama', 'granja noe', 
    'nestle', 'santiveri', 'valroble', 'mustela', 'la yerbera', 'clesa', 'campomar nature', 'oleocazorla', 'ozolife', 
    'urzante', 'sveltesse', 'casa juncal', 'olilan', 'k arginano', 'hacendado', 'olivar de segura', 'flora', 
    'el corte ingles', 'picualia', 'la colmenarena', 'unicla', 'guillen', 'celta', 'altamira', 'coosol', 'arboleda', 
    'ybarra', 'conde de benalua', 'saeta', 'maestros de hojiblanca', 'el molino d gines', 'iznaoliva', 'maeva', 
    'denenes', 'lactebal', 'bizkaia esnea', 'la organic cuisine', 'aljibes', 'k. arguinano', 'el lagar del soto', 
    'ecomil', 'fruto del sur', 'olivar del sur', 'mil olivas', 'villacorona', 'valdezarza', 'oleum', 'pascual', 
    'karlos arguinano', 'tresces', 'fuenroble', 'oleo cazorla', 'ato', 'cambil', 'lilibet', 'ondoliva', 'changlot real', 
    'somontano', 'nectar of bio', 'molino de olivas de bolea', 'germanor', 'cazorliva', 'venta del baron', 'elosol', 
    'la redonda', 'olibeas', 'abaco', 'nivea', 'letona', 'santa gadea', 'monegros', 'asturiana', 'rio', 'llet nostra', 
    'danone', 'la espanola', 'castillo de canena', 'valles unidos', 'unio', 'oleaurum', 'senorio de segura', 'ultzama', 
    'el castillo', 'dia'
    ]

    brands = sorted(brands, key=len, reverse=True)

    normalizations = {
        "k arginano": "karlos arguinano",
        "k. arguinano": "karlos arguinano",
        "karlos arguinano": "karlos arguinano",
        "carbonel": "carbonell",
        "el molino d gines": "el molino de gines",
        "la española": "la espanola",
        "oleo cazorla": "oleocazorla",
        "coop": "dcoop",
        "arrolan": "arrolan",
        "oleaestepa": "oleoestepa",
        "bailén": "oro bailen"
    }

    for brand in brands:
        if brand in product_name:
            return normalizations.get(brand,brand)
    else:
        return "otras"

    


def extract_distinction_eco(product_name, category):
    distinction = ""

    if category == "leche":
        if "semidesnatada" in product_name:
            distinction = "semidesnatada"
        elif "desnatada" in product_name:
            distinction = "desnatada"
        elif "entera" in product_name:
            distinction = "entera"

        if not pd.isna(distinction) and "lactosa" in product_name:
            distinction += " sin lactosa"
        if not pd.isna(distinction) and "calcio" in product_name:
            distinction += " calcio"       
        if not pd.isna(distinction) and "proteinas" in product_name:
            distinction += " proteinas"
        if not pd.isna(distinction) and "fresca" in product_name:
            distinction += " fresca"

    if " eco " in product_name or "ecologic" in product_name:
        eco = True
    else:
        eco = False

    return distinction, eco


def extract_subcategory(product_name, category, distinction):
    if category == "aceite_de_girasol":
        if "freir" in product_name:
            subcategory = "freir"
        else:
            subcategory = "normal"


    elif category == "aceite_de_oliva" and "en aceite" not in product_name and "con aceite" not in product_name:
        if "virgen extra" in product_name:
            subcategory = "virgen extra"
        elif "virgen"  in product_name:
            subcategory = "virgen"
        elif "intenso"  in product_name:
            subcategory = "intenso"
        else:
            subcategory = "suave"

    elif category == "leche": # leche
        if "cabra" in product_name:
            subcategory = "cabra"
        elif "vaca" in product_name:
            subcategory = "vaca"
        elif "condensada" in product_name:
            subcategory = "condensada"
        elif "leche" in product_name:
            subcategory = "vaca"
        else: 
            subcategory = "otras"

    else:
        subcategory = "otras"

    return subcategory


def get_subcategory_distinction(product_name, category):
    distinction, eco = extract_distinction_eco(product_name, category)
    subcategory = extract_subcategory(product_name, category, distinction)

    return subcategory, distinction, eco


def create_table_df(table_head_list,table_body_list, product_name, brand_name, quantity, 
        volume_weight, units, subcategory, distinction, eco, category_name, 
        supermarket_name, link):

    table_body_list_filled = [(row[0], row[1].replace(",", "."), product_name, brand_name, quantity, 
        volume_weight, units, subcategory, distinction, eco, category_name, 
        supermarket_name, link) for row in table_body_list]
    

    table_head_list.extend(["product_name", "brand","quantity", "volume_weight", "units",
                            "subcategory", "distinction","eco",
                            "category_name", "supermarket_name", "url"])
    
    table_df = pd.DataFrame(table_body_list_filled, columns=table_head_list)

    return table_df




def get_product_info(link, product_name):

    category_name = link.split("/")[4].replace("-", "_")
    supermarket_name = link.split("/")[3]
    product_name = sanitize_filename(unidecode(product_name.lower()))

    brand_name = extract_brand(product_name)

    quantity, volume_weight, units = extract_quantity_from_product_name(product_name, category_name)

    subcategory, distinction, eco = get_subcategory_distinction(product_name, category_name)

    return product_name, brand_name, quantity, volume_weight, units, subcategory, distinction, eco, category_name, supermarket_name

def parse_date(date_str):
    return datetime.strptime(date_str, '%d/%m/%Y').date()
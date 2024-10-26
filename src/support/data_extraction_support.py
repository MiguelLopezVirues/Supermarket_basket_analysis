# make requests
import requests

# parse static html
from bs4 import BeautifulSoup


# data processing
import pandas as pd
import numpy as np
import time

import os


import re 

def extract_table_from_link(link, product_name):

    # make request
    response = requests.get(link)

    # check response
    if response.status_code == 200:
        # print("Successful connection.")
        pass

    else:
        print("Connection failed.")

    # parse html
    product_data_soup = BeautifulSoup(response.content, "html.parser")

    table = product_data_soup.find("table", {"class":"table table-striped table-responsive text-center"})


    ## EXTRACT
    # extract table header and body
    table_head_list = [element.text.strip() for element in table.find("thead").findAll("th")][:2]

    table_body_list = [[element.text.strip() for element in row.findAll("td")][:2] for row in table.find("tbody").findAll("tr")]


    ## TRANSFORM

    # fill with , product_name, category, supermarket values

    category_name = link.split("/")[4].replace("-","_")

    supermarket_name = link.split("/")[3]

    product_name = product_name.replace("/","_")

    quantity, magnitude, units = extract_quantity_from_product_name(product_name, category_name)

    table_body_list_filled_tuples = [tuple([row[0], row[1].replace(",","."), product_name, quantity, magnitude, units,
                                              category_name, supermarket_name, link]) for row in table_body_list]
    
    # prepare dataframe header
    table_head_list.extend(["product_name", "quantity", "magnitude", "units","category_name","supermarket_name","url"])
    extracted_table_df = pd.DataFrame(table_body_list_filled_tuples, columns=table_head_list)

    ## LOAD
    # function here LOAD TO DATABASE

    # LOAD CSV
    # Define dir_path relative to the script location
    base_path = os.path.dirname(os.path.abspath(__file__))
    dir_path = os.path.join(base_path, "../data/extracted/", supermarket_name, category_name)

    os.makedirs(dir_path, exist_ok=True)

    extracted_table_df.to_csv(f"{dir_path}/{product_name}.csv")


    return extracted_table_df




def extract_quantity_from_product_name(product_name, category_name):

    patterns = {
        "aceite-de-oliva" : r"(\d+(?:[.,]\d+)?\s?(?:l|litros?|ml|mililitros?))",
        "aceite-de-girasol": r"(\d+(?:[.,]\d+)?\s?(?:l|litros?|ml|mililitros?))",
        "leche" : r"(\d+(?:[.,]\d+)?\s?(?:l|litros?|ml|g|gr|cl|g)|\d+\s?(?:uds\.?|botes|x)\s?\d+(?:[.,]\d+)?\s?(?:l|ml|g|gr|cl|g))"
    }

    conversions_magnitude = {
        'g': 1,          # grams as base for weight
        'kg': 1000,      # kilograms to grams
        'mg': 0.001,     # milligrams to grams
        'l': 1,          # liters as base for capacity
        'ml': 0.001,     # milliliters to liters
        'cl': 0.01       # centiliters to liters
    }

    conversions_unit = {
        'g': 'g',          # grams as base for weight
        'kg': 'g',      # kilograms to grams
        'mg': 'g',     # milligrams to grams
        'l': 'l',          # liters as base for capacity
        'ml': 'l',     # milliliters to liters
        'cl': 'l'       # centiliters to liters
    }


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

    magnitude = float(magnitude) * conversions_magnitude.get(units,np.nan)
    units = conversions_unit.get(units,np.nan)

    return quantity, magnitude, units



def extract_productnames_links(link):
    # make request
    response = requests.get(link)

    # check response
    if response.status_code == 200:
        # print("Successful connection.")
        pass

    else:
        print("Connection failed.")

    # parse html
    products_soup = BeautifulSoup(response.content, "html.parser")

    product_cards_grid = products_soup.findAll("div",{"class","row gx-4 gx-lg-5 row-cols-2 row-cols-md-3 row-cols-xl-4 justify-content-center"})[-1]

    product_cards = product_cards_grid.findAll("div",{"class":"card h-100"})

    product_names = [card.find("p").text.strip() for card in product_cards]

    product_links = [card.find("a")["href"] for card in product_cards]

    return product_names, product_links


def extract_categorynames_links(link):

    # make request
    response = requests.get(link)

    # check response
    if response.status_code == 200:
        # print("Successful connection.")
        pass

    else:
        print("Connection failed.")

    # parse html
    categories_soup = BeautifulSoup(response.content, "html.parser")

    category_cards = categories_soup.findAll("div",{"class":"card h-100"})

    category_links = [card.find("a")["href"] for card in category_cards]

    return category_links


def extract_supermarkets(link):

    # make request
    response = requests.get(link)

    # check response
    if response.status_code == 200:
        # print("Successful connection.")
        pass

    else:
        print("Connection failed.")

    # parse html
    main_soup = BeautifulSoup(response.content, "html.parser")

    supermarket_cards = main_soup.findAll("div",{"class":"card h-100"})

    supermarket_links = [card.find("a")["href"] for card in supermarket_cards]

    return supermarket_links
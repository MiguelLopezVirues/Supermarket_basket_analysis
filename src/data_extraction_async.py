import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import numpy as np
from tqdm.asyncio import tqdm_asyncio
import time

from unidecode import unidecode


# Async function to make HTTP requests with retry logic
async def fetch(session, url, retries=3, delay=5):
    for attempt in range(retries):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    print(f"Failed to fetch {url} (status: {response.status})")
                    return None
        except (aiohttp.ClientError, RuntimeError, AttributeError) as e:
            print(f"Error fetching {url}: {e}")
            if attempt < retries - 1:
                print(f"Retrying... ({attempt + 1}/{retries})")
                await asyncio.sleep(delay)
            else:
                print(f"Max retries reached for {url}")
                return None

# Asynchronous function to extract supermarkets
async def extract_supermarkets(session, link):
    content = await fetch(session, link)
    if content:
        main_soup = BeautifulSoup(content, "html.parser")
        supermarket_cards = main_soup.findAll("div", {"class": "card h-100"})
        return [card.find("a")["href"] for card in supermarket_cards]
    return []

# Asynchronous function to extract category names and links
async def extract_categorynames_links(session, link):
    content = await fetch(session, link)
    if content:
        categories_soup = BeautifulSoup(content, "html.parser")
        category_cards = categories_soup.findAll("div", {"class": "card h-100"})
        return [card.find("a")["href"] for card in category_cards]
    return []

# Asynchronous function to extract product names and links
async def extract_productnames_links(session, link):
    content = await fetch(session, link)
    if content:
        products_soup = BeautifulSoup(content, "html.parser")
        product_cards_grid = products_soup.findAll(
            "div", {"class", "row gx-4 gx-lg-5 row-cols-2 row-cols-md-3 row-cols-xl-4 justify-content-center"}
        )[-1]
        product_cards = product_cards_grid.findAll("div", {"class": "card h-100"})
        product_names = [card.find("p").text.strip() for card in product_cards]
        product_links = [card.find("a")["href"] for card in product_cards]
        return product_names, product_links
    return [], []

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

# Function to extract table from product link
async def extract_table_from_link(session, link, product_name):
    content = await fetch(session, link)
    if content:
        product_data_soup = BeautifulSoup(content, "html.parser")
        table = product_data_soup.find("table", {"class": "table table-striped table-responsive text-center"})
        
        if not table:
            return pd.DataFrame()  # If no table is found, return empty DataFrame

        table_head_list = [element.text.strip() for element in table.find("thead").findAll("th")][:2]
        table_body_list = [[element.text.strip() for element in row.findAll("td")][:2]
                           for row in table.find("tbody").findAll("tr")]

        category_name = link.split("/")[4].replace("-", "_")
        supermarket_name = link.split("/")[3]
        product_name = sanitize_filename(product_name)

        quantity, magnitude, units = extract_quantity_from_product_name(product_name, category_name)

        table_body_list_filled = [
            (row[0], row[1].replace(",", "."), product_name, quantity, magnitude, units,
             category_name, supermarket_name, link) for row in table_body_list
        ]

        table_head_list.extend(["product_name", "quantity", "magnitude", "units",
                                "category_name", "supermarket_name", "url"])
        extracted_table_df = pd.DataFrame(table_body_list_filled, columns=table_head_list)


        # Define dir_path relative to the script location
        base_path = os.path.dirname(os.path.abspath(__file__))
        dir_path = os.path.join(base_path, '../data/extracted/', supermarket_name, category_name)

        os.makedirs(dir_path, exist_ok=True)
        extracted_table_df.to_csv(f'{dir_path}/{product_name}.csv')

        return extracted_table_df

    return pd.DataFrame()

# Main async function with retry logic integrated in fetch
async def extract_all_history_prices():
    # Start time
    start_time = time.time()
    total_result_df = pd.DataFrame()

    async with aiohttp.ClientSession() as session:
        supermarket_links = await extract_supermarkets(session, "https://super.facua.org/")

        category_tasks = [extract_categorynames_links(session, link) for link in supermarket_links]
        category_tasks.reverse()
        category_links_lists = await tqdm_asyncio.gather(*category_tasks, desc="Categories")
        category_links = [link for sublist in category_links_lists for link in sublist]

        for category_link in tqdm_asyncio(category_links, desc="Products"):
            product_names, product_links = await extract_productnames_links(session, category_link)

            product_tasks = [extract_table_from_link(session, product_link, product_name)
                             for product_name, product_link in zip(product_names, product_links)]
            product_dfs = await tqdm_asyncio.gather(*product_tasks, desc="Extracting Tables")
            
            for df in product_dfs:
                if not df.empty:
                    total_result_df = pd.concat([total_result_df, df])

    total_result_df.reset_index(drop=True,inplace=True)

    # Define dir_path relative to the script location
    base_path = os.path.dirname(os.path.abspath(__file__))
    dir_path = os.path.join(base_path, '../data/extracted')
    total_result_df.to_csv(f'{dir_path}/facua_extracted_auto.csv')

    # End time
    end_time = time.time()

    # Calculate and print the elapsed time
    elapsed_time = end_time - start_time
    print(f"Computation time: {elapsed_time:.2f} seconds")

    return total_result_df

def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '', filename)

# Run asyncio loop with tqdm
if __name__ == "__main__":
    asyncio.run(extract_all_history_prices())

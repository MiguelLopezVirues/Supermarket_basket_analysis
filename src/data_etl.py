from bs4 import BeautifulSoup
import pandas as pd
import os
import numpy as np
import time

from tqdm import tqdm

from dotenv import load_dotenv
load_dotenv()
database_credentials = {
    "username": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD")
}

from unidecode import unidecode

from support.data_extraction_support import fetch, get_supermarkets_links, get_categories_links, get_product_names_links

from support.data_transformation_support import extract_distinction_eco, extract_subcategory, get_subcategory_distinction
from support.data_transformation_support import extract_brand, extract_quantity_from_product_name, create_table_df, get_product_info, parse_date

from support.data_load_support import save_to_csv, insert_brand, insert_category, insert_product, insert_subcategory
from support.data_load_support import insert_supermarket, insert_supermarket_product, insert_price
from support.data_load_support import connect_to_database, drop_all_tables, create_all_tables


# function to extract, process and load table data from a product link
def get_table_from_product_link_etl(conn, link, product_name):
    content = fetch(link)
    if not content:
        return pd.DataFrame()

    # Parse HTML
    product_data_soup = BeautifulSoup(content, "html.parser")
    table = product_data_soup.find("table", {"class": "table table-striped table-responsive text-center"})
    if not table:
        return pd.DataFrame()  

    # Extract table headers and rows
    table_head_list = [element.text.strip() for element in table.find("thead").findAll("th")][:2]
    table_body_list = [[element.text.strip().replace(",",".") for element in row.findAll("td")][:2]
                       for row in table.find("tbody").findAll("tr")]
    
    product_name = product_data_soup.find("h2").text.strip()

    # Transform data for table and load into database
    product_name, brand_name, quantity, volume_weight, units, subcategory, distinction, eco, category_name, supermarket_name = get_product_info(link, product_name)
    table_df = create_table_df(table_head_list, table_body_list, product_name, brand_name, quantity, volume_weight, units, subcategory, distinction, eco, category_name, supermarket_name, link)


    product_name_norm = category_name.replace("_"," ") + " " + subcategory + " " + distinction

    if eco:
        product_name_norm + " eco"

    # Database insertion
    brand_id = insert_brand(conn, brand_name)
    supermarket_id = insert_supermarket(conn, supermarket_name)
    category_id = insert_category(conn, category_name)
    subcategory_id = insert_subcategory(conn, subcategory, category_id, distinction, eco)
    product_id = insert_product(conn, brand_id, subcategory_id, product_name_norm, quantity, units, volume_weight)
    supermarket_product_id = insert_supermarket_product(conn, supermarket_id, product_id, link, product_name)
    price_table_data = [(supermarket_product_id, parse_date(row[0]), row[1]) for row in table_body_list]
    insert_price(conn, price_table_data)

    # Save as CSV
    save_to_csv(table_df, supermarket_name, category_name, product_name)
    return table_df

# Main function
def main():
    conn = connect_to_database("comparativa_supermercados", database_credentials)
    if not conn:
        print("Failed to connect to database.")
        return
    
    # Create database structure
    drop_all_tables(conn)
    create_all_tables(conn)

    # Measure elapsed time
    start_time = time.time()
    total_result_df = pd.DataFrame()

    e = 0
    # Fetch supermarket links
    supermarket_links = get_supermarkets_links("https://super.facua.org/")
    for supermarket_link in tqdm(supermarket_links, desc="Supermarkets"):

        # Fetch category links for each supermarket
        category_links = get_categories_links(supermarket_link)
        for category_link in tqdm(category_links, desc="Categories", leave=False):
            
            # Fetch product names and links for each category
            product_names, product_links = get_product_names_links(category_link)
            for product_name, product_link in zip(product_names, product_links):
  
                # Extract table and process data for each product
                df = get_table_from_product_link_etl(conn, product_link, product_name)
                if not df.empty:
                    total_result_df = pd.concat([total_result_df, df])
                e += 1

    total_result_df.reset_index(drop=True, inplace=True)
    save_to_csv(total_result_df, final=True)

    # Print elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Computation time: {elapsed_time:.2f} seconds")

    conn.close()
    return total_result_df

# Run main function
if __name__ == "__main__":
    main()

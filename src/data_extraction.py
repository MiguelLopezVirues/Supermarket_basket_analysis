import pandas as pd

from support.data_extraction_support import extract_table_from_link, extract_productnames_links, extract_categorynames_links, extract_supermarkets
from tqdm import tqdm

def extract_all_history_prices():
    total_result_df = pd.DataFrame()

    supermarket_links = extract_supermarkets("https://super.facua.org/")

    for supermarket_link in tqdm(supermarket_links):

        category_links = extract_categorynames_links(supermarket_link)

        for category_link in tqdm(category_links):

            product_names, product_links = extract_productnames_links(category_link)

            for product_name, product_link in tqdm(zip(product_names, product_links)):

                product_df = extract_table_from_link(product_link, product_name)

                total_result_df = pd.concat([total_result_df,product_df])

    total_result_df.to_csv("data/extracted/facua_extracted_auto.csv")
        
    return total_result_df

if __name__ == "__main__":
    extract_all_history_prices()
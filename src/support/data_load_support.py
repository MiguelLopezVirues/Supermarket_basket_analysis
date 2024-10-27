# database agent
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors

# data processing
import pandas as pd

# system
import os


# Database connection setup
def connect_to_database(database, credentials_dict):
    try:
        connection = psycopg2.connect(
            database=database,
            user=credentials_dict["username"],
            password=credentials_dict["password"],
            host="localhost",
            port="5432"
        )
        return connection
    except OperationalError as e:
        if e.pgcode == errorcodes.INVALID_PASSWORD:
            print("Invalid password.")
        elif e.pgcode == errorcodes.CONNECTION_EXCEPTION:
            print("Connection error.")
        else:
            print(f"Error occurred: {e}", e.pgcode)
        return None


def upsert_brand(conn, brand_name):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT brand_id FROM brands WHERE brand_name = %s", (brand_name,)
        )
        brand_id = cursor.fetchone()
        if not brand_id:
            cursor.execute(
                "INSERT INTO brands (brand_name) VALUES (%s) RETURNING brand_id",
                (brand_name,)
            )
            brand_id = cursor.fetchone()[0]
            # print(f"Insercion exitosa id {brand_id}, marca {brand_name}")
            conn.commit()
        else:
            brand_id = brand_id[0]
            # print(f"Marca existente con id {brand_id}, marca {brand_name}")
    return brand_id



def upsert_category(conn, category_name):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT category_id FROM categories WHERE category_name = %s", (category_name,)
        )
        category_id = cursor.fetchone()
        if not category_id:
            cursor.execute(
                "INSERT INTO categories (category_name) VALUES (%s) RETURNING category_id",
                (category_name,)
            )
            category_id = cursor.fetchone()[0]
            # print(f"Insercion exitosa id {category_id},  {category_name}")
            conn.commit()
        else:
            category_id = category_id[0]
            # print(f"Categoria existente con id {category_id},  {category_name}")
    return category_id


def upsert_subcategory(conn, subcategory_name, category_id, distinction=None, eco=False):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT subcategory_id FROM subcategories 
            WHERE subcategory_name = %s 
            AND category_id = %s 
            AND distinction = %s 
            AND eco = %s""",
            (subcategory_name, category_id, distinction, eco)
        )
        subcategory_id = cursor.fetchone()
        if not subcategory_id:
            cursor.execute(
                """
                INSERT INTO subcategories (subcategory_name, category_id, distinction, eco)
                VALUES (%s, %s, %s, %s) RETURNING subcategory_id
                """,
                (subcategory_name, category_id, distinction, eco)
            )
            subcategory_id = cursor.fetchone()[0]
            # print(f"Insercion exitosa id {subcategory_id},  {subcategory_name}")
            conn.commit()
        else:
            subcategory_id = subcategory_id[0]
            # print(f"Subategoria existente con id {subcategory_id},  {subcategory_name}")
    return subcategory_id



def upsert_supermarket(conn, supermarket_name):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT supermarket_id 
            FROM supermarkets 
            WHERE supermarket_name = %s""", (supermarket_name,)
        )
        supermarket_id = cursor.fetchone()
        if not supermarket_id:
            cursor.execute(
                "INSERT INTO supermarkets (supermarket_name) VALUES (%s) RETURNING supermarket_id",
                (supermarket_name,)
            )
            supermarket_id = cursor.fetchone()[0]
            
            conn.commit()
        else:
            supermarket_id = supermarket_id[0]
    return supermarket_id

def upsert_product(conn, brand_id, subcategory_id, product_name_norm, quantity, units, volume_weight):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT product_id FROM products 
            WHERE product_name_norm = %s 
            AND brand_id = %s 
            AND subcategory_id = %s 
            AND quantity = %s 
            AND units = %s 
            AND volume_weight = %s
            """, 
            (product_name_norm, brand_id, subcategory_id, quantity, units, volume_weight)
        )
        product_id = cursor.fetchone()
        if not product_id:
            cursor.execute(
                """
                INSERT INTO products (brand_id, subcategory_id, product_name_norm, quantity, units, volume_weight)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING product_id
                """,
                (brand_id, subcategory_id, product_name_norm, quantity, units, volume_weight)
            )
            product_id = cursor.fetchone()[0]
            # print(f"Insercion exitosa id {product_id},  {product_name_norm}")
            conn.commit()
        else:
            product_id = product_id[0]
            # print(f"Producto existente con id {product_id},  {product_name_norm}")
    return product_id

def upsert_supermarket_product(conn, supermarket_id, product_id, facua_url, product_name_supermarket):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT supermarket_product_id FROM supermarkets_products 
            WHERE supermarket_id = %s 
            AND product_id = %s 
            AND facua_url = %s 
            AND product_name_supermarket = %s
            """,
            (supermarket_id, product_id, facua_url, product_name_supermarket)
        )
        supermarket_product_id = cursor.fetchone()
        if not supermarket_product_id:
            cursor.execute(
                """
                INSERT INTO supermarkets_products (supermarket_id, product_id, facua_url, product_name_supermarket)
                VALUES (%s, %s, %s, %s) RETURNING supermarket_product_id
                """,
                (supermarket_id, product_id, facua_url, product_name_supermarket)
            )
            supermarket_product_id = cursor.fetchone()[0]
            print(f"Insercion exitosa id sup_id {supermarket_product_id}, prod_id {product_id}, name {product_name_supermarket}, facua_url {facua_url}")
            conn.commit()
        else:
            supermarket_product_id = supermarket_product_id[0]
            print(f"Supermarket_product existente sup_id {supermarket_product_id}, prod_id {product_id}, name {product_name_supermarket}, facua_url {facua_url}")
    return supermarket_product_id


def upsert_price(conn, price_table_data):
    with conn.cursor() as cursor:
        for supermarket_product_id, date, price_amount in price_table_data:
  
            cursor.execute(
                """
                SELECT price_id FROM prices 
                WHERE supermarket_product_id = %s 
                AND date = %s
                """,
                (supermarket_product_id, date)
            )
            price_id = cursor.fetchone()

            if not price_id:

                cursor.execute(
                    """
                    INSERT INTO prices (supermarket_product_id, date, price_amount)
                    VALUES (%s, %s, %s)
                    """,
                    (supermarket_product_id, date, price_amount)
                )

        conn.commit()


def save_to_csv(df, supermarket_name=None, category_name=None, product_name=None, final=False):
    base_path = os.path.dirname(os.path.abspath(__file__))
    if not final:
        dir_path = os.path.join(base_path, '../../data/extracted/', supermarket_name, category_name)
        os.makedirs(dir_path, exist_ok=True)
        df.to_csv(f'{dir_path}/{product_name}.csv', index=False)
    else:
        df.to_csv(f'{base_path}/../../data/extracted/facua_extracted_auto.csv', index=False)

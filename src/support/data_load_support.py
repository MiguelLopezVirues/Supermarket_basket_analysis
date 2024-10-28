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

def connect_and_query(database, credentials_dict, query, columns = "query"):

    # establish connection
    connection = connect_to_database(database=database, credentials_dict=credentials_dict)
    cursor = connection.cursor()

    # launch query
    cursor.execute(query)

    # take column names from query or user input
    if columns == "query":
        columns = [desc[0] for desc in cursor.description]
    elif not isinstance(columns, list):
        columns = None

    result_df = pd.DataFrame(cursor.fetchall(), columns=columns)

    # close connection
    cursor.close()
    connection.close()
    
    return result_df

def alter_update_query(database, credentials_dict, alter_update_query):
    # establish connection
    connection = connect_to_database(database=database, credentials_dict=credentials_dict)
    cursor = connection.cursor()

    with cursor:

        cursor.execute(alter_update_query)

        connection.commit()

    cursor.close()
    connection.close()


def drop_all_tables(conn):
    with conn.cursor() as cursor:

        cursor.execute(
            "DROP TABLE IF EXISTS products, categories, subcategories, prices, supermarkets, supermarkets_products, brands CASCADE;"
        )

        conn.commit()

def create_all_tables(conn):
    create_categories(conn)
    create_subcategories(conn)
    create_brands(conn)
    create_supermarkets(conn)
    create_products(conn)
    create_supermarkets_products(conn)
    create_prices(conn)

def create_categories(conn):
    with conn.cursor() as cursor:
        
        cursor.execute(
            """
            CREATE TABLE categories (
                category_id SERIAL PRIMARY KEY,
                category_name VARCHAR(100) NOT NULL
            );
            """
        )

        conn.commit()


def create_subcategories(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE subcategories (
                subcategory_id SERIAL PRIMARY KEY,
                category_id INT REFERENCES categories(category_id) ON DELETE CASCADE ON UPDATE CASCADE,
                subcategory_name VARCHAR(100) NOT NULL,
                distinction VARCHAR(100),
                eco BOOLEAN
            );
            """
        )
        conn.commit()

def create_brands(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE brands (
                brand_id SERIAL PRIMARY KEY,
                brand_name VARCHAR(100) NOT NULL
            );
            """
        )
        conn.commit()

def create_products(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE products (
                product_id SERIAL PRIMARY KEY,
                brand_id INT REFERENCES brands(brand_id) ON DELETE SET NULL ON UPDATE CASCADE,
                subcategory_id INT REFERENCES subcategories(subcategory_id) ON DELETE SET NULL ON UPDATE CASCADE,
                product_name_norm VARCHAR(200) NOT NULL,
                quantity NUMERIC,
                units VARCHAR(50),
                volume_weight NUMERIC
            );
            """
        )
        conn.commit()

def create_supermarkets(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE supermarkets (
                supermarket_id SERIAL PRIMARY KEY,
                supermarket_name VARCHAR(100) NOT NULL
            );
            """
        )
        conn.commit()

def create_supermarkets_products(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE supermarkets_products (
                supermarket_product_id SERIAL PRIMARY KEY,
                supermarket_id INT REFERENCES supermarkets(supermarket_id) ON DELETE CASCADE ON UPDATE CASCADE,
                product_id INT REFERENCES products(product_id) ON DELETE CASCADE ON UPDATE CASCADE,
                facua_url VARCHAR(255),
                product_name_supermarket VARCHAR(200)
            );
            """
        )
        conn.commit()

def create_prices(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE prices (
                price_id SERIAL PRIMARY KEY,
                supermarket_product_id INT REFERENCES supermarkets_products(supermarket_product_id) ON DELETE CASCADE ON UPDATE CASCADE,
                date DATE NOT NULL,
                price_amount NUMERIC(10, 2) NOT NULL
            );
            """
        )
        conn.commit()




def insert_brand(conn, brand_name):
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



def insert_category(conn, category_name):
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


def insert_subcategory(conn, subcategory_name, category_id, distinction=None, eco=False):
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



def insert_supermarket(conn, supermarket_name):
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

def insert_product(conn, brand_id, subcategory_id, product_name_norm, quantity, units, volume_weight):
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

def insert_supermarket_product(conn, supermarket_id, product_id, facua_url, product_name_supermarket):
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


def insert_price(conn, price_table_data):
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

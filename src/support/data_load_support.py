# database agent
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors

# data processing
import pandas as pd

# system
import os

# functions typing
from typing import Optional, Tuple, List, Union, Dict


def drop_all_tables(conn: psycopg2.extensions.connection) -> None:
    """
    Drops all tables from the database with CASCADE.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            "DROP TABLE IF EXISTS products, categories, subcategories, prices, supermarkets, supermarkets_products, brands CASCADE;"
        )
        conn.commit()

def create_all_tables(conn: psycopg2.extensions.connection) -> None:
    """
    Creates all tables in the database by calling specific creation functions.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    """
    create_categories(conn)
    create_subcategories(conn)
    create_brands(conn)
    create_supermarkets(conn)
    create_products(conn)
    create_supermarkets_products(conn)
    create_prices(conn)

def create_categories(conn: psycopg2.extensions.connection) -> None:
    """
    Creates the 'categories' table in the database.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    """
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

def create_subcategories(conn: psycopg2.extensions.connection) -> None:
    """
    Creates the 'subcategories' table in the database with foreign key constraints.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    """
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

def create_brands(conn: psycopg2.extensions.connection) -> None:
    """
    Creates the 'brands' table in the database.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    """
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

def create_products(conn: psycopg2.extensions.connection) -> None:
    """
    Creates the 'products' table in the database with foreign key constraints.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    """
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

def create_supermarkets(conn: psycopg2.extensions.connection) -> None:
    """
    Creates the 'supermarkets' table in the database.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    """
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

def create_supermarkets_products(conn: psycopg2.extensions.connection) -> None:
    """
    Creates the 'supermarkets_products' table with foreign key constraints.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    """
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

def create_prices(conn: psycopg2.extensions.connection) -> None:
    """
    Creates the 'prices' table with foreign key constraints.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    """
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

def insert_brand(conn: psycopg2.extensions.connection, brand_name: str) -> int:
    """
    Inserts a new brand or returns an existing brand ID.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    brand_name : str
        The name of the brand to insert or find.

    Returns:
    -------
    int
        The ID of the brand.
    """
    with conn.cursor() as cursor:
        cursor.execute("SELECT brand_id FROM brands WHERE brand_name = %s", (brand_name,))
        brand_id = cursor.fetchone()
        
        if not brand_id:
            cursor.execute(
                "INSERT INTO brands (brand_name) VALUES (%s) RETURNING brand_id",
                (brand_name,)
            )
            brand_id = cursor.fetchone()[0]
            conn.commit()
        else:
            brand_id = brand_id[0]
    
    return brand_id

def insert_category(conn: psycopg2.extensions.connection, category_name: str) -> int:
    """
    Inserts a new category or returns an existing category ID.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    category_name : str
        The name of the category to insert or find.

    Returns:
    -------
    int
        The ID of the category.
    """
    with conn.cursor() as cursor:
        cursor.execute("SELECT category_id FROM categories WHERE category_name = %s", (category_name,))
        category_id = cursor.fetchone()
        
        if not category_id:
            cursor.execute(
                "INSERT INTO categories (category_name) VALUES (%s) RETURNING category_id",
                (category_name,)
            )
            category_id = cursor.fetchone()[0]
            conn.commit()
        else:
            category_id = category_id[0]
    
    return category_id

def insert_subcategory(conn: psycopg2.extensions.connection, subcategory_name: str, category_id: int, distinction: Optional[str] = None, eco: bool = False) -> int:
    """
    Inserts a new subcategory or returns an existing subcategory ID.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    subcategory_name : str
        The name of the subcategory.
    category_id : int
        The ID of the parent category.
    distinction : Optional[str]
        Additional distinction information.
    eco : bool
        Whether the subcategory is eco-friendly.

    Returns:
    -------
    int
        The ID of the subcategory.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT subcategory_id FROM subcategories 
            WHERE subcategory_name = %s AND category_id = %s AND distinction = %s AND eco = %s
            """,
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
            conn.commit()
        else:
            subcategory_id = subcategory_id[0]
    
    return subcategory_id

def insert_supermarket(conn: psycopg2.extensions.connection, supermarket_name: str) -> int:
    """
    Inserts a new supermarket or returns an existing supermarket ID.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    supermarket_name : str
        The name of the supermarket.

    Returns:
    -------
    int
        The ID of the supermarket.
    """
    with conn.cursor() as cursor:
        cursor.execute("SELECT supermarket_id FROM supermarkets WHERE supermarket_name = %s", (supermarket_name,))
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

def insert_product(conn: psycopg2.extensions.connection, brand_id: Optional[int], subcategory_id: Optional[int], product_name_norm: str, quantity: Optional[float], units: Optional[str], volume_weight: Optional[float]) -> int:
    """
    Inserts a new product or returns an existing product ID.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    brand_id : Optional[int]
        ID of the brand.
    subcategory_id : Optional[int]
        ID of the subcategory.
    product_name_norm : str
        Normalized product name.
    quantity : Optional[float]
        Quantity of the product.
    units : Optional[str]
        Units for the quantity.
    volume_weight : Optional[float]
        Volume or weight of the product.

    Returns:
    -------
    int
        The ID of the product.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT product_id FROM products 
            WHERE product_name_norm = %s AND brand_id = %s AND subcategory_id = %s 
            AND quantity = %s AND units = %s AND volume_weight = %s
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
            conn.commit()
        else:
            product_id = product_id[0]
    
    return product_id

def insert_supermarket_product(conn: psycopg2.extensions.connection, supermarket_id: int, product_id: int, facua_url: str, product_name_supermarket: str) -> int:
    """
    Inserts a new supermarket-product or returns an existing ID.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    supermarket_id : int
        ID of the supermarket.
    product_id : int
        ID of the product.
    facua_url : str
        URL for the product in the supermarket.
    product_name_supermarket : str
        Product name as listed in the supermarket.

    Returns:
    -------
    int
        The ID of the supermarket-product.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT supermarket_product_id FROM supermarkets_products 
            WHERE supermarket_id = %s AND product_id = %s 
            AND facua_url = %s AND product_name_supermarket = %s
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
            conn.commit()
        else:
            supermarket_product_id = supermarket_product_id[0]
    
    return supermarket_product_id

def insert_price(conn: psycopg2.extensions.connection, price_table_data: List[Tuple[int, str, float]]) -> None:
    """
    Inserts prices into the 'prices' table, avoiding duplicates.

    Parameters:
    ----------
    conn : psycopg2.extensions.connection
        Connection to the PostgreSQL database.
    price_table_data : List[Tuple[int, str, float]]
        List of tuples containing supermarket_product_id, date, and price_amount.
    """
    with conn.cursor() as cursor:
        for supermarket_product_id, date, price_amount in price_table_data:
            cursor.execute(
                """
                SELECT price_id FROM prices 
                WHERE supermarket_product_id = %s AND date = %s
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

def save_to_csv(df: pd.DataFrame, supermarket_name: Optional[str] = None, category_name: Optional[str] = None, product_name: Optional[str] = None, final: bool = False) -> None:
    """
    Saves a DataFrame to a CSV file in a structured path.

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame to save.
    supermarket_name : Optional[str]
        Name of the supermarket (for folder structure).
    category_name : Optional[str]
        Name of the category (for folder structure).
    product_name : Optional[str]
        Product name (for file name).
    final : bool
        If True, saves to the final consolidated path.
    """
    base_path = os.path.dirname(os.path.abspath(__file__))
    
    if not final:
        dir_path = os.path.join(base_path, '../../data/extracted/', supermarket_name, category_name)
        os.makedirs(dir_path, exist_ok=True)
        df.to_csv(f'{dir_path}/{product_name}.csv', index=False)
    else:
        df.to_csv(f'{base_path}/../../data/extracted/facua_extracted_auto.csv', index=False)

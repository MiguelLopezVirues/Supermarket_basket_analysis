# database agent
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors

# data processing
import pandas as pd


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
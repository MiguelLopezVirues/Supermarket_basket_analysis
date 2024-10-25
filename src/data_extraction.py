# data processing
import pandas as pd 
import numpy as np

# database agent
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors

# env variables
import os
from dotenv import load_dotenv
load_dotenv()
database_credentials = {
    "username": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD")
}


from support.data_extraction_support import 
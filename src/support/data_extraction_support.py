# make requests
import requests

# parse static html
from bs4 import BeautifulSoup

# scrape dynamic content
from selenium import webdriver 
from webdriver_manager.chrome import ChromeDriverManager  
from selenium.webdriver.common.keys import Keys  
from selenium.webdriver.support.ui import Select 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException 

# data processing
import pandas as pd
import numpy as np
import time

# geographical processing
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="my-geopy-app")

# load environment variables
import dotenv
import os
dotenv.load_dotenv()


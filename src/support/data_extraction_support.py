import requests
from bs4 import BeautifulSoup
import time

def fetch(url, retries=3, delay=5):
    for attempt in range(retries):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.text
            else:
                print(f"Failed to fetch {url} (status: {response.status_code})")
        except (requests.RequestException, RuntimeError, AttributeError) as e:
            print(f"Error fetching {url}: {e}")
            if attempt < retries - 1:
                print(f"Retrying... ({attempt + 1}/{retries})")
                time.sleep(delay)
            else:
                print(f"Max retries reached for {url}")

def get_supermarkets_links(link):
    content = fetch(link)
    if content:
        main_soup = BeautifulSoup(content, "html.parser")
        supermarket_cards = main_soup.findAll("div", {"class": "card h-100"})
        return [card.find("a")["href"] for card in supermarket_cards]
    return []

def get_categories_links(link):
    content = fetch(link)
    if content:
        categories_soup = BeautifulSoup(content, "html.parser")
        category_cards = categories_soup.findAll("div", {"class": "card h-100"})
        return [card.find("a")["href"] for card in category_cards]
    return []

def get_product_names_links(link):
    content = fetch(link)
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

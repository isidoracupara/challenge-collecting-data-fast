import random
from functools import partial

from selenium import webdriver  
from selenium.webdriver.chrome.options import Options 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from fake_useragent import UserAgent

import ujson

from multiprocessing import Pool
#from multiprocessing import Pool

base_url = "https://www.immoweb.be/en/"
listing_url = base_url + "search/apartment/for-sale?countries=BE&page=%s&orderBy=newest"

def load_data():
    try:
        with open('data/temp.json', 'r') as fd:
            print("Loading temp_data.")
            return ujson.load(fd)
    except:
        return dict()
        print("No dump found.")


def serialize_sets(obj):
    if isinstance(obj, set):
        return list(obj)
    return obj


def save_data(temp_data):
    with open('data/temp.json', 'w') as fd:
        ujson.dump(temp_data, fd, default=serialize_sets)
        print("temp_data saved.")


def get_driver():
    chrome_options = Options() 
    chrome_options.add_argument(f'user-agent={UserAgent().random}')
    chrome_options.add_argument("--headless")

    screen_sizes = ("1920,1080", "1680,1050", "1920,1200", "2048,1152", "2048,1536", "2560,1080", "2560,1440", "3440,1440", "3840,2160")
    chrome_options.add_argument("--window-size=%s" % random.sample(screen_sizes, 1)[0])

    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(20)

    return driver


def get_number_of_pages():
    try:
        driver = get_driver()
        driver.get(listing_url % "1")
        page_number = int(driver.find_element(By.XPATH, '//*[@id="searchResults"]/div[3]/div/div/div[1]/div[1]/div/div[1]/div/nav/ul/li[4]/a/span[2]').text)
        driver.close()

        print("Found %d pages to crawl." % page_number)
    except Exception as e:
        print("get_number_of_pages: %s" % e)
    return page_number


def get_urls_from_page(i):
    try:
        driver = get_driver()
        driver.get(listing_url % i)
        urls = driver.find_elements(By.XPATH, "//a[@class='card__title-link']")
        print('Getting links from page %s' % (listing_url % i))
        s = set(el.get_attribute("href").split('?')[0] for el in urls)
        driver.close()
        return s
    except Exception as e:
        print("get_urls_from_page: %s" % e)
        print('Could not retrieve urls from page %d' % i)
        return set()



def get_urls(property_urls):
    page_number = get_number_of_pages()

    pool = Pool()
    for s in pool.map(get_urls_from_page, range(1, page_number + 1)):
        property_urls.update(s)
    return property_urls


def get_property(arg):
    cpt, url = arg
    city, postcode = url.split('/')[-3:-1]
    property = {'url': url, 'city': city.capitalize(), 'postcode': postcode,}
    
    try:
        driver = get_driver() 
        driver.get(url)

        table_labels = tuple(el.text.strip().lower() for el in driver.find_elements(By.XPATH, "//*[contains(@class, 'classified-table__header')]"))
        table_values = tuple(el.text.strip().lower() for el in driver.find_elements(By.XPATH, "//*[contains(@class, 'classified-table__data')]"))

        driver.close()

        property.update(dict(zip(table_labels, table_values)))
        print("[%d]: Got data from url: %s" % (cpt, url))
        return url, property
    except:
        print('Could not retrieve data from property: %s' % url)
        return url, None


def get_properties(property_urls, done, existing_properties):
    urls_to_scrap = property_urls.difference(done)

    pool = Pool()
    for url, property in pool.map(get_property, enumerate(urls_to_scrap)):
        done.add(url)
        if property is not None:
            existing_properties.append(property)

    return done, existing_properties


if __name__ == "__main__":
    temp_data = load_data()
    property_urls = set(temp_data.get('property_urls', list()))
    done = set(temp_data.get('done', list()))
    existing_properties = temp_data.get('existing_properties', list())    

    property_urls = get_urls(property_urls)

    done, existing_properties = get_properties(property_urls, done, existing_properties)

    temp_data = {'property_urls': property_urls, 'done': done, 'existing_properties': existing_properties, }
    save_data(temp_data)
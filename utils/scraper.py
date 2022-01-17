from selenium import webdriver  
from selenium.webdriver.common.keys import Keys  
from selenium.webdriver.chrome.options import Options 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import ujson

from ray.util.multiprocessing import Pool

chrome_options = Options() 
user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
chrome_options.add_argument(f'user-agent={user_agent}')
chrome_options.add_argument("--headless")
chrome_options.add_argument("--window-size=1920,1080")


base_url = "https://www.immoweb.be/en/"
listing_url = base_url + "search/house/for-sale?countries=BE&page=%s&orderBy=newest"



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

def get_number_of_pages(driver):
    # First page
    driver.get(listing_url % "1")
    number_of_pages = driver.find_element(By.XPATH, '//*[@id="searchResults"]/div[3]/div/div/div[1]/div[1]/div/div[1]/div/nav/ul/li[4]/a/span[2]').text
    print(number_of_pages)

def url_generator(driver, number_of_pages):
    for i in range(1, number_of_pages + 1):
        driver.get(listing_url % i)
        urls = driver.find_elements(By.XPATH, "//a[@class='card__title-link']")
        for el in urls:
            yield el.get_attribute("href").split('?')[0]

def get_urls(property_urls):
    for url in url_generator(driver, 10):
        if url not in property_urls:
            property_urls.add(url)
        else:
            break
    return property_urls

def get_property(url):
    driver = webdriver.Chrome(options=chrome_options)  
    driver.implicitly_wait(10)
    driver.get(url)

    city, postcode = url.split('/')[-3:-1]
    table_labels = tuple(el.text.strip().lower() for el in driver.find_elements(By.XPATH, "//*[contains(@class, 'classified-table__header')]"))
    table_values = tuple(el.text.strip().lower() for el in driver.find_elements(By.XPATH, "//*[contains(@class, 'classified-table__data')]"))
    
    driver.close()

    property = {'url':url, 'city':city.capitalize(), 'postcode':postcode,}
    property.update(dict(zip(table_labels, table_values)))
    return url, property

def get_properties(property_urls, done, existing_properties):

    urls_to_scrap = property_urls.difference(done)

    pool = Pool()
    for url, property in pool.map(get_property, urls_to_scrap):
        done.add(url)
        existing_properties.append(property)

    return done, existing_properties
        
if __name__ == "__main__":
    
    
    temp_data = load_data()
    property_urls = set(temp_data.get('property_urls', list()))
    done = set(temp_data.get('done', list()))
    existing_properties = temp_data.get('existing_properties', list())    

    driver = webdriver.Chrome(options=chrome_options)  
    driver.implicitly_wait(10)
    property_urls = get_urls(property_urls)
    driver.close()

    done, existing_properties = get_properties(property_urls, done, existing_properties)

    temp_data = {'property_urls': property_urls, 'done':done, 'existing_properties':existing_properties,}
    save_data(temp_data)

   
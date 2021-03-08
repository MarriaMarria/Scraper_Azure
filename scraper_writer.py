# in the container scraper_writer
import requests
import logging
from bs4 import BeautifulSoup
import unidecode
import unicodedata  # to get rid of xa0 in the string in python
import psycopg2

# logging configuration
logging.basicConfig(filename='scraper_writer.log', 
                    level=logging.INFO, 
                    format=f'%(asctime)s - %(name)s - %(threadName)s - %(message)s')

# connecting to Azure's postgres with psycopg2 library 
logging.info ("connecting to postgresql: start")
connection = psycopg2.connect(
    host="maria-db-scrap.postgres.database.azure.com",\
    port=5432, \
    dbname="postgres", \
    user="mariakononevskaya@maria-db-scrap", \
    password="PlayNice1987")
logging.info ("connecting to postgresql: end")

cursor = connection.cursor()

# creating table in DB
def create_table():
    logging.info("creating table in DB: start")
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS TESTING \
            (id SERIAL PRIMARY KEY, scraped TIMESTAMP default NOW(), title VARCHAR(100),\
            company VARCHAR(100), location VARCHAR(100), \
            salary VARCHAR(100), date_published VARCHAR(100), more_info TEXT)")
        connection.commit()
    except Exception as e:
        logging.info(f"There is an error, code {e}")
    logging.info("creating table in DB: end")

# inserting data to DB
def insert_data(mydata):
    logging.info("inserting data to DB: start")
    try:

        query = "INSERT INTO Job_Offers (title, company, location, salary, date_published, more_info) VALUES (%s, %s, %s, %s, %s, %s)"
        args = mydata
        # print(f"Printing {query}")
        # print(f"Printing {args}")
        cursor.executemany(query, args)
        connection.commit()

    except Exception as e:
        logging.info(f"Couldn't insert into table, error code is: {e}")
    logging.info("inserting data to DB: end")

    connection.close()
        
# accessing url
URL = 'https://fr.indeed.com/jobs?q=developpeur+alternance&l=%C3%8Ele-de-France'
try:
    response = requests.get(URL)
except Exception as e:
    logging.info(f"failed to access url, error {e}")

# parsing html
soup = BeautifulSoup(response.content, 'html.parser')
    #response.text, 'html.parser')
results = soup.find(id='resultsCol')
a_links = results.find_all('a', class_ = 'jobtitle turnstileLink')
divs = results.find_all('div', class_ = 'jobsearch-SerpJobCard')

# lists to fill with data
title_list = []
locations_list = []
date_lists = []
href_list = []
company_list = []
summary_list = []
salary_list = []

def find_salary():
    logging.info("getting salaries: start")
    try:
        salaries = results.find_all('span', class_ = "salaryText")
        for salary in salaries:
            salary = salary.text.strip()
            salary = unicodedata.normalize("NFKD", salary)
            # https://stackoverflow.com/questions/10993612/how-to-remove-xa0-from-string-in-python    
            salary_list.append(salary)
    except Exception as e:
        salaries = " "
        logging.info(f"failed to get any salary info, error {e}")

    return salary_list
    logging.info("getting salaries: end")


def find_and_store_links():
    logging.info("getting job links: start")

    try:
        for link in a_links:
            href = link.get('href')
            href = "https://indeed.fr" + href
            href_list.append(href)
    except Exception as e:
        logging.info(f"failed to get any links, error {e}")
    
    logging.info("getting job links: end")

def find_companies():
    logging.info("getting company names: start")

    try:
        companies = results.find_all('span', class_ = 'company')
        for company in companies:
            text = company.getText()
            text2 = text.strip()
            company_list.append(text2)
        return company_list
    except Exception as e:
        logging.info("couldn't get any company info, error {e}")      
    
    logging.info("getting company names: end")


def find_and_store_titles():
    logging.info("getting job titles: start")
    
    try:
        for title in a_links:
            title = title.get('title')
            unidecodizer = unidecode.unidecode(title)
            title_list.append(unidecodizer)
            title_list.append(title)
    except Exception as e:
        logging.info(f"couldn't get any titles, error {e}")  
    logging.info("getting job links: end")


def find_locations():
    logging.info("getting job location: start")

    try:
        locations = results.find_all(['div', 'span'], {'class': 'location'})
        for location in locations:
            locations_list.append(location.text)
        return locations_list
    except Exception as e:
        logging.info(f"couldn't get any location info, error {e}")
    logging.info("getting job location: end")


def date_when_posted():
    logging.info("getting date when posted: start")

    try:
        date_result = results.findAll('span', {'class': 'date'})
        for date in date_result:
            date = date.text.strip()
            date = unicodedata.normalize("NFKD", date)
            date_lists.append(date)
        return date_lists
    except Exception as e:
        logging.info(f"couldn't get any date when published info, error {e}")
    logging.info("getting date when posted: end")

# saving results from functions
company_var = find_companies()
links_var = find_and_store_links()
titles_var = find_and_store_titles()
locations_var = find_locations()
dates_var = date_when_posted()
salary_var = find_salary()


# print(f"COMPANIES : {company_list}")
# print(f"URLS: {href_list}")
# print(f"TITLES: {title_list}")
# print(f"LOCATIONS: {locations_list}")
# print(f"DATES: {date_lists}")
# print(f"SALARIES: {salary_list}")

# creating list of tuples to insert to DB
logging.info("saving list of tuples")
to_insert_to_DB = list(zip( title_list, company_list, locations_list, salary_list, date_lists, href_list))


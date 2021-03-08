# container api
# port 5002
from flask import Flask, render_template, request, redirect, jsonify
import logging
import json
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

logging.basicConfig(filename='api_flask.log', 
                    level=logging.INFO, 
                    format=f'%(asctime)s - %(name)s - %(threadName)s - %(message)s')

# connecting to Azure's postgres with psycopg2 library 
logging.info("connecting to postgresql: start")

connection = psycopg2.connect(
    host=os.environ.get('host'), \
    port=5432, \
    dbname="postgres", \
    user=os.environ.get('userDB'),\
    password=os.environ.get('passwordDB'))

logging.info("connecting to postgresql: end")

cursor = connection.cursor()

@app.route('/')
def hello():
    app.logger.info('checking homepage')
    return 'Hello you'


# search by city
@app.route('/search/<city>')
def search_by_city(city):
    app.logger.info('searching with city name: start')
    cursor.execute(f"SELECT title FROM job_offers WHERE location = '{city}'")
    # SELECT title FROM job_offers4 WHERE location = 'Paris (75)' // on met dans l'URL Paris (75)
    result = cursor.fetchall()
    return jsonify(result)
    app.logger.info('searching with city name: end')


@app.route('/getting_last_element')
def get_last():
    app.logger.info('searching within last 12 hours: start')
    # w.date_time1 >= (NOW() - INTERVAL '12 hours' )
    cursor.execute("SELECT * FROM job_offers WHERE scraped >= (NOW() - INTERVAL '12 hours' )") 
    result = cursor.fetchall()
    return jsonify(result)
    app.logger.info('searching within last 12 hours: start')


# all info
@app.route('/')
def choose_all():
    app.logger.info('searching all info: start')
    cursor.execute("Select * From job_offers")
    result = cursor.fetchall()
    return jsonify(result)
    # result_json = json.dump(result)
    # print(result_json)
    app.logger.info('searching all info: end')

# all titles
@app.route('/search/title')
def search_by_title():
    app.logger.info('searching by job title: start')
    cursor.execute("SELECT title FROM job_offers")
    result = cursor.fetchall()
    return jsonify(result)
    app.logger.info('searching by job title: end')


@app.route('/search/latest')
def search_date():
    app.logger.info('searching latest upfates start')
    cursor.execute("Select * From job_offers ORDER BY scraped < 24 hours")
    result = cursor.fetchall()
    return jsonify(result)
    app.logger.info('searching by date published: end')


# #search with a string in title (like front, php, angular...)
@app.route('/search/word/<word>')
def search_by_word(word):
    app.logger.info('searching with a string: start')
    cursor.execute(f'SELECT * FROM job_offers WHERE title LIKE "%{word}%"')
                     # SELECT * FROM job_offers4 WHERE title LIKE "%FRONT%";
    result = cursor.fetchall()
    return jsonify(result)
    app.logger.info('searching with a string: end')  


# we search like that http://localhost:4050/search/city=?Paris
@app.route('/search/city')
def city_search():
    city = request.args.get('city')
    return city

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5002, debug=True) 
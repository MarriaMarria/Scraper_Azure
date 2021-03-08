import smtplib, ssl  # smtplib is the built-in Python SMTP protocol client that allows us to connect to our email account and send mail via SMTP.
from email.mime.text import MIMEText  # MIME (Multipurpose Internet Mail Extensions) is a standard for formatting files to be sent over the internet so they can be viewed in a browser or email application.
# from email.mime.base import MimeBase
import datetime
import psycopg2
import logging

# logging configuration
logging.basicConfig(filename='cleaner_sender.log', 
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


def delete_dubles():
    logging.info("deleting doubles: start")
    try:
        cursor.execute("DELETE FROM job_offers AS P1  \
                        USING job_offers AS P2 \
                        WHERE P1.id > P2.id   \
                        AND P1.title = P2.title;")
        connection.commit()
    except Exception as e:
        logging.info(f"There is an error, code {e}")  
    logging.info("deleting doubles: end")
# delete = delete_dubles()

def get_last():
    logging.info("finding last annonces: start")
    try:
        cursor.execute("SELECT * FROM job_offers WHERE scraped >= (NOW() - INTERVAL '24 hours' );")
        result = cursor.fetchall()
        return result
        # json_result = jsonify(result) 
        connection.commit()
    except Exception as e:
        logging.info(f"There is an error, code {e}")
    logging.info("finding last annonces: end")

results = get_last()
print(results)


def send_email():
    logging.info("[def send_email][sending email: start]")
    sender = "maria.clouddev@gmail.com"
    receiver = "maria.clouddev@gmail.com"

    body_of_email = "Lastest annonces from Indeed.com. Don't forget to contact them!!!"
    message = str(results[0][1]).encode('utf-8', errors='ignore')
    message += str(" \n "+results[0][7]).encode('utf-8', errors='ignore') 

    # creating message
    logging.info("[def send_email][creating message]")
    msg = MIMEText(body_of_email, "html")
    msg["Subject"] = "Annonces from the last 12 hours"
    msg["From"] = sender
    msg["To"] = "receiver"

    # sending message
    try:
        logging.info("[def send_email][sending message]")
        server = smtplib.SMTP_SSL(host = "smtp.gmail.com", port = 465)
        server.login(user = "maria.clouddev@gmail.com", password = "CloudDeveloper2021")
        server.sendmail(sender, receiver, message)
        # msg.as_string(result)
        server.quit()

        #https://stackoverflow.com/questions/9942594/unicodeencodeerror-ascii-codec-cant-encode-character-u-xa0-in-position-20

        print("Email sent successfully!")
    except Exception as e:
        logging.info(f"Email not sent, error : {e}")
    logging.info("[def send_email][sending email: end]")




































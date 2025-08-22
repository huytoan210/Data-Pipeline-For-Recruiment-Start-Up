import os
import random
import time
import uuid
import json
from datetime import datetime

import mysql.connector
import pandas as pd
from kafka import KafkaProducer

# --- MySQL Config (inside Docker use service name 'mysql') ---
MYSQL_HOST = os.getenv('MYSQL_HOST', 'mysql')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', '3306'))
MYSQL_DB = os.getenv('MYSQL_DATABASE', 'outputdb')
MYSQL_USER = os.getenv('MYSQL_USER', 'user')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'userpass')

# --- Kafka Config ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'tracking')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_data_from_job():
    with mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DB,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    ) as cnx:
        query = """
        SELECT id AS job_id, campaign_id, group_id, company_id
        FROM job
        """
        return pd.read_sql(query, cnx)

def get_data_from_publisher():
    with mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DB,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    ) as cnx:
        query = """
        SELECT DISTINCT id AS publisher_id
        FROM publisher
        """
        return pd.read_sql(query, cnx)

def generate_and_publish(n_records, job_list, campaign_list, company_list, group_list, publisher_list):
    interact_options = ['click', 'conversion', 'qualified', 'unqualified']
    weights = (70, 10, 10, 10)

    for _ in range(n_records):
        now = datetime.utcnow()
        event = {
            "create_time": str(uuid.uuid1()),
            "bid": float(random.randint(0, 1)),
            "custom_track": random.choices(interact_options, weights=weights)[0],
            "job_id": float(random.choice(job_list)) if job_list else None,
            "publisher_id": float(random.choice(publisher_list)) if publisher_list else None,
            "group_id": float(random.choice(group_list)) if group_list else None,
            "campaign_id": float(random.choice(campaign_list)) if campaign_list else None,
            "ts": now.strftime('%Y-%m-%d %H:%M:%S')
        }
        producer.send(KAFKA_TOPIC, event)
    producer.flush()
    print(f"Published {n_records} records to Kafka topic '{KAFKA_TOPIC}'.")

# Fetch static data once
jobs_data = get_data_from_job()
publisher_data = get_data_from_publisher()

job_list = jobs_data['job_id'].tolist()
campaign_list = jobs_data['campaign_id'].tolist()
company_list = jobs_data['company_id'].tolist()
group_list = jobs_data[jobs_data['group_id'].notnull()]['group_id'].astype(int).tolist()
publisher_list = publisher_data['publisher_id'].tolist()

# Infinite loop to generate and publish periodically
while True:
    n_records = random.randint(1, 20)
    generate_and_publish(n_records, job_list, campaign_list, company_list, group_list, publisher_list)
    time.sleep(20)
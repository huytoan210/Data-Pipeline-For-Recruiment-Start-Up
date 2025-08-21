import random
import time
import uuid
from datetime import datetime

import mysql.connector
import pandas as pd
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.util import uuid_from_time

# --- MySQL Config ---
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3307'
MYSQL_DB = 'outputdb'
MYSQL_USER = 'user'
MYSQL_PASSWORD = 'userpass'

# --- Cassandra Config ---
CASSANDRA_HOSTS = ['127.0.0.1']
CASSANDRA_KEYSPACE = 'logs'
CASSANDRA_USER = 'cassandra'
CASSANDRA_PASSWORD = 'cassandra'

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

def generate_dummy_data(session, n_records, job_list, campaign_list, company_list, group_list, publisher_list):
    prepared = session.prepare("""
    INSERT INTO tracking (create_time, bid, campaign_id, custom_track, group_id, job_id, publisher_id, ts)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)
    
    interact_options = ['click', 'conversion', 'qualified', 'unqualified']
    weights = (70, 10, 10, 10)
    
    for _ in range(n_records):
        now = datetime.now()
        create_time = str(uuid_from_time(now))
        bid = random.randint(0, 1)
        custom_track = random.choices(interact_options, weights=weights)[0]
        job_id = random.choice(job_list)
        publisher_id = random.choice(publisher_list)
        group_id = random.choice(group_list)
        campaign_id = random.choice(campaign_list)
        ts = now.strftime('%Y-%m-%d %H:%M:%S')
        
        session.execute(prepared, (create_time, bid, campaign_id, custom_track, group_id, job_id, publisher_id, ts))
    
    print(f"{n_records} records generated successfully.")

# Establish Cassandra connection
auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
cluster = Cluster(contact_points=CASSANDRA_HOSTS, auth_provider=auth_provider)
session = cluster.connect(CASSANDRA_KEYSPACE)

# Fetch static data once
jobs_data = get_data_from_job()
publisher_data = get_data_from_publisher()

job_list = jobs_data['job_id'].tolist()
campaign_list = jobs_data['campaign_id'].tolist()
company_list = jobs_data['company_id'].tolist()  # Note: company_list was defined but unused; kept for potential future use
group_list = jobs_data[jobs_data['group_id'].notnull()]['group_id'].astype(int).tolist()
publisher_list = publisher_data['publisher_id'].tolist()

# Infinite loop to generate data periodically
while True:
    n_records = random.randint(1, 20)
    generate_dummy_data(session, n_records, job_list, campaign_list, company_list, group_list, publisher_list)
    time.sleep(20)
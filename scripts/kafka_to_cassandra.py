# scripts/kafka_to_cassandra.py
import os
import json
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'tracking')

CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'cassandra')
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'logs')
CASSANDRA_USER = os.getenv('CASSANDRA_USER')  # optional
CASSANDRA_PASSWORD = os.getenv('CASSANDRA_PASSWORD')  # optional

def get_cassandra_session():
    if CASSANDRA_USER and CASSANDRA_PASSWORD:
        auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
        cluster = Cluster(contact_points=[CASSANDRA_HOST], auth_provider=auth_provider)
    else:
        cluster = Cluster(contact_points=[CASSANDRA_HOST])
    session = cluster.connect(CASSANDRA_KEYSPACE)
    session.execute("""
        CREATE TABLE IF NOT EXISTS tracking (
            create_time TEXT PRIMARY KEY,
            bid DOUBLE,
            campaign_id DOUBLE,
            custom_track TEXT,
            group_id DOUBLE,
            job_id DOUBLE,
            publisher_id DOUBLE,
            ts TEXT
        )
    """)
    return session

def main():
    session = get_cassandra_session()
    insert_stmt = session.prepare("""
        INSERT INTO tracking (create_time, bid, campaign_id, custom_track, group_id, job_id, publisher_id, ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='cassandra-writer',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    )

    print(f"Consuming from topic '{KAFKA_TOPIC}' and writing to Cassandra keyspace '{CASSANDRA_KEYSPACE}'.")
    for msg in consumer:
        v = msg.value
        session.execute(
            insert_stmt,
            (
                v.get('create_time'),
                float(v['bid']) if v.get('bid') is not None else None,
                float(v['campaign_id']) if v.get('campaign_id') is not None else None,
                v.get('custom_track'),
                float(v['group_id']) if v.get('group_id') is not None else None,
                float(v['job_id']) if v.get('job_id') is not None else None,
                float(v['publisher_id']) if v.get('publisher_id') is not None else None,
                v.get('ts'),
            )
        )

if __name__ == "__main__":
    main()
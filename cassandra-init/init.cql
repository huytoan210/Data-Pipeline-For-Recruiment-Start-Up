CREATE KEYSPACE IF NOT EXISTS logs
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS tracking (
    create_time TEXT PRIMARY KEY,
    bid DOUBLE,
    bn TEXT,
    campaign_id DOUBLE,
    cd DOUBLE,
    custom_track TEXT,
    de TEXT,
    dl TEXT,
    dt TEXT,
    ed TEXT,
    ev DOUBLE,
    group_id DOUBLE,
    id DOUBLE,
    job_id DOUBLE,
    md TEXT,
    publisher_id DOUBLE,
    rl TEXT,
    sr TEXT,
    ts TEXT,
    tz DOUBLE,
    ua TEXT,
    uid TEXT,
    utm_campaign DOUBLE,
    utm_content DOUBLE,
    utm_medium DOUBLE,
    utm_source TEXT,
    utm_term DOUBLE,
    v DOUBLE,
    vp TEXT
);

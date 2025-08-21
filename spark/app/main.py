import os
import datetime
import time
from uuid import UUID

import time_uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import TimestampType

spark = SparkSession.builder \
    .appName("ETL Cassandra - Spark - MySQL") \
    .config("spark.cassandra.connection.host", os.getenv("CASSANDRA_HOST", "cassandra")) \
    .getOrCreate()

@udf(returnType=TimestampType())
def uuid_to_ts(u: str):
    return time_uuid.TimeUUID(bytes=UUID(u).bytes).get_datetime()


def calculating_clicks(df):
    df.filter(col("custom_track") == "click") \
      .na.fill({"bid": 0, "job_id": 0, "publisher_id": 0, "group_id": 0, "campaign_id": 0}) \
      .createOrReplaceTempView("clicks")

    return spark.sql("""
      SELECT job_id, date(ts) AS date, hour(ts) AS hour,
             publisher_id, campaign_id, group_id,
             AVG(bid) AS bid_set,
             COUNT(*) AS clicks,
             SUM(bid) AS spend_hour
      FROM clicks
      GROUP BY job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id
    """)

def calculating_conversion(df):
    df.filter(col("custom_track") == "conversion") \
      .na.fill({"job_id": 0, "publisher_id": 0, "group_id": 0, "campaign_id": 0}) \
      .createOrReplaceTempView("conversion")

    return spark.sql("""
      SELECT job_id, date(ts) AS date, hour(ts) AS hour,
             publisher_id, campaign_id, group_id,
             COUNT(*) AS conversions
      FROM conversion
      GROUP BY job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id
    """)

def calculating_qualified(df):
    df.filter(col("custom_track") == "qualified") \
      .na.fill({"job_id": 0, "publisher_id": 0, "group_id": 0, "campaign_id": 0}) \
      .createOrReplaceTempView("qualified")

    return spark.sql("""
      SELECT job_id, date(ts) AS date, hour(ts) AS hour,
             publisher_id, campaign_id, group_id,
             COUNT(*) AS qualified
      FROM qualified
      GROUP BY job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id
    """)

def calculating_unqualified(df):
    df.filter(col("custom_track") == "unqualified") \
      .na.fill({"job_id": 0, "publisher_id": 0, "group_id": 0, "campaign_id": 0}) \
      .createOrReplaceTempView("unqualified")

    return spark.sql("""
      SELECT job_id, date(ts) AS date, hour(ts) AS hour,
             publisher_id, campaign_id, group_id,
             COUNT(*) AS unqualified
      FROM unqualified
      GROUP BY job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id
    """)

def process_cassandra_data(df):
    clicks = calculating_clicks(df)
    conv   = calculating_conversion(df)
    qual   = calculating_qualified(df)
    unqual = calculating_unqualified(df)

    return clicks \
        .join(conv,   ["job_id","date","hour","publisher_id","campaign_id","group_id"], "full") \
        .join(qual,   ["job_id","date","hour","publisher_id","campaign_id","group_id"], "full") \
        .join(unqual, ["job_id","date","hour","publisher_id","campaign_id","group_id"], "full")


def retrieve_company_data(jdbc_url, driver, user, password):
    subquery = "(SELECT id AS job_id, company_id, group_id, campaign_id FROM job) AS t"
    return spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", driver) \
        .option("dbtable", subquery) \
        .option("user", user) \
        .option("password", password) \
        .load()


def import_to_mysql(df, jdbc_url, driver, user, password):
    out = df.select(
        "job_id", "date", "hour",
        "publisher_id", "company_id", "campaign_id", "group_id",
        "unqualified", "qualified", "conversions", "clicks", "bid_set", "spend_hour"
    ).withColumnRenamed("date", "dates") \
     .withColumnRenamed("hour", "hours") \
     .withColumnRenamed("qualified", "qualified_application") \
     .withColumnRenamed("unqualified", "disqualified_application") \
     .withColumnRenamed("conversions", "conversion") \
     .withColumn("sources", lit("Cassandra"))

    out.write \
       .format("jdbc") \
       .option("url", jdbc_url) \
       .option("driver", driver) \
       .option("dbtable", "events") \
       .option("user", user) \
       .option("password", password) \
       .mode("append") \
       .save()


def get_latest_time_cassandra():
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="tracking", keyspace="logs") \
        .load() \
        .select("create_time") \
        .withColumn("ts", uuid_to_ts(col("create_time")))

    return df.agg({"ts": "max"}).collect()[0][0]


def get_mysql_latest_time(jdbc_url, driver, user, password):
    sub = "(SELECT MAX(updated_at) AS max_ts FROM events) AS t"
    mysql_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", driver) \
        .option("dbtable", sub) \
        .option("user", user) \
        .option("password", password) \
        .load()

    max_ts = mysql_df.collect()[0][0]
    return max_ts.strftime("%Y-%m-%d %H:%M:%S") if max_ts else "1998-01-01 00:00:00"


if __name__ == "__main__":
    host     = os.getenv("MYSQL_HOST", "mysql")
    port     = os.getenv("MYSQL_PORT", "3306")
    db_name  = os.getenv("MYSQL_DATABASE",   "outputdb")
    user     = os.getenv("MYSQL_USER", "user")
    password = os.getenv("MYSQL_PASSWORD", "userpass")

    jdbc_url = f"jdbc:mysql://{host}:{port}/{db_name}?serverTimezone=UTC"
    driver   = "com.mysql.cj.jdbc.Driver"

    cass_ts  = get_latest_time_cassandra()
    mysql_ts = get_mysql_latest_time(jdbc_url, driver, user, password)
    print(f"Cassandra latest: {cass_ts}, MySQL latest: {mysql_ts}")

    if cass_ts > datetime.datetime.fromisoformat(mysql_ts):
        df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="tracking", keyspace="logs") \
            .load() \
            .select("create_time", "job_id", "custom_track", "bid",
                    "campaign_id", "group_id", "publisher_id") \
            .withColumn("ts", uuid_to_ts(col("create_time"))) \
            .filter(col("job_id").isNotNull())

        cass_output = process_cassandra_data(df)
        company     = retrieve_company_data(jdbc_url, driver, user, password)
        final       = cass_output.join(
                        company,
                        on="job_id",
                        how="left"
                    ).drop(company.group_id).drop(company.campaign_id)
        
        import_to_mysql(final, jdbc_url, driver, user, password)
        print("Imported new data to MySQL.")
    else:
        print("No new data to import.")


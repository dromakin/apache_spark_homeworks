"""
sparkAvroTransfer.py

created by dromakin as 09.01.2022
Project m12_kafkastreams_jvm_azure
"""

__author__ = 'dromakin'
__maintainer__ = 'dromakin'
__credits__ = ['dromakin', ]
__copyright__ = "EPAM, 2022"
__status__ = 'Development'
__version__ = '20220120'

import kafka
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, col, struct
from kafka import KafkaConsumer
import json
import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# BLOB
# Data is in Azure ADLS gen2 storage
ABFS_CONNECTION_STRING = os.getenv("ABFS_CONNECTION_STRING")
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
STORAGE_FILE_SYSTEM_NAME = os.getenv("STORAGE_FILE_SYSTEM_NAME")

# Docker-compose confluent
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")


def spark_init_session():
    """
    Create spark session with configuration to connect to remote ADLS Storage gen 2.

    :return: Spark Session
    """

    session = SparkSession.builder \
        .appName("Spark Avro Loader to Kafka") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-avro_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .getOrCreate()

    # Connection to adls storage
    session.conf.set("fs.azure.account.key.bd201stacc.dfs.core.windows.net", STORAGE_ACCOUNT_KEY)
    session.conf.set("fs.azure.account.key.bd201stacc.blob.core.windows.net", STORAGE_ACCOUNT_KEY)
    return session


if __name__ == '__main__':
    spark = spark_init_session()

    df = spark.read.format("avro").load(ABFS_CONNECTION_STRING + "topics/expedia")

    print(">>>> [INFO] Number of rows: ", df.count())

    df.show(5)

    df.select(to_json(struct([col(c).alias(c) for c in df.columns])).alias("value")).write.format("kafka").option(
        "kafka.bootstrap.servers", BOOTSTRAP_SERVERS).option("topic", KAFKA_TOPIC).save()

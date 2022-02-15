"""
sparkJob.py

created by dromakin as 16.08.2021
Project m06_sparkbasics_python_azure
"""

__author__ = 'dromakin'
__maintainer__ = 'dromakin'
__credits__ = ['dromakin', ]
__copyright__ = "EPAM LLC, 2021"
__status__ = 'Development'
__version__ = '20210815'

# Need to install pysaprk locally
import findspark

findspark.init()

import os

import pygeohash as gh
from opencage.geocoder import OpenCageGeocode
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, year, month, dayofmonth

from azure.storage.filedatalake import (
    DataLakeServiceClient,
)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# BLOB
# Data is in Azure ADLS gen2 storage
ORIG_DATA_SPARK_BASICS_ADLS_SAS_URL = os.getenv("ORIG_DATA_SPARK_BASICS_ADLS_SAS_URL")
ABFS_CONNECTION_STRING = os.getenv("ABFS_CONNECTION_STRING")

# ADSL gen 2 storage
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
STORAGE_CONNECTION_STRING = os.getenv("STORAGE_CONNECTION_STRING")
STORAGE_FILE_SYSTEM_NAME = os.getenv("STORAGE_FILE_SYSTEM_NAME", )
# "abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/<directory-name>"
STORAGE_LINK_STRING_ADLS_WRITE = os.getenv("STORAGE_LINK_STRING_ADLS_WRITE")

# Spark conf for OAUTH
AUTH_TYPE = os.getenv("AUTH_TYPE")
PROVIDER_TYPE = os.getenv("PROVIDER_TYPE")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
CLIENT_ENDPOINT = os.getenv("CLIENT_ENDPOINT")

# Name dataframe
DATAFRAME_NAME = os.getenv("DATAFRAME_NAME")
FINAL_DATAFRAME_DIR = os.getenv("FINAL_DATAFRAME_DIR")

# Geocoder
GEOCODER_KEY = os.getenv("GEOCODER_KEY")


def adls_manager_create_container():
    """
    Create container (file system) on remote ADLS Storage gen 2.

    To create container we need storage account name & key in system ENV.
    """
    service_client = DataLakeServiceClient(
        account_url="{}://{}.dfs.core.windows.net".format(
            "https",
            STORAGE_ACCOUNT_NAME
        ),
        credential=STORAGE_ACCOUNT_KEY
    )

    service_client.create_file_system(file_system=STORAGE_FILE_SYSTEM_NAME)
    service_client.close()


def spark_init_session():
    """
    Create spark session with configuration to connect to remote ADLS Storage gen 2.

    :return: Spark Session
    """
    session = SparkSession.builder \
        .appName("ETL job") \
        .getOrCreate()

    session.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", AUTH_TYPE)
    session.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", PROVIDER_TYPE)
    session.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", CLIENT_ID)
    session.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", CLIENT_SECRET)
    session.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", CLIENT_ENDPOINT)
    # Connection to my adls storage
    # "fs.azure.account.key.<storage-account-name>.dfs.core.windows.net",
    session.conf.set("fs.azure.account.key.stprodwesteurope.dfs.core.windows.net", STORAGE_ACCOUNT_KEY)
    session.conf.set("fs.azure.account.key.stprodwesteurope.blob.core.windows.net", STORAGE_ACCOUNT_KEY)

    return session


def load_df_hotels_from_adls(spark, link_string):
    """
    Load data from remote ADLS Storage gen 2.

    :param spark: Spark Session
    :param link_string: abfss connection string abfss://<STORAGE_CONTAINER>@<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net
    :return: dataframe
    """
    df = spark.read.csv(link_string + "hotels/", header=True)
    return df


def load_df_weather_from_adls(spark, link_string):
    """
    Load data from remote ADLS Storage gen 2.

    :param spark: Spark Session
    :param link_string: abfss connection string abfss://<STORAGE_CONTAINER>@<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net
    :return: dataframe
    """
    df = spark.read.parquet(link_string + "/weather", header=True)
    return df


def get_lat_lng_by_address(geocoder, ids, row, mod):
    """
    Function to update row with latitude or longitude using geocoder API.

    :param geocoder: OpenCageGeocode client
    :param ids: list with rows ids that need to update lat & lng
    :param row: Row from dataframe
    :param mod: str lng or lat - mode to change value in Row
    :return: Row with updated latitude or longitude
    """
    row_dict = row.asDict()
    if row_dict.get("Id") in ids:
        query = f'{row_dict.get("Address")}, {row_dict.get("City")}'
        results = geocoder.geocode(query)

        if mod == "lng":
            row_dict['Longitude'] = str(results[0]['geometry']['lng'])
        elif mod == "lat":
            row_dict['Latitude'] = str(results[0]['geometry']['lat'])
        else:
            assert False, "Smth wrong with ids"

        newrow = Row(**row_dict)
        return newrow
    return row


def geohash_function_hotels(row):
    """
    Calculate geohash for Row from hotel dataframe using latitude and longitude.

    :param row: Row from dataframe
    :return: Updated Row
    """
    row_dict = row.asDict()
    row_dict['Geohash'] = gh.encode(float(row_dict.get("Latitude")), float(row_dict.get("Longitude")), precision=4)
    newrow = Row(**row_dict)
    return newrow


def geohash_function_weather(row):
    """
    Calculate geohash for Row from weather dataframe using latitude and longitude.

    :param row: Row from dataframe
    :return: Updated Row
    """
    row_dict = row.asDict()
    row_dict['Geohash'] = gh.encode(float(row_dict.get("lat")), float(row_dict.get("lng")), precision=4)
    newrow = Row(**row_dict)
    return newrow


def fix_lat(spark, geocoder, df):
    """
    Function to fix dataframe hotels Latitude in Rows.

    :param spark: Spark session
    :param geocoder: OpenCageGeocode client
    :param df: dataframe (hotels)
    :return: updated and fixed dataframe
    """
    ids_lat = [row.Id for row in df.filter(col("Latitude").isNull() | col("Latitude").rlike("NA")).collect()]

    df_hotels_rdd = df.rdd
    df_hotels_rdd_new = df_hotels_rdd.map(lambda row: get_lat_lng_by_address(geocoder, ids_lat, row, mod='lat'))
    df_hotels_lat = spark.createDataFrame(df_hotels_rdd_new)
    return df_hotels_lat


def fix_lng(spark, geocoder, df):
    """
    Function to fix dataframe hotels Longitude in Rows.

    :param spark: Spark session
    :param geocoder: OpenCageGeocode client
    :param df: dataframe (hotels)
    :return: updated and fixed dataframe
    """
    ids_lng = [row.Id for row in df.filter(col("Longitude").isNull() | col("Longitude").rlike("NA")).collect()]

    df_hotels_lat_rdd = df.rdd
    df_hotels_lat_lng_rdd_new = df_hotels_lat_rdd.map(
        lambda row: get_lat_lng_by_address(geocoder, ids_lng, row, mod='lng')
    )
    df_hotels_new = spark.createDataFrame(df_hotels_lat_lng_rdd_new)
    return df_hotels_new


def calc_geohash(spark, df, mode):
    """
    Calculate geohash for hotels and weather for each row in dataframe

    :param spark: Spark session
    :param df: dataframe to calculate geohash
    :param mode: hotels or weather
    :return: updated dataframe
    """
    df_new_rdd = df.rdd
    df_rdd_new = None

    if mode == "hotels":
        df_rdd_new = df_new_rdd.map(lambda row: geohash_function_hotels(row))
    elif mode == "weather":
        df_rdd_new = df_new_rdd.map(lambda row: geohash_function_weather(row))
    else:
        assert False, "mode is not valid"

    df_geohash = spark.createDataFrame(df_rdd_new)
    return df_geohash


def main_spark_job():
    """
    Main Spark Job
    """
    spark = spark_init_session()
    geocoder = OpenCageGeocode(GEOCODER_KEY)

    # Start working with hotels
    df_hotels = load_df_hotels_from_adls(spark, ABFS_CONNECTION_STRING)

    # Checking & fixing Latitude
    df_hotels_lat = fix_lat(spark, geocoder, df_hotels)

    # Checking & fixing Longitude
    df_hotels_lat_lng = fix_lng(spark, geocoder, df_hotels_lat)

    # Calculate geohash for hotel df
    df_hotels_geohash = calc_geohash(spark, df_hotels_lat_lng, "hotels")

    # Start work with weather
    # Union all dfs in one
    df_weather = load_df_weather_from_adls(spark, ABFS_CONNECTION_STRING)

    # calculate geohash for weather df
    df_weather_geohash = calc_geohash(spark, df_weather, "weather")

    # filtering weather data by geohash from hotels
    df_hotels_geohash_list = [str(row['Geohash']) for row in df_hotels_geohash.collect()]
    df_weather_geohash_filter = df_weather_geohash.filter(df_weather_geohash.Geohash.isin(df_hotels_geohash_list))

    # Join dfs
    # Inner Join
    result_df_inner = df_hotels_geohash.join(df_weather_geohash_filter, ["Geohash"])

    # Create container before writing using python client
    # terraform can't create container in adls storage
    adls_manager_create_container()

    # Write to adls container in 1 file:
    # result_df_inner.coalesce(1).write.parquet(STORAGE_LINK_STRING_ADLS_WRITE)

    # Write parquet in many files:
    # result_df_inner.write.parquet(STORAGE_LINK_STRING_ADLS_WRITE)

    # Write result partition by year, month, day:
    result_df_inner.write.partitionBy("year", "month", "day").parquet(STORAGE_LINK_STRING_ADLS_WRITE)

    # Stop spark
    spark.stop()


if __name__ == '__main__':
    main_spark_job()

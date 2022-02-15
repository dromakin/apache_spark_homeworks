"""
test_spark.py

created by dromakin as 16.09.2021
Project m06_sparkbasics_python_azure
"""

__author__ = 'dromakin'
__maintainer__ = 'dromakin'
__credits__ = ['dromakin', ]
__copyright__ = "Cybertonica LLC, London, 2021"
__status__ = 'Development'
__version__ = '20210916'

import os
import pytest

# Need to install pysaprk locally
import findspark

findspark.init()

from opencage.geocoder import OpenCageGeocode
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, StringType, DoubleType

from src.main.python.RemoteGroup.sparkJobRemoteRun import (
    fix_lat,
    fix_lng,
    calc_geohash,
    geohash_function_hotels,
    geohash_function_weather,
)

# Geocoder
GEOCODER_KEY = os.getenv("GEOCODER_KEY", "33674fa87b8c408bb5c8830985f5f741")

hotel_schema = StructType(
    [
        StructField("Id", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("Latitude", StringType(), True),
        StructField("Longitude", StringType(), True),
    ]
)

weather_schema = StructType(
    [
        StructField("lng", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("avg_tmpr_f", DoubleType(), True),
        StructField("avg_tmpr_c", DoubleType(), True),
        StructField("wthr_date", StringType(), True),
        StructField("wthr_year", StringType(), True),
        StructField("wthr_month", StringType(), True),
        StructField("wthr_day", StringType(), True),
    ]
)

hotel_data = [
    {'Id': '2', 'Name': 'Parkside Inn At Incline', 'Country': 'US', 'City': 'Incline Village',
     'Address': '1003 Tahoe Boulevard (sr 28)', 'Latitude': '39.244493', 'Longitude': '-119.936437'},
    {'Id': '8589934592', 'Name': 'Cadillac Motel', 'Country': 'US', 'City': 'Brandywine', 'Address': '16101 Crain Hwy',
     'Latitude': 'NA', 'Longitude': 'NA'},
    {'Id': '17179869184', 'Name': 'Days Inn Brookings', 'Country': 'US', 'City': 'Brookings', 'Address': '2500 6th St',
     'Latitude': '44.31141', 'Longitude': '-96.76286'},
    {'Id': '42949672960', 'Name': 'Americana Resort Properties', 'Country': 'US', 'City': 'Dillon',
     'Address': '135 Main St', 'Latitude': None, 'Longitude': None},
    {'Id': '60129542147', 'Name': 'Ubaa Old Crawford Inn', 'Country': 'US', 'City': 'Des Plaines',
     'Address': '5460 N River Rd', 'Latitude': None, 'Longitude': None},
    {'Id': '455266533383', 'Name': 'Busy B Ranch', 'Country': 'US', 'City': 'Jefferson',
     'Address': '1100 W Prospect Rd', 'Latitude': None, 'Longitude': None},
    {'Id': '1108101562370', 'Name': 'Motel 6', 'Country': 'US', 'City': 'Rockport', 'Address': '106 W 11th St',
     'Latitude': 'NA', 'Longitude': 'NA'},
    {'Id': '1382979469315', 'Name': 'La Quinta', 'Country': 'US', 'City': 'Twin Falls', 'Address': '539 Pole Line Rd',
     'Latitude': None, 'Longitude': None},
]

hotel_data_geohash = [
    {'Id': '2', 'Name': 'Parkside Inn At Incline', 'Country': 'US', 'City': 'Incline Village',
     'Address': '1003 Tahoe Boulevard (sr 28)', 'Latitude': '39.244493', 'Longitude': '-119.936437', 'Geohash': '9qfx'},
    {'Id': '8589934592', 'Name': 'Cadillac Motel', 'Country': 'US', 'City': 'Brandywine', 'Address': '16101 Crain Hwy',
     'Latitude': '38.66893', 'Longitude': '-76.87629', 'Geohash': 'dqc7'},
    {'Id': '17179869184', 'Name': 'Days Inn Brookings', 'Country': 'US', 'City': 'Brookings', 'Address': '2500 6th St',
     'Latitude': '44.31141', 'Longitude': '-96.76286', 'Geohash': '9zgh'},
    {'Id': '42949672960', 'Name': 'Americana Resort Properties', 'Country': 'US', 'City': 'Dillon',
     'Address': '135 Main St', 'Latitude': '39.6286685', 'Longitude': '-106.0451009', 'Geohash': '9xh9'},
    {'Id': '60129542147', 'Name': 'Ubaa Old Crawford Inn', 'Country': 'US', 'City': 'Des Plaines',
     'Address': '5460 N River Rd', 'Latitude': '42.0625805', 'Longitude': '-87.89069', 'Geohash': 'dp3r'},
    {'Id': '455266533383', 'Name': 'Busy B Ranch', 'Country': 'US', 'City': 'Jefferson',
     'Address': '1100 W Prospect Rd', 'Latitude': '38.648378', 'Longitude': '-85.456373', 'Geohash': 'dng5'},
    {'Id': '1108101562370', 'Name': 'Motel 6', 'Country': 'US', 'City': 'Rockport', 'Address': '106 W 11th St',
     'Latitude': '28.02077', 'Longitude': '-97.05601', 'Geohash': '9ufz'},
    {'Id': '1382979469315', 'Name': 'La Quinta', 'Country': 'US', 'City': 'Twin Falls', 'Address': '539 Pole Line Rd',
     'Latitude': '42.56297', 'Longitude': '-114.46087', 'Geohash': '9rwd'},
]

weather_data = [
    {'lng': -88.1964, 'lat': 42.0497, 'avg_tmpr_f': 65.1, 'avg_tmpr_c': 18.4, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29},
    {'lng': -96.9856, 'lat': 44.3281, 'avg_tmpr_f': 66.2, 'avg_tmpr_c': 19.0, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29},
    {'lng': -85.7572, 'lat': 38.5031, 'avg_tmpr_f': 71.1, 'avg_tmpr_c': 21.7, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29},
    {'lng': -114.408, 'lat': 42.545, 'avg_tmpr_f': 78.0, 'avg_tmpr_c': 25.6, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29},
    {'lng': -97.3793, 'lat': 27.9514, 'avg_tmpr_f': 78.7, 'avg_tmpr_c': 25.9, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29},
    {'lng': -120.142, 'lat': 39.2037, 'avg_tmpr_f': 67.1, 'avg_tmpr_c': 19.5, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29},
    {'lng': -76.9781, 'lat': 38.5235, 'avg_tmpr_f': 63.8, 'avg_tmpr_c': 17.7, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29},
    {'lng': -106.009, 'lat': 39.5543, 'avg_tmpr_f': 57.2, 'avg_tmpr_c': 14.0, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29},
]

weather_data_geohash = [
    {'lng': -88.1964, 'lat': 42.0497, 'avg_tmpr_f': 65.1, 'avg_tmpr_c': 18.4, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29, 'Geohash': 'dp3r'},
    {'lng': -96.9856, 'lat': 44.3281, 'avg_tmpr_f': 66.2, 'avg_tmpr_c': 19.0, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29, 'Geohash': '9zgh'},
    {'lng': -85.7572, 'lat': 38.5031, 'avg_tmpr_f': 71.1, 'avg_tmpr_c': 21.7, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29, 'Geohash': 'dng5'},
    {'lng': -114.408, 'lat': 42.545, 'avg_tmpr_f': 78.0, 'avg_tmpr_c': 25.6, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29, 'Geohash': '9rwd'},
    {'lng': -97.3793, 'lat': 27.9514, 'avg_tmpr_f': 78.7, 'avg_tmpr_c': 25.9, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29, 'Geohash': '9ufz'},
    {'lng': -120.142, 'lat': 39.2037, 'avg_tmpr_f': 67.1, 'avg_tmpr_c': 19.5, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29, 'Geohash': '9qfx'},
    {'lng': -76.9781, 'lat': 38.5235, 'avg_tmpr_f': 63.8, 'avg_tmpr_c': 17.7, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29, 'Geohash': 'dqc7'},
    {'lng': -106.009, 'lat': 39.5543, 'avg_tmpr_f': 57.2, 'avg_tmpr_c': 14.0, 'wthr_date': '2017-08-29', 'year': 2017,
     'month': 8, 'day': 29, 'Geohash': '9xh9'},
]


@pytest.fixture
def geocoder():
    return OpenCageGeocode(GEOCODER_KEY)


@pytest.fixture
def spark():
    """
    Create spark session with configuration to connect to remote ADLS Storage gen 2.

    :return: Spark Session
    """
    session = SparkSession.builder \
        .appName("ETL job") \
        .getOrCreate()

    return session


def test_main_fix_lat(spark, geocoder):
    df_hotels = spark.createDataFrame(Row(**x) for x in hotel_data)
    assert df_hotels.filter(col("Latitude").isNull() | col("Latitude").rlike("NA")).count() == 6

    df_hotels_lat = fix_lat(spark, geocoder, df_hotels)
    assert df_hotels_lat.filter(col("Latitude").isNull() | col("Latitude").rlike("NA")).count() == 0


def test_main_fix_lng(spark, geocoder):
    df_hotels = spark.createDataFrame(Row(**x) for x in hotel_data)
    assert df_hotels.filter(col("Longitude").isNull() | col("Longitude").rlike("NA")).count() == 6

    df_hotels_lat = fix_lng(spark, geocoder, df_hotels)
    assert df_hotels_lat.filter(col("Longitude").isNull() | col("Longitude").rlike("NA")).count() == 0


def test_main_geohash_hotels(spark, geocoder):
    df_hotels = spark.createDataFrame(Row(**x) for x in hotel_data)
    assert df_hotels.filter(col("Latitude").isNull() | col("Latitude").rlike("NA")).count() == 6
    assert df_hotels.filter(col("Longitude").isNull() | col("Longitude").rlike("NA")).count() == 6

    df_hotels_lat = fix_lat(spark, geocoder, df_hotels)
    df_hotels_fixed = fix_lng(spark, geocoder, df_hotels_lat)
    assert df_hotels_fixed.filter(col("Latitude").isNull() | col("Latitude").rlike("NA")).count() == 0
    assert df_hotels_fixed.filter(col("Longitude").isNull() | col("Longitude").rlike("NA")).count() == 0

    df_new_rdd = df_hotels_fixed.rdd
    df_rdd_new = df_new_rdd.map(lambda row: geohash_function_hotels(row))
    df_geohash = spark.createDataFrame(df_rdd_new)
    assert df_geohash.filter(col("Geohash").isNull() | col("Geohash").rlike("NA")).count() == 0
    assert df_geohash.filter(col("Geohash").isNotNull()).count() == 8


def test_main_geohash_weather(spark):
    df_weather = spark.createDataFrame(Row(**x) for x in weather_data)
    df_new_rdd = df_weather.rdd
    df_rdd_new = df_new_rdd.map(lambda row: geohash_function_weather(row))
    df_geohash = spark.createDataFrame(df_rdd_new)
    assert df_geohash.filter(col("Geohash").isNotNull()).count() == 8


def test_main_geohash_multi_hotels(spark, geocoder):
    df_hotels = spark.createDataFrame(Row(**x) for x in hotel_data)
    assert df_hotels.filter(col("Latitude").isNull() | col("Latitude").rlike("NA")).count() == 6
    assert df_hotels.filter(col("Longitude").isNull() | col("Longitude").rlike("NA")).count() == 6

    df_hotels_lat = fix_lat(spark, geocoder, df_hotels)
    df_hotels_lat_lng = fix_lng(spark, geocoder, df_hotels_lat)
    assert df_hotels_lat_lng.filter(col("Latitude").isNull() | col("Latitude").rlike("NA")).count() == 0
    assert df_hotels_lat_lng.filter(col("Longitude").isNull() | col("Longitude").rlike("NA")).count() == 0

    df_hotels_geohash = calc_geohash(spark, df_hotels_lat_lng, "hotels")
    assert df_hotels_geohash.filter(col("Geohash").isNull() | col("Geohash").rlike("NA")).count() == 0
    assert df_hotels_geohash.filter(col("Geohash").isNotNull()).count() == 8


def test_main_geohash_multi_weather(spark):
    df_weather = spark.createDataFrame(Row(**x) for x in weather_data)
    df_geohash = calc_geohash(spark, df_weather, "weather")
    assert df_geohash.filter(col("Geohash").isNotNull()).count() == 8


def test_main_geohash_multi_error_check(spark):
    df_weather = spark.createDataFrame(Row(**x) for x in weather_data)

    try:
        calc_geohash(spark, df_weather, "error")
    except AssertionError:
        assert True, "Ok!"
    except:
        assert False, "Another error!"


def test_main_filter_check(spark, geocoder):
    df_hotels = spark.createDataFrame(Row(**x) for x in hotel_data[:5])
    df_hotels_lat = fix_lat(spark, geocoder, df_hotels)
    df_hotels_lat_lng = fix_lng(spark, geocoder, df_hotels_lat)
    df_hotels_geohash = calc_geohash(spark, df_hotels_lat_lng, "hotels")

    df_weather = spark.createDataFrame(Row(**x) for x in weather_data)
    df_weather_geohash = calc_geohash(spark, df_weather, "weather")

    # filtering weather data by geohash from hotels
    df_hotels_geohash_list = [str(row['Geohash']) for row in df_hotels_geohash.collect()]
    df_weather_geohash_filter = df_weather_geohash.filter(df_weather_geohash.Geohash.isin(df_hotels_geohash_list))

    assert df_weather_geohash_filter.count() == 4


def test_main_join(spark, geocoder):
    df_hotels = spark.createDataFrame(Row(**x) for x in hotel_data[:5])
    df_hotels_lat = fix_lat(spark, geocoder, df_hotels)
    df_hotels_lat_lng = fix_lng(spark, geocoder, df_hotels_lat)
    df_hotels_geohash = calc_geohash(spark, df_hotels_lat_lng, "hotels")

    df_weather = spark.createDataFrame(Row(**x) for x in weather_data)
    df_weather_geohash = calc_geohash(spark, df_weather, "weather")

    # filtering weather data by geohash from hotels
    df_hotels_geohash_list = [str(row['Geohash']) for row in df_hotels_geohash.collect()]
    df_weather_geohash_filter = df_weather_geohash.filter(df_weather_geohash.Geohash.isin(df_hotels_geohash_list))

    # inner join
    result_df_inner = df_hotels_geohash.join(df_weather_geohash_filter, ["Geohash"])

    assert result_df_inner.count() == 4
    assert str(result_df_inner.schema.names) == str(['Geohash', 'Id', 'Name', 'Country', 'City', 'Address', 'Latitude',
                                                     'Longitude', 'lng', 'lat', 'avg_tmpr_f', 'avg_tmpr_c', 'wthr_date',
                                                     'year', 'month', 'day'])


if __name__ == '__main__':
    spark = SparkSession.builder \
            .appName("ETL job") \
            .getOrCreate()
    geocoder = OpenCageGeocode(GEOCODER_KEY)
    test_main_fix_lat(spark, geocoder)
    test_main_fix_lng(spark, geocoder)
    test_main_geohash_hotels(spark, geocoder)
    test_main_geohash_weather(spark)
    test_main_geohash_multi_hotels(spark, geocoder)
    test_main_geohash_multi_weather(spark)
    test_main_geohash_multi_error_check(spark)
    test_main_filter_check(spark, geocoder)
    test_main_join(spark, geocoder)

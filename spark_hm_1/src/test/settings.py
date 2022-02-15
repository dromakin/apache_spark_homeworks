"""
settings.py

created by dromakin as 30.08.2021
Project m06_sparkbasics_python_azure
"""

__author__ = 'dromakin'
__maintainer__ = 'dromakin'
__credits__ = ['dromakin', ]
__copyright__ = "Cybertonica LLC, London, 2021"
__status__ = 'Development'
__version__ = '20210830'

import os

import findspark

findspark.init()

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    DoubleType
)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
DATA_DIR = os.path.join(ROOT_DIR, "data")

# BLOB
# Data is in Azure ADLS gen2 storage bd201stacc/m06sparkbasics
ORIG_DATA_SPARK_BASICS_ADLS_SAS_URL = os.getenv("ORIG_DATA_SPARK_BASICS_ADLS_SAS_URL", "https://bd201stacc.blob.core.windows.net/m06sparkbasics?sv=2020-02-10&ss=bfqt&srt=sco&sp=rlx&se=2031-04-08T17:32:24Z&st=2021-04-08T09:32:24Z&spr=https&sig=5An75VCvK%2FCfuPiiWf8knAHhrMNIR%2BE37oUx3b%2FLUQc%3D")
ABFS_CONNECTION_STRING = os.getenv("ABFS_CONNECTION_STRING", "abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/")

# ADSL gen 2 storage
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME", "stprodwesteurope")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY", "AMrNrrFh7IpfKYvB/77FNj24WAEB5lTqIL1FFVROyk/Knwd+lkSqJ899NE6JeqzbHe0SIX5wiFNkqW8uKLNHrg==")
STORAGE_CONNECTION_STRING = os.getenv("STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=stprodwesteurope;AccountKey=AMrNrrFh7IpfKYvB/77FNj24WAEB5lTqIL1FFVROyk/Knwd+lkSqJ899NE6JeqzbHe0SIX5wiFNkqW8uKLNHrg==;EndpointSuffix=core.windows.net")
STORAGE_FILE_SYSTEM_NAME = os.getenv("STORAGE_FILE_SYSTEM_NAME", "sparkbasics")
STORAGE_LINK_STRING_ADLS_WRITE = os.getenv("STORAGE_LINK_STRING_ADLS_WRITE", "abfss://sparkbasics@stprodwesteurope.dfs.core.windows.net/result")

# Spark conf for OAUTH
AUTH_TYPE = os.getenv("AUTH_TYPE", "OAuth")
PROVIDER_TYPE = os.getenv("PROVIDER_TYPE", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
CLIENT_ID = os.getenv("CLIENT_ID", "f3905ff9-16d4-43ac-9011-842b661d556d")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "mAwIU~M4~xMYHi4YX_uT8qQ.ta2.LTYZxT")
CLIENT_ENDPOINT = os.getenv("CLIENT_ENDPOINT", "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token")

# Name dataframe
DATAFRAME_NAME = os.getenv("DATAFRAME_NAME", "Final_df_hotel_weather")
FINAL_DATAFRAME_DIR = os.getenv("FINAL_DATAFRAME_DIR", "Final_df_hotel_weather")

# DF schema
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

# Geocoder
# GEOCODER_KEY = os.getenv("GEOCODER_KEY", "8566ce4a944a43b49d3d81f8c4202719")
GEOCODER_KEY = os.getenv("GEOCODER_KEY", "33674fa87b8c408bb5c8830985f5f741")

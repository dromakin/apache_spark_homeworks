"""
test_adls_source.py

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
import random

from azure.storage.blob import BlobClient
from azure.storage.filedatalake import (
    DataLakeServiceClient,
)
from opencage.geocoder import OpenCageGeocode

ORIG_DATA_SPARK_BASICS_ADLS_SAS_URL = os.getenv("ORIG_DATA_SPARK_BASICS_ADLS_SAS_URL")
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
STORAGE_FILE_SYSTEM_NAME = os.getenv("STORAGE_FILE_SYSTEM_NAME")
GEOCODER_KEY = os.getenv("GEOCODER_KEY")


# https://docs.microsoft.com/en-us/azure/storage/common/storage-samples-python
# https://docs.microsoft.com/en-us/azure/developer/python/sdk/storage/storage-file-datalake-readme?view=storage-py-v2
# https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/storage/azure-storage-blob/samples/blob_samples_common.py
def test_connection_sparkbasics_original_adls_data_sas():
    try:
        blob_client = BlobClient.from_blob_url(blob_url=ORIG_DATA_SPARK_BASICS_ADLS_SAS_URL)
        properties = blob_client.get_blob_properties()
        assert properties is not None
    except:
        assert False, "No access to ADLS storage gen 2 using SAS"


SOURCE_FILE = 'SampleSource.txt'


# help method to provide random bytes to serve as file content
def get_random_bytes(size):
    rand = random.Random()
    result = bytearray(size)
    for i in range(size):
        result[i] = int(rand.random() * 255)
    return bytes(result)


def test_connection_download_upload_sparkbasics_adls_data():
    service_client = DataLakeServiceClient(
        account_url="{}://{}.dfs.core.windows.net".format(
            "https",
            STORAGE_ACCOUNT_NAME
        ),
        credential=STORAGE_ACCOUNT_KEY
    )

    fs_name = "testfs{}".format(random.randint(1, 1000))
    filesystem_client = service_client.create_file_system(file_system=fs_name)

    try:
        file_name = "testfile"
        file_client = filesystem_client.get_file_client(file_name)
        file_client.create_file()

        file_content = get_random_bytes(4 * 1024)

        # append data to the file
        # the data remain uncommitted until flush is performed
        file_client.append_data(data=file_content[0:1024], offset=0, length=1024)
        file_client.append_data(data=file_content[1024:2048], offset=1024, length=1024)
        file_client.append_data(data=file_content[2048:3072], offset=2048, length=1024)
        file_client.append_data(data=file_content[3072:4096], offset=3072, length=1024)

        # data is only committed when flush is called
        file_client.flush_data(len(file_content))

        # read the data back
        download = file_client.download_file()
        downloaded_bytes = download.readall()

        # verify the downloaded content
        assert file_content == downloaded_bytes

        file_client.delete_file()

    except:
        assert False, "No connection to ADLS storage gen 2"
    finally:
        # clean up the demo filesystem
        filesystem_client.delete_file_system()


def test_create_delete_container_filesystem():
    service_client = DataLakeServiceClient(
        account_url="{}://{}.dfs.core.windows.net".format(
            "https",
            STORAGE_ACCOUNT_NAME
        ),
        credential=STORAGE_ACCOUNT_KEY
    )

    try:
        file_system_client = service_client.get_file_system_client(file_system="test")
        assert False
    except:
        service_client.create_file_system(file_system="test")
        service_client.close()
        assert True


# def test_download_sparkbasics_adls_data():
#     service_client = DataLakeServiceClient(
#         account_url="{}://{}.dfs.core.windows.net".format(
#             "https",
#             STORAGE_ACCOUNT_NAME
#         ),
#         credential=STORAGE_ACCOUNT_KEY
#     )
#
#     FILENAME = "part-00000-7b2b2c30-eb5e-4ab6-af89-28fae7bdb9e4-c000.csv.gz"
#
#     file_system_client = service_client.get_file_system_client(file_system=STORAGE_FILE_SYSTEM_NAME)
#
#     directory_client = file_system_client.get_directory_client("hotels")
#     local_file = open(f"./{FILENAME}", 'wb')
#
#     file_client = directory_client.get_file_client(FILENAME)
#
#     download = file_client.download_file()
#     downloaded_bytes = download.readall()
#
#     local_file.write(downloaded_bytes)
#     local_file.close()
#
#     assert os.path.isfile(FILENAME)
#     os.remove(FILENAME)


def test_geocoder_key():
    geocoder = OpenCageGeocode(GEOCODER_KEY)
    results = geocoder.reverse_geocode(44.8303087, -0.5761911)
    assert len(results) == 1


def test_upload_data_to_adls():
    service_client = DataLakeServiceClient(
        account_url="{}://{}.dfs.core.windows.net".format(
            "https",
            STORAGE_ACCOUNT_NAME
        ),
        credential=STORAGE_ACCOUNT_KEY
    )

    fs_name = "testfs{}".format(random.randint(1, 1000))
    filesystem_client = service_client.create_file_system(file_system=fs_name)
    file_name = "testfile"
    file_client = filesystem_client.get_file_client(file_name)
    file_client.create_file()

    file_content = get_random_bytes(4 * 1024)

    # append data to the file
    # the data remain uncommitted until flush is performed
    file_client.append_data(data=file_content[0:1024], offset=0, length=1024)
    file_client.append_data(data=file_content[1024:2048], offset=1024, length=1024)
    file_client.append_data(data=file_content[2048:3072], offset=2048, length=1024)
    file_client.append_data(data=file_content[3072:4096], offset=3072, length=1024)

    # data is only committed when flush is called
    file_client.flush_data(len(file_content))

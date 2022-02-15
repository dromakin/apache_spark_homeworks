# Apache Spark Homeworks

## List of Homeworks
### 1. [Spark Basic Homework](spark_hm_1)
* Fork jvm or python base of the project (please, preserve project naming).
* Data is in Azure ADLS gen2 storage. You can explore it, by connecting Storage Account in Azure Storage Explorer if needed, use this SAS URI in Azure Explorer.
* Create Spark etl job to read data from storage container. For this, you need to add special libraries like hadoop-azure and azure-storage into your project. Details are described here & here. Use ABFS drivers to connect and below OAuth credentials:

* Check hotels data on incorrect (null) values (Latitude & Longitude). For incorrect values map (Latitude & Longitude) from OpenCage Geocoding API in job on fly (Via REST API).
* Generate geohash by Latitude & Longitude using one of geohash libraries (like geohash-java) with 4-characters length in extra column.
* Left join weather and hotels data by generated 4-characters geohash (avoid data multiplication and make you job idempotent)
* Deploy Spark job on Azure Kubernetes Service (AKS), to setup infrastructure use terraform scripts from module. For this use Running Spark on Kubernetes deployment guide and corresponding to your spark version docker image. Default resource parameters (specifically memory) will not work because of free tier limitations. You needed to setup memory and cores properly.  Development and testing is recommended to do locally in your IDE environment.
* Store enriched data (joined data with all the fields from both datasets) in provisioned with terraform Azure ADLS gen2 storage preserving data partitioning in parquet format in “data” container (it marked with prevent_destroy=true and will survive terraform destroy).

### 2. [Spark Basic Homework](spark_hm_2)
* Fork jvm or python base of the project (please, preserve project naming).
* Copy hotel/weather and expedia data from Azure ADLS gen2 storage into provisioned with terraform Azure ADLS gen2 storage.
* Create Databricks Notebooks (Azure libraries like hadoop-azure and azure-storage are already part of Databricks environment, details are described here). Use ABFS drivers and OAuth credentials.
* Create delta tables based on data in storage account.
* Using Spark SQL calculate and visualize in Databricks Notebooks (for queries use hotel_id - join key, srch_ci- checkin, srch_co - checkout:
    1. Top 10 hotels with max absolute temperature difference by month.
    1. Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.
    1. For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.
* For designed queries analyze execution plan. Map execution plan steps with real query. Specify the most time (resource) consuming part. For each analysis you could create tables with proper structure and partitioning if necessary.
* Deploy Databricks Notebook on cluster, to setup infrastructure use terraform scripts from module. Default resource parameters (specifically memory) will not work because of free tier limitations. You needed to setup memory and cores properly.
* Development and testing is recommended to do locally in your IDE environment with delta delta-core library.
* Store final DataMarts and intermediate data (joined data with all the fields from both datasets) in provisioned with terraform Azure ADLS gen2 storage preserving data partitioning in parquet format in “data” container (it marked with prevent_destroy=true and will survive terraform destroy).

### 3. [Spark ML Homework](spark_hm_3)
To complete this notebook, do the following steps:
* Import data from your local machine into the Databricks File System (DBFS)
* Visualize the data using Seaborn and matplotlib
* Run a parallel hyperparameter sweep to train machine learning models on the dataset
* Explore the results of the hyperparameter sweep with MLflow
* Register the best performing model in MLflow
* Apply the registered model to another dataset using a Spark UDF
* Set up model serving for low-latency requests

Expected results:
* Repository with notebook (with output results), configuration scripts, application sources, analysis and etc.
* Upload in task Readme MD file with link on repo, fully documented homework with screenshots and comments.

### 4. [Kafka Basics Homework](spark_hm_4)
This module needs to be done locally. To learn basic Kafka operations, please, complete this quickstart guide (STEPS 1-5).
After you learn Kafka basics proceed with rest of Kafka utilities.
Fork base of the project (please, preserve project naming) and proceed with Clickstream Data Analysis Pipeline Using KSQL (Docker) tutorial.
Tutorial focuses on building real-time analytics of users to determine:

* General website analytics, such as hit count and visitors
* Bandwidth use
* Mapping user-IP addresses to actual users and their location
* Detection of high-bandwidth user sessions
* Error-code occurrence and enrichment
* Sessionization to track user-sessions and understand behavior (such as per-user-session-bandwidth, per-user-session-hits etc)
* The tutorial uses standard streaming functions (i.e., min, max, etc) and enrichment using child tables, stream-table join, and different types of windowing functionality.

Expected results:
* Repository with Docker, configuration scripts, application sources and etc.
* Upload in task Readme MD file with link on repo, fully documented homework with screenshots and comments.

### 5. [Kafka connect in Kubernetes](spark_hm_5)
* Deploy Azure Kubernetes Service (AKS) and Azure Storage Account, to setup infrastructure use terraform scripts from module. Kafka will be deployed in AKS, use Confluent Operator and Confluent Platform for this (Confluent for Kubernetes).
* Modify Kafka Connect to read data from storage container into Kafka topic (expedia). Use this tutorial (Azure Blob Storage Source Connector for Confluent Platform) and credentials
* Before uploading data into Kafka topic, please, mask time from the date field using MaskField transformer like: 2015-08-18 12:37:10 -> 0000-00-00 00:00:00

Expected results:
* Repository with Docker, configuration scripts, application sources and etc.
* Upload in task Readme MD file with link on repo, fully documented homework with screenshots and comments.

### 6. [Kafka connect in Kubernetes](spark_hm_6)
* Write Kafka Streams job to read data from Kafka topic (expedia) and calculate customer's duration of stay as days between requested check-in (srch_ci) and check-out (srch_o) date. Use underling logic to setup proper category.
  * "Erroneous data": null, less than or equal to zero
  * "Short stay": 1-4 days
  * "Standard stay": 5-10 days
  * "Standard extended stay": 11-14 days
  * "Long stay": 2 weeks plus
* Store enriched data in Kafka topic (expedia_ext). Visualized data in Kafka topic (expedia_ext) with KSQL. Show total amount of hotels (hotel_id) and number of distinct hotels (hotel_id) for each category.

Expected results:
* Repository with Docker, configuration scripts, application sources and etc.
* Upload in task Readme MD file with link on repo, fully documented homework with screenshots and comments.

### 7. [Spark Streaming Homework](spark_hm_7)
* Organize incremental copy of hotel/weather data from Azure ADLS gen2 storage into provisioned with terraform Azure ADLS gen2 storage (with a delay, one day per cycle).
* Create Databricks Notebooks (Azure libraries like hadoop-azure and azure-storage are already part of Databricks environment, details are described here). Use ABFS drivers and OAuth credentials like below:
* Create Spark Structured Streaming application with Auto Loader to incrementally and efficiently processes hotel/weather data as it arrives in provisioned Azure ADLS gen2 storage. Using Spark calculate in Databricks Notebooks for each city each day:
    * Number of distinct hotels in the city.
    * Average/max/min temperature in the city.
* Visualize incoming data in Databricks Notebook for 10 biggest cities (the biggest number of hotels in the city, one chart for one city):
    * X-axis: date (date of observation).
    * Y-axis: number of distinct hotels, average/max/min temperature.
* Deploy Databricks Notebook on cluster, to setup infrastructure use terraform scripts from module. Default resource parameters (specifically memory) will not work because of free tier limitations. You needed to setup memory and cores properly.

Expected results:
* Repository with notebook (with output results), configuration scripts, application sources, execution plan dumps, analysis and etc.
* Upload in task Readme MD file with link on repo, fully documented homework with screenshots and comments.

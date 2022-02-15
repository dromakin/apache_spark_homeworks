# Spark Basic Homework
## Original steps

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

## Project Structure

The following project structure will be used


```sh
.
├── README.md
├── notebooks
│   ├── MetadataCreate.sql
│   ├── sparksql.dbc
│   ├── sparksql.html
│   └── sparksql.ipynb
├── requirements.txt
├── setup.py
├── src
│   └── main
│       └── python
│           ├── __init__.py
│           └── sparksql.py
└── terraform
    ├── main.tf
    ├── terraform.plan
    ├── variables.tf
    └── versions.tf

```

# Steps of homework
I start working using Databrick.

## Deploy using terraform
```shell
terraform init -reconfigure \
    -backend-config="storage_account_name=m07" \
    -backend-config="container_name=sparksql" \
    -backend-config="access_key=<STORAGE_KEY>" \
    -backend-config="key=prod.terraform.sparksql"
terraform plan -out terraform.plan
terraform apply terraform.plan
....
terraform destroy
```
* Launch notebooks on Databricks cluster

## Data overview using Azure Storage Explorer
### core directory
![](./data/article_src/data1.png)
### expedia - avro
![](./data/article_src/datadir1.png)
### hotel-weather - parquet with partition by year, month, day
![](./data/article_src/datadir21.png)
![](./data/article_src/datadir22.png)

## Copy data from original ADLS to terraform ADLS Storage gen 2
### Copy data using Data Factory
![](./data/article_src/DataFactory1.png)
### Create Data Factory
![](./data/article_src/DataFactory2.png)
![](./data/article_src/DataFactory4.png)
![](./data/article_src/DataFactory5.png)
![](./data/article_src/DataFactory6.png)
### Data Factory Configuration
![](./data/article_src/DataFactory9.png)
![](./data/article_src/DataFactory10.png)
### Data Factory run pipeline
![](./data/article_src/DataFactory7.png)
### Copy result
![](./data/article_src/DataFactory8.png)

## Working in Databrick's notebook

## Notebook in html
* [sparksql.html](./notebooks/sparksql.html)

### Configure Databrick's spark
![](./data/article_src/spark1.png)

### Create container to upload result and temp data
![](./data/article_src/spark2.png)

### Create Parquet Table
The same action for hotels-weather table.
![](./data/article_src/spark3.png)

### Create Delta Table using Parquet Table
The same action for hotels-weather table.
![](./data/article_src/spark4.png)

## Analyse data using sql
### task 1: Top 10 hotels with max absolute temperature difference by month
![](./data/article_src/task11.png)
![](./data/article_src/task110.png)
### Result task 1
#### Table
![](./data/article_src/task121.png)
#### Plot
![](./data/article_src/task122.png)

### task 2: Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.
#### Sort without join tables use only hotel_id (without partitioning by year)
![](./data/article_src/task23.png)
![](./data/article_src/task231.png)
![](./data/article_src/task232.png)
![](./data/article_src/task233.png)

### Result task 2
#### Table
![](./data/article_src/task241.png)
#### Plot
![](./data/article_src/task24.png)
![](./data/article_src/task242.png)


### task 3: For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.
#### Analyse data and try to understand how to get data of last and first day
![](./data/article_src/task31.png)
![](./data/article_src/task32.png)

#### Get data of last and first day by creating new columns
![](./data/article_src/task33.png)

#### Let's calculate data separatly and then join them
![](./data/article_src/task34.png)
![](./data/article_src/task35.png)
![](./data/article_src/task36.png)

### Result task 3
![](./data/article_src/task37.png)
#### Table
![](./data/article_src/task371.png)
#### Plot
![](./data/article_src/task38.png)

## Save result in ADLS Storage gen 2
### task 1
![](./data/article_src/save1.png)

### task 2
![](./data/article_src/save2.png)
![](./data/article_src/save3.png)

### task 3
![](./data/article_src/save4.png)
![](./data/article_src/save5.png)

### Save joined data with all the fields from both datasets
![](./data/article_src/task21.png)
![](./data/article_src/join22.png)

### temp df
![](./data/article_src/save6.png)
![](./data/article_src/save7.png)

## Overview results in ADLS Storage gen 2
![](./data/article_src/core.png)

### task 1
![](./data/article_src/save_result1.png)

### task 2
![](./data/article_src/save_result2.png)

### task 3
![](./data/article_src/save_result3.png)

### temp df
![](./data/article_src/save_result4.png)
![](./data/article_src/save_result5.png)

## Overview executions plans

### Task 1
![](./data/article_src/ex1.png)
![](./data/article_src/ex2.png)
![](./data/article_src/ex3.png)
![](./data/article_src/ex1cost.png)

### Task 2
![](./data/article_src/ex4.png)
![](./data/article_src/ex5.png)
![](./data/article_src/ex6.png)
![](./data/article_src/ex2cost1.png)
![](./data/article_src/ex2cost2.png)

### Task 3
![](./data/article_src/ex7.png)
![](./data/article_src/ex8.png)
![](./data/article_src/ex9.png)
![](./data/article_src/ex3cost1.png)
![](./data/article_src/ex3cost2.png)

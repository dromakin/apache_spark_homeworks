# Spark ML Homework
## Description
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

# Homework steps
## Data overview
![](./data/article_src/data1.png)
![](./data/article_src/data2.png)

We have csv data in blob.

## Create cluster
* Deploy infrastructure with terraform
```
terraform init
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
### Terminal apply
![](./data/article_src/terraform1.png)
![](./data/article_src/terraform2.png)
![](./data/article_src/terraform3.png)

## Copy notebook and data into Databricks cluster
### Copy notebook
![](./data/article_src/notebook.png)

### Copy data
![](./data/article_src/data.png)

## Configure DBR
### Quota problem
We need to understand that we have limits to use cpu in Databricks.
![](./data/article_src/DBRquota.png)

### DBR problem
Where need to choose DBR version 9.1 LTS.

But if we choose less version we will have problem using SparkTrials():
* Error databrick in DBR: https://github.com/hyperopt/hyperopt/issues/798
* Stackoverflow: https://stackoverflow.com/questions/67272876/hyperopt-spark-3-0-issues
![](./data/article_src/ErrorDBR.png)

We choose DBR version 9.1 LTS then we need to install ml libs using pip:
![](./data/article_src/DBR911.png)
![](./data/article_src/DBR912.png)

### DBR solution
Of course, we can do everything simpler.
We use DBR version 9.1 LTS ML (ML libs already exist in this cluster):
![](./data/article_src/DBRsparkml.png)

Single node:
![](./data/article_src/DBRsparkml2.png)

## Run cells
### Data Visualization and Analysing
![](./data/article_src/analyse1.png)
![](./data/article_src/analyse2.png)

### Building a Baseline Model
![](./data/article_src/baseline.png)

### Registering the model in the MLflow Model Registry
![](./data/article_src/mlflow_model_registry.png)

### Experimenting with a new model
#### Time spend running
![](./data/article_src/new_model1.png)

#### Use MLflow to view the results
![](./data/article_src/new_model2.png)

#### Overview 1 model
![](./data/article_src/new_model3.png)

#### Register the best performing model in MLflow
![](./data/article_src/new_model4.png)
![](./data/article_src/new_model5.png)

### Apply the registered model to another dataset using a Spark UDF
![](./data/article_src/udf1.png)
![](./data/article_src/udf2.png)

### Set up model serving for low-latency requests
#### Error create cluster for serving (quota limit)
![](./data/article_src/reg1.png)
![](./data/article_src/reg2.png)

#### Solving this problem
Create cluster in another location and register our model:
![](./data/article_src/reg3.png)

#### Result
![](./data/article_src/reg4.png)

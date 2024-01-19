# Generative AI solutions for a Data Lake

## Dataset
We are using the [Amazon Product Review Dataset](https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/#complete-data). The citation for this dataset is:

    Justifying recommendations using distantly-labeled reviews and fined-grained aspects
    Jianmo Ni, Jiacheng Li, Julian McAuley
    Empirical Methods in Natural Language Processing (EMNLP), 2019


## Installation 

Clone the git repo. Then:

    cd cdk
    npm install
    cdk synth
    cdk deploy

Note the S3 bucket name output, which will look like this:

    GenAIDataLake.S3BucketName = genaidatalake-s3bucketXXX-YYYY

## Walking through the scenario

### Log in to SageMaker Studio

The CDK stack created a SageMaker Studio domain and user for you. Open the SageMaker console and select `Domains`. Click on the domain `GenAIDataLakeDomain`.

Under `Users`, you should see a user named `genaidatalakeuser`. Launch Studio for this user.

### Create a JupyterLab space

Once Studio launches, go to `JupyterLab` in the list of applications on the left toolbar. Click `Create JupyterLab space` and give it a name. On the next screen change the instance type to `m5.2xlarge` and set the volume size to 300 GB.

Now run the space. Once it launches, click `Open JupyterLab`.

### Clone repo

In JupyterLab, open a terminal, and clone the GitHub repository.

    git clone https://github.com/aws-samples/amazon-genai-datalake

In the file tree on the left, go into the folder `genai-datalake`.

### Download dataset

Now in SageMaker Studio, go into the `notebooks` folder and open the notebook `dataset.ipynb`. In the first code cell, change the variable `bucket` to reflect the name of the S3 bucket created by the CDK stack.

Execute the rest of the cells. Wait for the upload to S3 to complete. This may take well over an hour in total.

### Access EMR Studio

For these next steps we'll be working in EMR Serverless via notebooks in EMR Studio. Go to the EMR console and click on `Studios` in the `EMR Studio` section of the sidebar.

Now click on the `Studio Access URL` link for the Studio called `GenAI-Datalake`.

Next click `Create Workspace`. On the next screen give it a name and click `Create Workspace`.

An EMR Studio JupyterLab should now open in a new browser tab. 

### Enable interactive endpoint

In the EMR Studio console, access the `GenAIDataLakeApp` application and enable interactive endpoints.

### Clone repo

In JupyterLab, open a terminal, and clone the GitHub repository.

    git clone https://github.com/aws-samples/amazon-genai-datalake

In the file tree on the left, go into the folder `genai-datalake`.

### Prepare dataset

Now in EMR Studio, go into the `notebooks` folder and open the notebook `review-analysis.ipynb`. Select `PySpark` as the kernel.

In the first code cell, change the variable `bucket` to reflect the name of the S3 bucket created by the CDK stack.

In the `Compute` section of the sidebar, attach the EMR Serverless application `GenAIDataLakeApp` and select the available runtime role.

Execute the rest of the cells. After this notebook completes, you'll have the dataset written in Parquet format and registered in the Glue catalog.

### Glue data quality

You can run the default Glue data quality checks through the Glue catalog console. 
There is a role precreated for you to use named `GenAIDataLake-GlueDataQualityRole*`

### Configure Athena

Log in to the Athena console. Edit your Athena work group and set the output location to use a path in the S3 bucket created by this deployment.

### Query the data lake

Now switch back to SageMaker Studio and open the notebook `athena-nlp.ipynb`.  In the first code cell, change the variable `region` to reflect your AWS Region, and set the variable `s3stagingathena` to the query result location from your Athena workgroup.

Now you can execute the rest of the cells in the notebook and experiment with different questions.

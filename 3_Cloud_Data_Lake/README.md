# Project purpose and goals
Sparkify, a music streaming company, collects JSON files representing two critical aspects of their business: a) information about songs, and b) event logs representing streams (i.e. plays) of these songs. They hope to create a pipeline that facilitates analytical querying of this data, and have decided to do this through a cloud provider. By using cloud as part of their data infrastructure, Sparkify does not need to provision their own machines, instead opting for their data to be hosted remotely. This also gives Sparkify access to an abundance of tools that they can use for their data transformation pipeline. 


# Data Pipeline
As the JSON files are currently stored in S3, an intuitive pipeline for analysis queries is to transform them into relational tables using Spark (via EMR). Once we've created the schemas and tables, we can export the tables as parquet files back into S3 for storage. This helps to reduce on the cost of maintaining active EMR objects. When we need to perform analytical tasks using our data warehouse tables, we can load the parquet files in S3 back into Spark for querying. 

This pipeline is cost efficient. Since the EMR machines for Spark are run only to perform data transformation, Sparkify does not have to pay too much for the uptime of a Spark cluster. While this does mean that the parquet files must be loaded into EMR in order to access the data warehouse, we can trust that networking optimizations are performed under the hood by AWS, and this extra trouble is offset by the reduction in savings. 

# Steps
1. Add AWS credentials to the config file
2. Check the main method in etl.py and ensure that you are outputting to the expected bucket 
3. In terminal, run etl.py
4. Verify that parquet files have arrived in the output bucket
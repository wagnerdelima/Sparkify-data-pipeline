# Project: Sparkify Data Pipelines

## Introduction
A music streaming company, Sparkify, has decided that it is time to
introduce more automation and monitoring to their data warehouse ETL
pipelines and come to the conclusion that the best tool to achieve
this is Apache Airflow.

They have decided to bring you into the project and expect you to
create high grade data pipelines that are dynamic and built from
reusable tasks, can be monitored, and allow easy backfills.
They have also noted that the data quality plays a big part when
analyses are executed on top the data warehouse and want to run tests
against their datasets after the ETL steps have been executed to
catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's
data warehouse in Amazon Redshift. The source datasets consist of JSON
logs that tell about user activity in the application and JSON metadata
about the songs the users listen to.

## Resolution
The resolution of this problem entails writing customised Python Airflow Operators
for loading data from S3 into staging tables in Redshift -- Star Schema. Furthermore, 
I also desinged customised operators to insert data from Staging tables into tables
within a star schema. 

## Project Structure

```
├── README.md
├── create_tables.sql
├── dags
│   └── udac_example_dag.py
└── plugins
    ├── __init__.py
    ├── helpers
    │   ├── __init__.py
    │   └── sql_queries.py
    └── operators
        ├── __init__.py
        ├── data_quality.py
        ├── load_dimension.py
        ├── load_fact.py
        └── stage_redshift.py

4 directories, 11 files
```

## Instructions
Before running this project you need to set up an AWS Redshift Cluster. Once the cluster is
created run the sql scrips from within the `create_tables.py`.

Run you local airflow installation and access Airflow UI. Please create two connections from
the Admin > Connections menu. One connection for AWS credentials and another for AWS
Redshift cluster.

Once that's done please run your DAG.

## Scripts Usage
- create_tables.sql - Contains the DDL for all tables used in this project.
- udac_example_dag.py - The DAG execution file to run in Airflow.
- stage_redshift.py - Operator to read files from S3 and load into Redshift
staging tables.
- load_fact.py - Operator to load the fact table in Redshift.
- load_dimension.py - Operator to read from staging tables and load
the dimension tables in Redshift.
- data_quality.py - Operator for data quality checking.

# Sparkify S3 to Redshift ETL

## Introduction 
Sparkify, our music streaming startup, has seen significant growth in both its user base and song collection. To support this expansion, we are transitioning our workflows and data to the cloud. Currently, our data is stored in S3, organized into two main directories: one containing JSON logs of user activity on the app, and the other housing JSON metadata about the songs in our library.

This project focuses on developing an ETL pipeline to extract data from S3, load it into staging tables in Redshift, and transform it into dimensional tables. These tables will empower our analytics team to gain deeper insights into user listening habits and song preferences.

## ETL pipeline

### Datasets
The dataset consists of JSON files stored in AWS S3 buckets:
- Log_data: s3://udacity-dend/log_data (service usage events).
- Song_data: s3://udacity-dend/song_data (artist and song details).

### Staging tables
Two staging tables are created in the Redshift database to load data extracted from S3:
- staging_events: Stores data from the log dataset.
- staging_songs: Stores data from the song dataset.

### Star Schema Database
The data from the staging tables is converted into a Star Schema database to support analytics. The schema includes a fact table for songplays and dimension tables for users, artists, songs, and time.

## Usage
1. Begin by setting up a Redshift cluster and creating an IAM Role to allow data staging.
2. Update the `dwh.cfg` file with the necessary database and account credentials.
3. Execute the `create_tables.py` script to initialize the database and tables in Redshift. This script will create two staging tables for
4. loading log and song data from S3, as well as a fact table and four dimension tables for the star schema data warehouse.
5. Run the `etl.py` script to load data from the staging tables into the star schema database.

### Summary
The project aims to develop an ETL pipeline that extracts data from S3, stages it in Redshift, and transforms it into a fact table and dimension tables. This enables the analytics team to gain insights into the songs users are listening to.
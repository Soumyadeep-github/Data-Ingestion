# Data Ingestion Pipeline

The aim of this project is to automate data ingestion from flat files like CSV and compressed files like GZIP into a database like Postgres. The entire setup is automated used Docker and is pretty fast too as multiprocessing is being used.

Current benchmarking results - 

- 80 MB - 500000 ROWS - Ingested within 25 seconds
- 2  MB - 100000 ROWS - Ingested within 1.5 seconds
- 25 MB - 1000000 ROWS - Ingested within 18 seconds

## Benefits 

- Support for parallel insertions.
- Keeps raw data intact and stores it in a separate table for further reconciliation.
- Support for updates to the given data.

## How to use the script:

Clone the entire repo and edit the settings.py file.

   ### Raw data Configuration :

   FILENAME : Provide the filename which you would like to extract.

   DELIMITER : Specify the row-delimiters.

   DATA_LOCATION : Copy the necessary file in the folder ./data-files/

   ### Data Specific Configurations :

   MAIN_TABLE_NAME : Specify what you would like the table name to be.

   MAIN_STAGING_TABLE_NAME : Specify what you would like the staging table name to be. Staging table will be used to temporarily store the data in between processes.

   RAW_DATA_TABLE : Specify what you would like the raw data table name to be.

   MAIN_TABLE_COUNT : Specify a table where you would like to store a count of the a specific column, a category name for example.

   MAIN_COLUMN_NAMES : Specify the column and their data types.

   HEADERS : Specify whether your data contains headers or not.

   KEY_COLUMNS : Specify a list of unique keys for the data set. 1 column name means a single Primary Key while multiple would become a composite key.

   FIELDNAMES : A list of a column names.

   SCD_TYPE : 1 - Overwrite updates into the same column.
              2 - Overwrite updates into another column when updates are present.
    
   UPDATE_COLUMNS : A dictionary of column names from where updates will be conisdered. 

   SCD_COLUMNS : A dictionary of column names that need to be updated accoridng to UPDATE_COLUMNS.

   SCD_UPDATE_MAPPING : This provides a one to one mapping between UPDATE_COLUMNS and SCD_COLUMNS.

   COUNT_COLUMN_NAMES : For a count only table the column name needs to specified.

   GROUP_BY_COLUMNS : Columns on which the group by is going to happen.

   AGGREGATION : The type of aggregation and column name on which the aggregation needs to be performed.

   ALIAS_AGGREGATE_MAPPING : The alias name for the aggregate column name.



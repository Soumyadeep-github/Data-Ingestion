# Welcome to the settings file. Here you shall be configuring everything, be it data source or 
# database URI. 


######################################################################
##################### DATABASE Configurations :#######################
######################################################################
SCHEMA = "ingestion_pipeline"
DB_URL = 'postgresql://postgres:1995@localhost/testdb'
# DB_URL = 'postgresql://root:root@db/pipeline_db'
# FILENAMES = 'products.csv.gz'


######################################################################
##################### Raw data Configurations :#######################
######################################################################

FILENAME = 'u.data'
DELIMITER = ","
# DELIMITER = "\t"

DATA_LOCATION = f'./data-files/{FILENAME}'




######################################################################
################### DATA specific Configurations :####################
######################################################################

#----------------------------------------------------------#
#---------------# Main Table Configuration #---------------#
#----------------------------------------------------------#

#~~~~~~~~~~~~~~~~~~~~~ Data Definition ~~~~~~~~~~~~~~~~~~~~#
# MAIN_COLUMN_NAMES = {'name':"VARCHAR(100)", 
#                     'sku':"VARCHAR(100)", 
#                     'description':"VARCHAR(2000)"}

# MAIN_TABLE_NAME = "products_table"
# MAIN_STAGING_TABLE_NAME = "products_staging_table"
# RAW_DATA_TABLE = "products_raw"

MAIN_TABLE_NAME = "movie_lens_100k"
MAIN_STAGING_TABLE_NAME = "movie_lens_stg_100k"
RAW_DATA_TABLE = "movie_lens_raw_100k"
MAIN_TABLE_COUNT = "movie_lens_count_100k"

MAIN_COLUMN_NAMES = {'userId':"VARCHAR(10)", 
                    'movieId':"VARCHAR(10)", 
                    'rating':"FLOAT",
                    'timestamp_column': "VARCHAR(20)"}

HEADERS = True
# KEY_COLUMNS = ['name', 'sku']
KEY_COLUMNS = ["userId", "movieId"]
# FIELDNAMES = ['name', 'sku', 'description']
FIELDNAMES = ["userId", "movieId", "rating", "timestamp_column"]

SCD_TYPE = 1

#~~~~~~~~~~~~~~~~~~~~~ Data Manipulation ~~~~~~~~~~~~~~~~~~#
# UPDATE_COLUMNS = {'description': "VARCHAR(2000)"}
# SCD_COLUMNS = {'description': "VARCHAR(2000)"}
# # SCD_COLUMNS = {'new_description': "VARCHAR(2000)"}
# SCD_UPDATE_MAPPING = {'description': 'new_description'}

UPDATE_COLUMNS = {'timestamp_column': "VARCHAR(20)",
                  'rating': "FLOAT"}
SCD_COLUMNS = {'timestamp_column': "VARCHAR(20)",
                  'rating': "FLOAT"}
# For SCD_TYPE 2:
# SCD_COLUMNS = {'new_timestamp_column': "VARCHAR(20)",
#                  'new_rating': "FLOAT"}
# but UPDATE_COLUMNS remain the same. Also specify the new SCD_COLUMNS
# in SCD_UPDATE_MAPPING.
# SCD_UPDATE_MAPPING = {'timestamp_column': 'new_timestamp_column',
#                      'rating': 'new_rating'}

SCD_UPDATE_MAPPING = {'timestamp_column': 'timestamp_column',
                      'rating': 'rating'}


# OPTIONAL CONFIGURATIONS:
#----------------------------------------------------------#
#----------------# Count Table (OPTIONAL) #----------------#
#----------------------------------------------------------#

# MAIN_TABLE_COUNT = "products_count_table"

# COUNT_COLUMN_NAMES = {'name': "VARCHAR(100) NOT NULL", 
#                       'number_of_records': "INT NOT NULL"}

# GROUP_BY_COLUMNS = ['name']

# AGGREGATION = {'COUNT': 'sku'}
# ALIAS_AGGREGATE_MAPPING = {'sku': 'number_of_records'}


COUNT_COLUMN_NAMES = {'movieId': "VARCHAR(100) NOT NULL", 
                      'number_of_reviews': "INT NOT NULL"}

GROUP_BY_COLUMNS = ['movieId']

AGGREGATION = {'COUNT': 'userId'}
ALIAS_AGGREGATE_MAPPING = {'userId': 'number_of_reviews'}

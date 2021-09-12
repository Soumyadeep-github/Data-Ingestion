# Welcome to the settings file. Here you shall be configuring everything, be it data source or 
# database URI. 


######################################################################
##################### DATABASE Configurations :#######################
######################################################################
SCHEMA = "ingestion_pipeline"
DB_URL = 'postgresql://postgres:1995@localhost/testdb'
DATA_LOCATION = './data-files/products.csv.gz'

# DB_URL = 'sqlite:///data-files/products.db'



######################################################################
################### DATA specific Configurations :####################
######################################################################

#----------------------------------------------------------#
#---------------# Main Table Configuration #---------------#
#----------------------------------------------------------#

#~~~~~~~~~~~~~~~~~~~~~ Data Definition ~~~~~~~~~~~~~~~~~~~~#
MAIN_COLUMN_NAMES = {'name':"VARCHAR(100)", 
                    'sku':"VARCHAR(100)", 
                    'description':"VARCHAR(2000)"}

MAIN_TABLE_NAME = "products_table"
MAIN_STAGING_TABLE_NAME = "products_staging_table"
RAW_DATA_TABLE = "products_raw"

KEY_COLUMNS = ['name', 'sku']
FIELDNAMES = ['name', 'sku', 'description']

SCD_TYPE = 1

#~~~~~~~~~~~~~~~~~~~~~ Data Manipulation ~~~~~~~~~~~~~~~~~~#
UPDATE_COLUMNS = {'description': "VARCHAR(2000)"}
SCD_COLUMNS = {'description': "VARCHAR(2000)"}
# SCD_COLUMNS = {'new_description': "VARCHAR(2000)"}
# SCD_UPDATE_MAPPING = {'description': 'new_description'}



# OPTIONAL CONFIGURATIONS:
#----------------------------------------------------------#
#----------------# Count Table (OPTIONAL) #----------------#
#----------------------------------------------------------#

MAIN_TABLE_COUNT = "products_count_table"

COUNT_COLUMN_NAMES = {'name': "VARCHAR(100) NOT NULL", 
                      'number_of_records': "INT NOT NULL"}

GROUP_BY_COLUMNS = ['name']

AGGREGATION = {'COUNT': 'sku'}
ALIAS_AGGREGATE_MAPPING = {'sku': 'number_of_records'}

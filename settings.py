# Welcome to the settings file. Here you shall be configuring everything, be it data source or 
# database URI. 


######################################
##### DATABASE Configurations :#######
######################################
# DB_URL = "postgresql://root:root@db:5432/pipeline_db"
SCHEMA = "ingestion_pipeline"
DB_URL = 'postgresql://postgres:1995@localhost/testdb'
DATA_LOCATION = './data-files/products.csv.gz'

# DB_URL = 'sqlite:///data-files/products.db'



##############################################
####### DATA specific Configurations :########
##############################################

MAIN_TABLE_NAME = "products_table"
MAIN_STAGING_TABLE_NAME = "products_count_table"

COLUMNS_DTYPES = {'name':"VARCHAR(100)", 
                    'sku':"VARCHAR(100)", 
                    'description':"VARCHAR(2000)"}

KEY_COLUMNS = ['name', 'sku']

# DB_URL = 'sqlite:///Database/testing_.db'
FIELDNAMES = ['name', 'sku', 'description']

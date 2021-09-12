import sys
sys.path.append('../data_pipeline')

from settings import *
from connection import connect

class DDL:

    TABLES = {}
    def __init__(self):
        self.TABLE_DEFS = {}

    def create_tables(self, 
                      key:bool=False,
                      ):
        """Generate SQL query for creating tables. 
        Three tables shall be created:
            - main table (where unique key constraints apply)
            - staging table (to store temporary data in between processes)
            - raw table (to store all the data without any modification)
        
        Parameters
        ----------
        key : boolean variable

        If true then column names defined in KEY_COLUMNS will be considered
        as primary keys or else there will be no primary keys.
        """

        ddl_query = ""
        columns = ""
        staging_columns = ""
        key_definitions = ""
        staging_query = ""
        raw_query = ""
        scd_2_update = ""

        if KEY_COLUMNS != [] and key:
            key_definitions += f", PRIMARY KEY ({','.join(KEY_COLUMNS)})"
        
        if key:
            if SCD_TYPE and SCD_TYPE == 1:
                columns += ",".join([f"{column} {datatype} NOT NULL" if column in KEY_COLUMNS \
                                     or column in UPDATE_COLUMNS.keys() else f"{column} {datatype}" \
                                     for column, datatype in MAIN_COLUMN_NAMES.items()])
            else:
                columns += ",".join([f"{column} {datatype} NOT NULL" if column in KEY_COLUMNS \
                                     else f"{column} {datatype}" \
                                     for column, datatype in MAIN_COLUMN_NAMES.items()])
        else:
            columns += ",".join([f"{column} {datatype}" for column, datatype in MAIN_COLUMN_NAMES.items()])
        
        if SCD_TYPE and SCD_TYPE == 2 and SCD_COLUMNS != {}:
            scd_2_update += ", ".join([f"{column} {datatype} NOT NULL" \
                                    for column, datatype in SCD_COLUMNS.items() \
                                    if column not in MAIN_COLUMN_NAMES])
            columns += ', ' + scd_2_update   
        
        staging_columns += ",".join([f"{column} {datatype} NOT NULL" if column in KEY_COLUMNS else f"{column} {datatype}" \
                                    for column, datatype in MAIN_COLUMN_NAMES.items()])

        ddl_query += f"CREATE TABLE IF NOT EXISTS {MAIN_TABLE_NAME}({columns}{key_definitions});"
        staging_query += f"CREATE TABLE IF NOT EXISTS {MAIN_STAGING_TABLE_NAME}({staging_columns});"
        raw_query += f"CREATE TABLE IF NOT EXISTS {RAW_DATA_TABLE}({staging_columns});"

        self.TABLE_DEFS[MAIN_TABLE_NAME.upper()] = ddl_query
        self.TABLE_DEFS[MAIN_STAGING_TABLE_NAME.upper()] = staging_query
        self.TABLE_DEFS[RAW_DATA_TABLE.upper()] = raw_query

    def count_table(self):
        """
        Create a SQL table to store the count of specified columns.
        """
        if COUNT_COLUMN_NAMES != []:
            self.TABLE_DEFS[MAIN_TABLE_COUNT.upper()] = f"CREATE TABLE IF NOT EXISTS {MAIN_TABLE_COUNT} ({', '.join([col+' '+dtype \
                                                for col,dtype in COUNT_COLUMN_NAMES.items()])});"

    def run(self):
        self.create_tables(True)
        self.count_table()
        queries_inst = list(self.TABLE_DEFS.values())
        # print("DDL queries: start")
        # for i in queries_inst:
        #     print(i)
        with connect() as conn:
            with conn.cursor() as cur:
                for query in queries_inst:
                    cur.execute(query)
                # cur.execute("SELECT * FROM pg_catalog.pg_tables;")
                # rows = cur.fetchall()[0]
                # print( rows[0])


DDL().run()




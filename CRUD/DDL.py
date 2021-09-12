from settings import *
from connection import connect

class DDL:

    TABLES = {}
    def __init__(self):
        self.TABLE_DEFS = {}

    def create_tables(self, 
                      key:bool=False,
                      ):

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
        if COUNT_COLUMN_NAMES != []:
            self.TABLE_DEFS[MAIN_TABLE_COUNT.upper()] = f"CREATE TABLE IF NOT EXISTS {MAIN_TABLE_COUNT} ({', '.join([col+' '+dtype for col,dtype in COUNT_COLUMN_NAMES.items()])});"

    
    TABLES['PRODUCTS_TABLE'] = """
                        CREATE TABLE IF NOT EXISTS  products_table(
                        name VARCHAR(100),
                        sku VARCHAR(200) NOT NULL,
                        description VARCHAR(20000),
                        PRIMARY KEY (sku, name)
                        );
                    """

    TABLES['PRODUCTS_TABLE_STAGE'] = """
                            CREATE TABLE IF NOT EXISTS  products_staging_table(
                            name VARCHAR(100),
                            sku VARCHAR(200) NOT NULL,
                            description VARCHAR(20000)
                            );
                           """
    
    TABLES['PRODUCTS_TABLE_RAW'] = """
                            CREATE TABLE IF NOT EXISTS  products_raw(
                            name VARCHAR(100),
                            sku VARCHAR(200) NOT NULL,
                            description VARCHAR(20000)
                            );
                           """

    TABLES['AGGREGATE_TABLE'] = """
                        CREATE TABLE IF NOT EXISTS  products_count_table(
                        name VARCHAR(100) NOT NULL,
                        number_of_records INT
                        );
                      """

    TABLES['AGGREGATE_TABLE_STAGE'] = """
                        CREATE TABLE IF NOT EXISTS  count_table_stg(
                        name VARCHAR(100) NOT NULL,
                        number_of_records VARCHAR(200)
                        );
                        """

    def run(self):
        self.create_tables(True)
        self.count_table()
        queries_inst = list(self.TABLE_DEFS.values())
        print("DDL queries: start")
        with connect() as conn:
            with conn.cursor() as cur:
                for query in queries_inst:
                    cur.execute(query)
                cur.execute("SELECT * FROM pg_catalog.pg_tables;")
                rows = cur.fetchall()[0]
                print( rows[0])

    # TABLES['DROP_ALL'] = """DROP TABLE IF EXISTS products_table;
    #                         DROP TABLE IF EXISTS products_count_table;"""

# if __name__ == "__main__":
# def run():
# definition_instance = DDL()
# definition_instance.create_tables(True)
# definition_instance.count_table()
# queries_inst = list(definition_instance.TABLE_DEFS.values())
# print("DDL queries: start")
# with connect() as conn:
#     with conn.cursor() as cur:
#         for query in queries_inst:
#             cur.execute(query)
#         cur.execute("SELECT * FROM pg_catalog.pg_tables;")
#         rows = cur.fetchall()[0]
#         print( rows[0])

DDL().run()

# print("DDL queries: stop")



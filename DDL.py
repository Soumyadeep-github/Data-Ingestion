from settings import *
import psycopg2

class DDL:

    TABLES = {}
    def __init__(self):
        self.TABLE_DEFS = {}

    def create_tables(self, 
                         tbl_name:str, 
                         col_names:dict, 
                         key_columns:list=None, 
                         p_key:bool=None, 
                         comp_key:bool=None):

        ddl_query = ""
        columns = ""
        staging_columns = ""
        key_definitions = ""
        staging_query = ""
        staging_tbl_name = tbl_name+"_staging"

        if key_columns and (p_key or comp_key):
            key_definitions += f", PRIMARY KEY ({','.join(key_columns)})"
        
        if p_key or comp_key:
            columns += ",".join([f"{column} {datatype} NOT NULL" if column in key_columns else f"{column} {datatype}" \
                                    for column, datatype in col_names.items()])
        else:
            columns += ",".join([f"{column} {datatype}" for column, datatype in col_names.items()])

        staging_columns += ",".join([f"{column} {datatype}" for column, datatype in col_names.items()])

        ddl_query += f"CREATE TABLE IF NOT EXISTS {tbl_name}({columns}{key_definitions});"
        staging_query += f"CREATE TABLE IF NOT EXISTS {staging_tbl_name}({columns});"

        self.TABLE_DEFS[tbl_name.upper()] = ddl_query
        self.TABLE_DEFS[staging_tbl_name.upper()] = staging_query


    
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
                        number_of_records VARCHAR(200)
                        );
                      """

    TABLES['AGGREGATE_TABLE_STAGE'] = """
                        CREATE TABLE IF NOT EXISTS  count_table_stg(
                        name VARCHAR(100) NOT NULL,
                        number_of_records VARCHAR(200)
                        );
                        """


if __name__ == "__main__":
    queries_inst = list(DDL.TABLES.values())
    query = "".join(queries_inst)
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(query)



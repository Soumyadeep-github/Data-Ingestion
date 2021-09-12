import multiprocessing as mp
import time
import numpy as np
from DDL import DDL
# from CRUD import DDL
from CRUD.DML import STATEMENTS
# from DML import DML
# from DML_ import upsert_statements, insert_count, insert_raw_data, STATEMENTS
# from DML_ import STATEMENTS
from settings import *
from DataExtraction import DATA
from connection import connect


def get_slices(DATA:str):
    record_count = len(DATA)
    step_size = 500 if record_count < 5000 else record_count // 10
    indices = np.arange(0, record_count + 1, step_size)
    slices = [(indices[i], indices[i + 1]) for i in range(len(indices)) if i < len(indices) - 1]
    return slices

def get_table_count(query):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {query};")
            result = cur.fetchall()[0]
            return result

def process_data(function, slices, mp=mp):
    with mp.Pool(10) as pool:
        pool.starmap(function, slices)

def bulk_upload(chunksa, chunksb):
    values = ""
    for row in DATA[chunksa:chunksb]:
        fields = [f"'{row[i]}'" for i in FIELDNAMES]
        values += f"({', '.join(fields)}),"
        pass
    with connect() as conn:
        with conn.cursor() as cur:
            # Need to be imported from settings.py as 2 variables, with name of staging table and column names.
            cur.execute(f"INSERT INTO {MAIN_STAGING_TABLE_NAME} ({', '.join(FIELDNAMES)}) VALUES {values[:-1]};")
            # cur.execute(DML.COMMANDS['Insert_Products'])
            # cur.execute(DML.COMMANDS['Insert_Products_Raw'])
            # cur.execute(DML.COMMANDS['Flush_Staging_tables'])
            cur.execute(STATEMENTS["UPSERT_STATEMENT"])
            cur.execute(STATEMENTS["RAW_DATA_INSERT"])
            cur.execute(STATEMENTS["DELETE_QUERIES"])
            print("Processes excuted.")

def insert_count():         
    with connect() as conn:
        with conn.cursor() as cur:
            # cur.execute(DML.COMMANDS['Insert_Products_Count'])
            cur.execute(STATEMENTS["COUNT_INSERT_STATEMENT"])
            print("Counts inserted.")
        



if __name__ == "__main__":
    # conn = psycopg2.connect(DB_URL, options='-c search_path=ingestion_pipeline')
    start_time = time.time()
    # DDL().run()
    # with connect() as conn:
    #     with conn.cursor() as cur:
    #         cur.execute(DDL().TABLES['PRODUCTS_TABLE'])
    #         print("Products table created.")
    #         cur.execute(DDL().TABLES['PRODUCTS_TABLE_STAGE'])
    #         print("Products staging table created.")
    #         cur.execute(DDL().TABLES['AGGREGATE_TABLE'])
    #         print("Products count table created.")
    #         cur.execute(DDL.TABLES['PRODUCTS_TABLE_RAW'])
    #         print("Products raw table created.")

    print(STATEMENTS.keys())
    # STATEMENTS = {}
        
    # STATEMENTS["UPSERT_STATEMENT"] = upsert_statements()
    # STATEMENTS["COUNT_INSERT_STATEMENT"] = insert_count()
    # STATEMENTS["RAW_DATA_INSERT"] = insert_raw_data()
    # STATEMENTS["DELETE_QUERIES"] = f"""DELETE FROM {MAIN_STAGING_TABLE_NAME}"""

    print("Number of rows in the file is : ", len(DATA))
    
    print("Number of records currently in table: ",get_table_count(MAIN_TABLE_NAME)[0])
    
    print("Multi-Processing has started.")

    slices = get_slices(DATA)
    process_data(bulk_upload, slices)

    insert_count()
    
    print("Number of rows inserted : ",get_table_count(MAIN_TABLE_NAME)[0])
    print("Number of rows inserted in counts table : ", get_table_count(MAIN_TABLE_COUNT)[0])
    print("Number of rows in raw data : ", get_table_count(RAW_DATA_TABLE)[0])

    print("--- %s seconds ---" % (time.time() - start_time))
    
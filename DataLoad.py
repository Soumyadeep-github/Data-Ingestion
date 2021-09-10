import multiprocessing as mp
import time
import numpy as np
from DDL import DDL
from DML import DML
# from psycopg2 import pool
import psycopg2
from settings import *
from DataExtraction import DATA


def connect():
    return psycopg2.connect(DB_URL, options="-c search_path={}".format(SCHEMA))

def get_slices(DATA):
    record_count = len(DATA)
    step_size = 500 if record_count < 5000 else record_count // 10
    indices = np.arange(0, record_count + 1, step_size)
    r = []
    for i in range(len(indices)):
        if i < len(indices) - 1:
            r.append(DATA[indices[i]:indices[i + 1]])
    return r



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
            cur.execute(DML.COMMANDS['Insert_Products'])
            cur.execute(DML.COMMANDS['Insert_Products_Raw'])
            cur.execute(DML.COMMANDS['Flush_Staging_tables'])
            print("Processes excuted.")

def insert_count():         
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(DML.COMMANDS['Insert_Products_Count'])
            print("Counts inserted.")
        



if __name__ == "__main__":
    # conn = psycopg2.connect(DB_URL, options='-c search_path=ingestion_pipeline')
    start_time = time.time()
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL().TABLES['PRODUCTS_TABLE'])
            print("Products table created.")
            cur.execute(DDL().TABLES['PRODUCTS_TABLE_STAGE'])
            print("Products staging table created.")
            cur.execute(DDL().TABLES['AGGREGATE_TABLE'])
            print("Products count table created.")
            cur.execute(DDL.TABLES['PRODUCTS_TABLE_RAW'])
            print("Products raw table created.")


    print("Number of rows in the file is : ", len(DATA))
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {MAIN_TABLE_NAME};")
            result = cur.fetchall()[0]
            print("Number of records currently in table: ",result[0])
    
    record_count = len(DATA)
    step_size = 500 if record_count < 5000 else record_count // 10
    indices = np.arange(0, record_count + 1, step_size)
    slices = [(indices[i], indices[i + 1]) for i in range(len(indices)) if i < len(indices) - 1]
    print("Multi-Processing has started.")

    with mp.Pool(10) as pool:
        results = pool.starmap(bulk_upload, slices)

    insert_count()
    
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {MAIN_TABLE_NAME};")
            rows_prd = cur.fetchall()[0]
            print("Number of rows inserted : ", rows_prd[0])
            cur.execute(f"SELECT COUNT(*) FROM {MAIN_STAGING_TABLE_NAME};")
            rows = cur.fetchall()[0]
            print("Number of rows inserted in counts table : ", rows[0])

    print("--- %s seconds ---" % (time.time() - start_time))
    
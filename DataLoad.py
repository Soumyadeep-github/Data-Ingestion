import multiprocessing as mp
import time
import numpy as np
from CRUD.DML import STATEMENTS
from settings import *
from DataExtraction import DATA
from connection import connect


def get_slices(DATA:list):
    """
    Generates slices from the data to process it parallely.

    Parameters:
    DATA : List where the whole data is stored.
    """
    record_count = len(DATA)
    step_size = 500 if record_count < 5000 else record_count // 100
    indices = np.arange(0, record_count + 1, step_size)
    slices = [(indices[i], indices[i + 1]) for i in range(len(indices)) if i < len(indices) - 1]
    return slices

def get_table_count(query):
    """
    Fetch count of specified table.

    Parameters:
    query : Name of the table from where we would like the count.
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {query};")
            result = cur.fetchall()[0]
            return result

def process_data(function, slices, mp=mp):
    """
    Initiate a pool of processes for the given function.

    Parameters:
    function : a function to process the data in parallel
    slices : a list of slices to read a specific chunk of the data 
    mp : multiprocessing pool object
    """
    with mp.Pool(10) as pool:
        pool.starmap(function, slices)

def bulk_upload(chunksa, chunksb):
    """
    Creates an insert statement from the data and then executes the queries.
    Parameters :
    chunksa : starting index of the data chunk
    chunksb : ending index of the data chunk
    """
    values = ""
    for row in DATA[chunksa:chunksb]:
        fields = [f"'{row[i]}'" for i in FIELDNAMES]
        values += f"({', '.join(fields)}),"
        pass
    with connect() as conn:
        with conn.cursor() as cur:
            # Need to be imported from settings.py as 2 variables, with name of staging table and column names.
            cur.execute(f"INSERT INTO {MAIN_STAGING_TABLE_NAME} ({', '.join(FIELDNAMES)}) VALUES {values[:-1]};")
            for i in STATEMENTS.keys():
                if "COUNT" not in i:
                    cur.execute(STATEMENTS[i])
            print("Processes excuted.")

def insert_count():   
    """
    Process the insert query for MAIN_TABLE_COUNT.
    """      
    with connect() as conn:
        with conn.cursor() as cur:
            # cur.execute(DML.COMMANDS['Insert_Products_Count'])
            for i in STATEMENTS.keys():
                if "COUNT" in i:
                    cur.execute(STATEMENTS[i])
            print("Counts inserted.")
        



if __name__ == "__main__":
    start_time = time.time()

    print("Qeuries to be executed:")
    for query_name, query in STATEMENTS.items():
        print(query_name)
        print(query)

    print("Number of rows in the file is : ", len(DATA))
    
    print("Number of records currently in table: ",get_table_count(MAIN_TABLE_NAME)[0])
    
    print("Multi-Processing has started.")
    
    slices = get_slices(DATA)
    # print(slices[:10])
    process_data(bulk_upload, slices)

    insert_count()
    
    print("Number of rows inserted : ",get_table_count(MAIN_TABLE_NAME)[0])
    print("Number of rows inserted in counts table : ", get_table_count(MAIN_TABLE_COUNT)[0])
    print("Number of rows in raw data : ", get_table_count(RAW_DATA_TABLE)[0])

    print("--- %s seconds ---" % (time.time() - start_time))
    
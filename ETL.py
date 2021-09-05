import multiprocessing as mp
import gzip, time
import unicodecsv as ucsv
import numpy as np
from DDL import DDL
from DML import DML
import psycopg2
from settings import DB_URL, DATA_LOCATION

global data, conn
# DB_URL = "postgresql://postgres:1995@localhost/Misc"
# DB_URL = "postgresql://root:root@localhost:5432/test_db"
# DB_URL = "postgresql://root:root@db:5432/test_db"
# DB_URL = "postgresql://dbuser:admin2021@localhost:5432/todoapp"
data_path = DATA_LOCATION


data = []
with gzip.open(data_path, 'rb') as r_file:
    # Need to be imported from settings.py as a variable. Probably list?
    rowlist = ucsv.DictReader(r_file, fieldnames=['name', 'sku', 'description'])
    for row in rowlist:
        data.append(row)


data = data[1:]
   


def get_slices(data):
    record_count = len(data)
    step_size = 500 if record_count < 5000 else record_count // 10
    indices = np.arange(0, record_count + 1, step_size)
    r = []
    for i in range(len(indices)):
        if i < len(indices) - 1:
            r.append(data[indices[i]:indices[i + 1]])
    return r



def bulk_upload(chunksa, chunksb):
    conn = psycopg2.connect(DB_URL)

    values = ""
  
    for row in data[chunksa:chunksb+1]:
        # Need to be imported from settings.py as a variable. Probably dictionary?
        values += f"('{row['name']}', '{row['sku']}', '{row['description']}'),"
    with conn:
        with conn.cursor() as cur:
            # Need to be imported from settings.py as 2 variables, with name of staging table and column names.
            cur.execute(f"INSERT INTO products_staging_table (name, sku, description) VALUES {values[:-1]};")
            cur.execute(DML.COMMANDS['Insert_Products'])
            print("1")
            cur.execute(DML.COMMANDS['Insert_Count_Staging'])
            print("2")
            cur.execute(DML.COMMANDS['Flush_Staging_tables'])
            print("3")

        



if __name__ == "__main__":
    conn = psycopg2.connect(DB_URL)
    start_time = time.time()
    with conn:
        with conn.cursor() as cur:
            cur.execute(DDL.TABLES['PRODUCTS_TABLE'])
            print("Products table created.")
            cur.execute(DDL().TABLES['PRODUCTS_TABLE_STAGE'])
            print("Products staging table created.")
            cur.execute(DDL().TABLES['AGGREGATE_TABLE'])
            print("Products count table created.")
            cur.execute(DDL.TABLES['AGGREGATE_TABLE_STAGE'])
            print("Products count staging table created.")

    print("Number of rows in the file is : ", len(data))
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.products_table;")
            result = cur.fetchall()[0]
            print("Number of records currently in table: ",result[0])
 
    print("Multi-Processing has started.")

    record_count = len(data)
    step_size = 500 if record_count < 5000 else record_count // 10
    indices = np.arange(0, record_count + 1, step_size)
    slices = [(indices[i], indices[i + 1]) for i in range(len(indices)) if i < len(indices) - 1]

    with mp.Pool(10) as pool:
        results = pool.starmap(bulk_upload, slices)

    
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.products_table;")
            rows = cur.fetchall()[0]
            print("Number of rows inserted : ", rows[0])

    print("--- %s seconds ---" % (time.time() - start_time))
    
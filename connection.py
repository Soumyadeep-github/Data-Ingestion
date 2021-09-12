import psycopg2
from settings import *

def connect():
    return psycopg2.connect(DB_URL, options="-c search_path={}".format(SCHEMA))
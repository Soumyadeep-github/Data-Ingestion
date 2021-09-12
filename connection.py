import psycopg2
from settings import *

def connect():
    """
    Create a connection object to the Postgres database.
    """
    return psycopg2.connect(DB_URL, options="-c search_path={}".format(SCHEMA))
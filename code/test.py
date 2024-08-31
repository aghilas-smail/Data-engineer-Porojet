import os
import sys
import psycopg2

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.data import DB_FIELDS

dbname = "postgres"
user = "postgres"
password = os.getenv("POSTGRES_PASSWORD")
host = "localhost"

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
cur = conn.cursor()

#Make connection to the database

def try_execut_sql(sql: str):
    try:
        cur.execute(sql)
        conn.commit()
        print(f"Executed {sql} DONE")
    except Exception as e:
        print(f"Couldn't execute {sql} du to this probleme: {e}")
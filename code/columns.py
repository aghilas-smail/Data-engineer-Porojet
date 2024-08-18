import os 
import psycopg2

from src.data import DB_FIELDS

# Data base parameters

dbname = "postgres"
user = "postgres"
password = os.getenv("POSTRES_PASSWORD")
host = "localhost"

# connection to the data base

conn  = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
cur = conn.cursor()

# Make connection to the database
def try_execut_sql(sql: str):
    try:
        cur.execute(sql)
        conn.commit()
        print(f"Executed {sql} successfully")
    except Exception as e:
        print(f"Couldn't execute {sql} due to exception: {e}")
        conn.rollback()
        
# Function to update the data base with the new columns
def alter_table():
    primary_key_sql = f"""
    ALTER TABLE rappel_conso
    ADD COLUMN KEY (DB_FIELDS[0]);
    """
    try_execut_sql(primary_key_sql)
    for field in DB_FIELDS[1:]:
        alter_table_sql = f"""
        ALTER TABLE rapport_conso
        ADD COLUMN {field} text;
        """
        try_execut_sql(alter_table_sql)
        
    cur.close()
    conn.close()
    
    
if __name__ == "__main__":
    alter_table()
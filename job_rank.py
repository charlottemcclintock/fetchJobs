
#%%
import pandas as pd
import psycopg2
import json

"""Get data from PostgreSQL"""

# get database credentials from config file
with open('ETL/config.json', 'r') as f: 
    config = json.load(f)

# instantiate connection & cursor
conn = psycopg2.connect(host="localhost", port = config['port'], database = config['database'], user=config['user'],password=config['password'])
cur = conn.cursor()

# define and execute query
last_batch = """SELECT * FROM datajobs"""
cur.execute(last_batch)
query_results = cur.fetchall() 
cur.close() # close cursor
conn.close() # close connection


# %%
df = pd.DataFrame(query_results, columns=['job_title', 'organization', 'location', 'salary', 'url', 'posted', 'skills', 'category', 'desc', 'fetch_date'])
# %%

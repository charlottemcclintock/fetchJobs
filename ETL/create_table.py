
#%%
import psycopg2
import json

# queries
with open('ETL/queries/create_table.sql', 'r') as f:
    write_table = f.read()

# db config
with open('ETL/config.json', 'r') as f: 
    config = json.load(f)

# instantiate connection & cursor
conn = psycopg2.connect(host="localhost", port = config['port'], database = config['database'], user=config['user'],password=config['password'])

# create cursor object
cur = conn.cursor()

# execute drop & write
cur.execute(write_table)

# commit and close
conn.commit()
cur.close()
conn.close()
# %%

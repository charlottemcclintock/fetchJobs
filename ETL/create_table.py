
#%%
import psycopg2

# queries
with open('queries/create_table.sql', 'r') as f:
    write_table = f.read()
conn = psycopg2.connect(host="localhost", port = 5432, database="personal", user='postgres',password="learning")

# create cursor object
cur = conn.cursor()
# execute drop & write
cur.execute(write_table)
# commit and close
conn.commit()
cur.close()
conn.close()
# %%

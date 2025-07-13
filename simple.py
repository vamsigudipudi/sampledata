import snowflake.connector as sf
import pandas as pd
# Connect to Snowflake
conn = sf.connect(
    user='VAMSIKRISHNAGUDIPUDI45',
    password='Vamsi@1234567890',
    account='RPWPXOG-TU04180',
    warehouse='COMPUTE_WH',
    database='SNOWFLAKE_SAMPLE_DATA',
    schema='TPCH_SF1',
    role='ACCOUNTADMIN'
)

# Run query
cur = conn.cursor()
cur.execute("SELECT * FROM CUSTOMER LIMIT 5")
data = cur.fetchall()
df = pd.DataFrame(data, columns=[col[0] for col in cur.description])
print(df)
try:
    
    cur.execute("""
    SELECT C.C_NAME, O.O_ORDERDATE
    FROM CUSTOMER C
    JOIN ORDERS O ON C.C_CUSTKEY = O.O_CUSTKEY
    LIMIT 10
     """)
    for row in cur.fetchall():
        print(row)

  
finally:
    cur.close()
    conn.close()

import snowflake.connector as sf

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
try:
    cur.execute("SELECT * FROM CUSTOMER LIMIT 10")
    for row in cur:
        print(row)
finally:
    cur.close()
    conn.close()

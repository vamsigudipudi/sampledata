import snowflake.connector as sf
import pandas as pd
# Connect to Snowflake
conn = sf.connect(
    user='VAMSIKRISHNAGUDIPUDI45',
    password='Vamsi@1234567890',
    account='RPWPXOG-TU04180',
    warehouse='COMPUTE_WH',
    database='MY_DB',
    schema='',
    role='ACCOUNTADMIN'
)

# Run query
cur = conn.cursor()
# Define databases and schemas
source_db = 'SNOWFLAKE_SAMPLE_DATA'
source_schema = 'TPCH_SF1'
target_db = 'MY_DB'
target_schema = 'STGE'
tables_to_copy = ['CUSTOMER', 'LINEITEM','NATION','ORDERS']
try:
    # Create target schema if not exists
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {target_db}.{target_schema}")

    for table in tables_to_copy:
        # Step 1: Create target table with same structure
        create_like = f"""
        CREATE OR REPLACE TABLE {target_db}.{target_schema}.{table}
        LIKE {source_db}.{source_schema}.{table};
        """
        cur.execute(create_like)
        print(f"Created table: {target_db}.{target_schema}.{table}")

        # Step 2: Insert data
        insert_data = f"""
        INSERT INTO {target_db}.{target_schema}.{table}
        SELECT * FROM {source_db}.{source_schema}.{table};
        """
        cur.execute(insert_data)
        print(f"Copied data into: {target_db}.{target_schema}.{table}")

except Exception as e:
    print("❌ Error occurred:", e)

finally:
    cur.close()
    conn.close()
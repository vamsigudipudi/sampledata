import snowflake.connector as sf
import pandas as pd

# ✅ Step 1: Connect to Snowflake
conn = sf.connect(
    user='VAMSIKRISHNAGUDIPUDI45',
    password='Vamsi@1234567890',
    account='RPWPXOG-TU04180',
    warehouse='COMPUTE_WH',
    database='MY_DB',
    schema='STGE',  # Set the default schema to STGE
    role='ACCOUNTADMIN'
)

cur = conn.cursor()

# ✅ Step 2: Define source and target schemas
source_schema = 'STGE'
target_schema = 'TRANS'

# ✅ Step 3: Update EXTENDEDPRICE where NATIONKEY = 8
try:
    update_query = f"""
        UPDATE {source_schema}.LINEITEM l
        SET L_EXTENDEDPRICE = L_EXTENDEDPRICE + 100
        WHERE L_ORDERKEY IN (
            SELECT o.O_ORDERKEY
            FROM {source_schema}.ORDERS o
            JOIN {source_schema}.CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY
            WHERE c.C_NATIONKEY = 8
        );
    """
    cur.execute(update_query)
    print("✅ Updated L_EXTENDEDPRICE in STGE.LINEITEM for NATIONKEY = 8.")
except Exception as e:
    print("❌ Update error:", e)

# ✅ Step 4: Create TRANS schema if not exists
try:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
    print("✅ TRANS schema ready.")
except Exception as e:
    print("❌ Failed to create TRANS schema:", e)

# ✅ Step 5: Copy updated rows to TRANS.LINEITEM
try:
    copy_query = f"""
        CREATE OR REPLACE TABLE {target_schema}.LINEITEM AS
        SELECT l.*
        FROM {source_schema}.LINEITEM l
        JOIN {source_schema}.ORDERS o ON l.L_ORDERKEY = o.O_ORDERKEY
        JOIN {source_schema}.CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY
        WHERE c.C_NATIONKEY = 8
    """
    cur.execute(copy_query)
    print("✅ Copied updated rows to TRANS.LINEITEM.")
except Exception as e:
    print("❌ Failed to copy updated rows to TRANS:", e)

# ✅ Step 6: Preview TRANS.LINEITEM
try:
    cur.execute(f"""
        SELECT L_ORDERKEY, L_EXTENDEDPRICE
        FROM {target_schema}.LINEITEM
        LIMIT 10
    """)
    df = pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])
    print("🔍 Preview of updated data in TRANS.LINEITEM:")
    print(df)
except Exception as e:
    print("❌ Preview error:", e)

# ✅ Clean up
cur.close()
conn.close()

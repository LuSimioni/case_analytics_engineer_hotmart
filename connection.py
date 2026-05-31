from dotenv import load_dotenv
import os
import snowflake.connector

load_dotenv()

# DEBUG: Ver o que está sendo lido
print(f"\n[DEBUG] Account: {os.environ.get('SNOWFLAKE_ACCOUNT')}")
print(f"[DEBUG] User: {os.environ.get('SNOWFLAKE_USER')}")
print(f"[DEBUG] Database: {os.environ.get('SNOWFLAKE_DATABASE')}\n")

def get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="EVENTS_HOTMART",
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
    )

def get_connection_params() -> dict:
    return {
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "database":  os.environ["SNOWFLAKE_DATABASE"],
        "schema":    "EVENTS_HOTMART",
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "role":      os.environ["SNOWFLAKE_ROLE"],
    }
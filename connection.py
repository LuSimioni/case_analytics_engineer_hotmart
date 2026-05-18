import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()


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
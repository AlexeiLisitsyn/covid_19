import snowflake.connector
from snowflake.connector import SnowflakeConnection
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())

password = os.environ.get("PASS_SNOWFLAKE")
account = os.environ.get("ACC_SNOWFLAKE")

connection: SnowflakeConnection = snowflake.connector.connect(
    user='ALEXEI23LIFE',
    password=f'{password}',
    account=f'{account}',
    database='COVID19_EPIDEMIOLOGICAL_DATA'
)

cursor = connection.cursor()

try:
    query_1 = "select * from ECDC_GLOBAL_WEEKLY WHERE COUNTRY_REGION ='Lithuania'"
    query_2 = """SELECT 
    avg(cases_weekly) OVER (PARTITION BY country_region ORDER BY date ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS cases_smoothed5,
    cases_weekly,
    country_region,
    date 
    FROM ECDC_GLOBAL_WEEKLY;"""

    cursor.execute(query_2)
    results = cursor.execute(query_2).fetchmany(10)  # display 10 rows
    for rec in results:
        print(rec)

finally:
    cursor.close()
connection.close()
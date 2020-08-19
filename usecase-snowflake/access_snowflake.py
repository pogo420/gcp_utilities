import snowflake.connector as sf_con
from json import load
from google.cloud import storage
from re import search


def test_connection(cursor):
    test = "SELECT current_version()"
    print("Snowflake connection test query:", test)
    cursor.execute(test)
    test_data = cursor.fetchall()
    print("Version:", test_data[0])


def max_date_gcs():
    bucket_name = "bq2snowflake"
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix="bq-sink")
    temp = max([blob.name for blob in blobs])
    date_pattern = r".*?_(\d{8})-.*"
    # return search(date_pattern, temp).group(1)
    return "20200406"
    # return "20200415"


SF_USER = ""
SF_PASSWORD = ""
SF_ACCOUNT = ""
SF_DATABASE = ""
SF_SCHEMA = ""
SF_TABLE = ""
SF_STAGE = ""

with open("config.json") as f:
    json_data = load(f).get("snow-flake")
    SF_USER = json_data.get("SF_USER")
    SF_PASSWORD = json_data.get("SF_PASSWORD")
    SF_ACCOUNT = json_data.get("SF_ACCOUNT")
    SF_DATABASE = json_data.get("SF_DATABASE")
    SF_SCHEMA = json_data.get("SF_SCHEMA")
    SF_TABLE = json_data.get("SF_TABLE")
    SF_TABLE_STG = json_data.get("SF_TABLE_STG")
    SF_STAGE = json_data.get("SF_STAGE")


ctx = sf_con.connect(
    user=SF_USER,
    password=SF_PASSWORD,
    account=SF_ACCOUNT,
    database=SF_DATABASE,
    schema=SF_SCHEMA
    )
cs = ctx.cursor()
test_connection(cs)

role = "USE ROLE ACCOUNTADMIN;"

external_stage = """
//use schema {0}.{1};
copy into {0}.{1}.{2}
  from @{3}
  file_format = (type = csv FIELD_DELIMITER = "|")
  pattern = '.*?/iowa_liquor_sales_{4}.*'
  on_error = 'skip_file'
  ;
  """.format(
      SF_DATABASE,
      SF_SCHEMA,
      SF_TABLE_STG,
      SF_STAGE,
      max_date_gcs()
      )

print("Executing role query:\r\n")
print(role)
print(
    "Copy data from external stage from stage table in snowflake\r\n",
    external_stage)
cs.execute(role)
re = cs.execute(external_stage)
for i in re:
    print(i)

cs.close()
ctx.close()

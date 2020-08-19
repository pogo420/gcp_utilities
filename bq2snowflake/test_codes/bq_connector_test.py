from google.cloud import bigquery
from json import dumps


def get_data():
    query_job = bq_client.query(QUERY)
    for row in query_job:
        row_json = {}
        for i in row.items():
            row_json[i[0]] = i[1]
        print("values('{0}')".format(str(dumps(row_json))))


def get_schema():
    table_ref = bq_client.get_table(TABLE)
    schema_text = []
    for i in table_ref.schema:
        schema_text.append("{0} {1}".format(i.name, i.field_type))
    schema_text = "("+",".join(schema_text)+")"
    print(schema_text)


PROJECT_ID = "sublime-mission-251813"
bq_client = bigquery.Client(project=PROJECT_ID)
QUERY = "SELECT * FROM `sublime-mission-251813.bq2snowflake.bq_input_clean\
` LIMIT 1"
TABLE = "sublime-mission-251813.bq2snowflake.bq_input_clean"
get_data()
get_schema()

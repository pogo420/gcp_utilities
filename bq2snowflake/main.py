from google.cloud import bigquery
from json import load
import snowflake.connector as sf_con


def create_snowflake_table(cs, schema_string, data_generator, sf_table_name):
    '''Function to create table in snowflake'''
    print("Creating table {0} in sf environment".format(sf_table_name))
    cs.execute("CREATE OR REPLACE TABLE {1} ({0})".format(
                                                                schema_string,
                                                                sf_table_name))

    # print(insert_format_data(data_generator, sf_table_name))
    print("adding data to table:", sf_table_name)

    # for row in data_generator:
    #     cs.execute('INSERT INTO {0} values({1})'.format(sf_table_name, ",".join(row[1])))
    cs.execute(insert_format_data(data_generator, sf_table_name))

    print("table: '{0}' created in sf environment..".format(sf_table_name))


def insert_format_data(data_generator, sf_table_name):
    j = 0
    data = 'INSERT INTO {0} values'.format(sf_table_name)
    for row in data_generator:
        data += " ({0}){1}".format(",".join(row[1]),",")
        j += 1
    return data[:-1]+";"


def get_data(bq_client, query):
    print("Executing bq query:", query)
    query_job = bq_client.query(query)
    for row in query_job:
        column = []
        values = []
        # for i in row.items():
        #     column.append(i[0])
        #     values.append("'"+str(i[1])+"'")
        column = [i[0] for i in row.items()]
        values = ["'"+str(i[1])+"'" for i in row.items()]
        yield (column, values)


def get_schema(bq_client, table):
    print("Getting schema of bq table:", table)
    table_ref = bq_client.get_table(table)
    schema_text = []
    for i in table_ref.schema:
        schema_text.append("{0} {1}".format(i.name, i.field_type))
    schema_text = ",".join(schema_text)
    return schema_text


def main():
    PROJECT_ID = ""
    BQ_INPUT_TABLE = ""
    SF_USER = ""
    SF_PASSWORD = ""
    SF_ACCOUNT = ""
    SF_DATABASE = ""
    SF_SCHEMA = ""
    SF_TABLE = ""
    # getting configurations
    with open("config.json") as f:
        json_data = load(f)
        PROJECT_ID = json_data.get("PROJECT_ID")
        BQ_INPUT_TABLE = json_data.get("BQ_INPUT_TABLE")
        SF_USER = json_data.get("SF_USER")
        SF_PASSWORD = json_data.get("SF_PASSWORD")
        SF_ACCOUNT = json_data.get("SF_ACCOUNT")
        SF_DATABASE = json_data.get("SF_DATABASE")
        SF_SCHEMA = json_data.get("SF_SCHEMA")
        SF_TABLE = json_data.get("SF_TABLE")

    print("Creating BQ link..")
    bq_client = bigquery.Client(project=PROJECT_ID)
    BQ_INPUT_QUERY = "SELECT * FROM `{0}` LIMIT 1".format(BQ_INPUT_TABLE)  
    # defining query
    print("Creating SnowFlake link..")
    ctx = sf_con.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        database=SF_DATABASE,
        schema=SF_SCHEMA
        )
    cs = ctx.cursor()

    try:
        schema_string = get_schema(bq_client, BQ_INPUT_TABLE)  # getting schema
        data_generator = get_data(bq_client, BQ_INPUT_QUERY)  # getting data as generator
        print(data_generator)
        # ingestion into snowflake
        # create_snowflake_table(cs, schema_string, data_generator, SF_TABLE)
    finally:
        pass
        cs.close()
    ctx.close()


if __name__ == "__main__":
    main()

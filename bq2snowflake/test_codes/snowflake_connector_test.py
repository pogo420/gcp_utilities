import snowflake.connector


def create_sample_table(cs):
    '''Function to create sample table'''
    cs.execute("CREATE OR REPLACE TABLE test_table(col1 integer, col2 string)")
    cs.execute("INSERT INTO test_table(col1, col2) VALUES (123, 'test string1\
    '),(456, 'test string2')")
    print("sample table: 'test_table' created")


# Gets the version
ctx = snowflake.connector.connect(
    user='armukherjee',
    password='Showflakes@123',
    account='uu93982.us-east-1',
    database='BQ2SNOWFLAKE',
    schema='PUBLIC'
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print("Snowflake version:")
    print(one_row[0])
    create_sample_table(cs)
finally:
    cs.close()
ctx.close()

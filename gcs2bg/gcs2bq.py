import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from json import load
import sys
import csv


def get_source(df_options, source):
    '''Function for getting GCS source'''
    # return df_options.get("gcs").get("source")
    return source


def test_split(data):
    '''Function for testing purpose'''
    data = csv.reader(data.split('\n'))
    for i in data:
        for j in i:
            print(j)


def convert_json(line, schema, csv):
    '''Function to convert data into json format for BQ load'''
    temp_dic = {}
    headers = [i.split(":")[0] for i in schema.split(",")]
    print(headers)
    print(line)
    # for index, data in enumerate(line.split(",")):
    for temp in csv.reader(line.split('\n')):
        for index, data in enumerate(temp):
            temp_dic[headers[index]] = data
    return (temp_dic)


def bq_sink_creation(table_schema, project, data_set, output_table):
    '''Fuction to create BQ sink object'''
    print("BQ sink creation for dataflow...")
    table_schema = table_schema
    # dest_data = output_table.split(".")
    sink = beam.io.WriteToBigQuery(
        output_table,
        project=project,
        dataset=data_set,
        schema=table_schema,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED'
        )

    return sink


def main(source):
    df_options = None
    with open("./config_gcs2bq.json") as f:  # reading config file
        df_options = load(f)

    print("Configuring Sink")
    sink = bq_sink_creation(
        df_options.get("big-query").get("output_schema"),
        df_options.get("big-query").get("project"),
        df_options.get("big-query").get("output_dataset"),
        df_options.get("big-query").get("output_table")
        )

    print("Collecting Pipeline Options...")
    pipeline_options = PipelineOptions(flag=[], **df_options["data-flow"])
    print("Creating Pipeline Object...")
    pipeline = beam.Pipeline(options=pipeline_options)
    pipeline\
        | "Reading from gcs"\
        >> ReadFromText(get_source(df_options, source), skip_header_lines=1)\
        | "Converting to JSON"\
        >> beam.Map(convert_json, schema=df_options.get(
            "big-query").get("output_schema"), csv=csv)\
        | "Writing to BigQuery"\
        >> beam.io.Write(sink)

    print("Executing Pipeline...")
    # pipeline.run().wait_until_finish(duration=600000)  # 10 minutes
    pipeline.run()  #
    print("Check Dataflow UI for details...")


if __name__ == "__main__":
    main(sys.argv[1])
    # schema = "user_id:STRING,geoid:STRING,city:STRING,state:STRING,latitude:FLOAT,longitude:FLOAT,edu_none:FLOAT,edu_no_high_school:FLOAT,edu_some_high_school:FLOAT,edu_high_school:FLOAT,edu_some_college:FLOAT,edu_associate:FLOAT,edu_bachelor:FLOAT,edu_master:FLOAT,edu_phd:FLOAT,age_0_to_5:FLOAT,age_5_to_10:FLOAT,age_10_to_15:FLOAT,age_15_to_18:FLOAT,age_18_to_20:FLOAT,age_20:FLOAT,age_21:FLOAT,age_22_to_25:FLOAT,age_25_to_30:FLOAT,age_30_to_35:FLOAT,age_35_to_40:FLOAT,age_40_to_45:FLOAT,age_45_to_50:FLOAT,age_50_to_55:FLOAT,age_55_to_60:FLOAT,age_60_to_62:FLOAT,age_62_to_65:FLOAT,age_65_to_67:FLOAT,age_67_to_70:FLOAT,age_70_to_75:FLOAT,age_75_to_80:FLOAT,age_80_to_84:FLOAT,age_84_plus:FLOAT,eth_white:FLOAT,eth_black:FLOAT,eth_native_american:FLOAT,eth_asian:FLOAT,eth_hawaiian:FLOAT,eth_other:FLOAT,eth_mixed:FLOAT,gen_male:FLOAT,gen_female:FLOAT,inc_0_to_10:FLOAT,inc_10_to_15:FLOAT,inc_15_to_20:FLOAT,inc_20_to_25:FLOAT,inc_25_to_30:FLOAT,inc_30_to_35:FLOAT,inc_35_to_40:FLOAT,inc_40_to_45:FLOAT,inc_45_to_50:FLOAT,inc_50_to_60:FLOAT,inc_60_to_75:FLOAT,inc_75_to_100:FLOAT,inc_100_to_125:FLOAT,inc_125_to_150:FLOAT,inc_150_to_200:FLOAT,inc_200_plus:FLOAT"
    # convert_json('line', schema)
    # line = """2020-03-20 22:30:01 UTC,E2FE058ABD5DA9821BFD05A3023F35DA,aaid,19100027,AmericInn by Wyndham,3000590768,"AmericInn Stuart, IA",IA,50250,679,Travel>Hotels & Accommodations"""
    # test_split(line)

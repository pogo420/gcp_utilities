import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from json import load
from datetime import datetime

# Execution line: python get-bq-data.py --runner DataflowRunner


def route_none(data):
    temp = data.split("|")
    for inx,i in enumerate(temp):
        if  i == "None":
            temp[inx] = ""
        else:
            # temp[inx] = "check"
            pass
    return data


def csv_format(data, columns):
    temp = [str(data.get(i)) for i in columns]
    return "|".join(temp)


def main(df_options):
    source_query = "SELECT * FROM {0}.{1}.{2} WHERE data_date='2020-04-15'".format(
        df_options.get("bigquery").get("project_id"),
        df_options.get("bigquery").get("data_set"),
        df_options.get("bigquery").get("source_table"),)

    # now = datetime.today().strftime(r"%Y%m%d")
    # now = "20200406"
    now = "20200415"
    bq_sink = df_options.get("bigquery").get("sink_bucket") + "_" + now

    columns = df_options.get("bigquery").get("columns")

    print("Data to be transferred: ", source_query)
    print("Collecting Pipeline Options...")
    pipeline_options = PipelineOptions(flag=[], **df_options["data-flow"])

    print("Creating Pipeline Object...")
    pipeline = beam.Pipeline(options=pipeline_options)

    pipeline \
        | "Reading from BQ" \
        >> beam.io.Read(beam.io.BigQuerySource(
            query=source_query, use_standard_sql=True)) \
        | "Converting to CSV equivalent"\
        >> beam.Map(csv_format,columns = columns)\
        | "Cleanup none"\
        >> beam.Map(route_none)\
        | "Writing to GCS" \
        >> beam.io.Write(beam.io.WriteToText(bq_sink))

    print("Executing Pipeline...")
    # pipeline.run().wait_until_finish(duration=600000)  # 10 minutes
    pipeline.run()
    print("Check Dataflow UI for details...")


if __name__ == "__main__":
    df_options = None
    with open("./config.json") as f:
        df_options = load(f)

    main(df_options)

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromPubSub
from json import load


def parse_pubsub(line):
    import json
    record = json.loads(line)
    return (record)


def bq_sink_creation(project, output_table):
    print("BQ sink creation for dataflow...")
    table_schema = "timeCreated:STRING, bucket:STRING, name:STRING"
    dest_data = output_table.split(".")
    sink = beam.io.WriteToBigQuery(
        dest_data[
            2], project=dest_data[
            0], dataset=dest_data[
            1], schema=table_schema, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)  # write append for stream
    return sink


def main():
    df_options = None
    with open("config.json") as f:
        df_options = load(f)

    sink = bq_sink_creation(df_options.get(
        "big-query").get("project"), df_options.get(
                "big-query").get("output_table"))
    print(sink)
    print("Creating Pipeline Object...")
    pipeline_options = PipelineOptions(flag=[], **df_options["data-flow"])

    # # needed for stream #
    # pipeline_options.view_as(StandardOptions).streaming = True

    pipeline = beam.Pipeline(options=pipeline_options)
    print("executing pipeline:")
    subscription_name = "projects/{0}/subscriptions/{1}".format(
        df_options.get("PROJECT_ID"), df_options.get(
            "PS_SUBSCRIPTION_NAME"))
    print(subscription_name)
    pipeline\
        | "Reading Pub/Sub" \
        >> ReadFromPubSub(
            subscription=subscription_name,
            id_label="id")\
        | "json conv"\
        >> beam.Map(parse_pubsub)\
        | "extract data"\
        >> beam.Map(lambda x: {
            "timeCreated": x.get("timeCreated"),
            "bucket": x.get("bucket"),
            "name": x.get("name"), })\
        | "Writing to BigQuery"\
        >> beam.io.Write(sink)

        # | "Writing to GCS"
        # >> beam.io.Write(beam.io.WriteToText(
        #     "gs://etl_accelerator/results/pubsub_data"
        # ))

    pipeline.run().wait_until_finish(duration=600000)  # 10 minutes


if __name__ == "__main__":
    main()

'''
Created on Oct 1, 2019

@author: Arnab Mukherjee
@email: armukherjee@deloitte.com
@note:  This code is a sample library for implementing all string operation
'''
from __future__ import absolute_import
from json import load
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery


class ToUpper(beam.DoFn):
    '''
    input: A Pcollection with target column name
    output: A Pollection with all input columns but target columns modified to
    upper case.
    '''

    def __init__(self, tar_com):
        super(ToUpper, self).__init__()
        self.tar_com = tar_com

    def process(self, element, *a, **b):
        temp = {}
        for i in element.keys():
            if i == self.tar_com:
                temp[i] = element[i].encode('utf-8').upper()
            else:
                temp[i] = element[i]
        # return (temp, "")
        yield (temp)


class ToLower(beam.DoFn):
    '''
    input: A Pcollection with target column name
    output: A Pollection with all input columns but target columns modified to
    lower case.
    '''

    def __init__(self, tar_com):
        super(ToLower, self).__init__()
        self.tar_com = tar_com

    def process(self, element, *a, **b):
        temp = {}
        for i in element.keys():
            if i == self.tar_com:
                temp[i] = element[i].encode('utf-8').lower()
            else:
                temp[i] = element[i]
        # return (temp, "")
        yield (temp)


class FinLen(beam.DoFn):
    '''
    input: A Pcollection with target column name
    output: A Pollection with all input columns but target columns modified to
    length of data in that column.
    '''

    def __init__(self, tar_com):
        super(FinLen, self).__init__()
        self.tar_com = tar_com

    def process(self, element, *a, **b):
        temp = {}
        for i in element.keys():
            if i == self.tar_com:
                temp[i] = str(len(element[i].encode('utf-8')))
            else:
                temp[i] = element[i]
        # return (temp, "")
        yield (temp)


class FinRev(beam.DoFn):
    '''
    input: A Pcollection with target column name
    output: A Pollection with all input columns but target columns modified to
    reverse of data in that column.
    '''

    def __init__(self, tar_com):
        super(FinRev, self).__init__()
        self.tar_com = tar_com

    def process(self, element, *a, **b):
        temp = {}
        for i in element.keys():
            if i == self.tar_com:
                temp[i] = ""
                for j in element[i].encode('utf-8'):
                    temp[i] = j + temp[i]
            else:
                temp[i] = element[i]
        # return (temp, "")
        yield (temp)


class Replace(beam.DoFn):
    '''
    input: A Pcollection with pattern to replace,
           replace with string and target column name
    output: A Pollection with all input columns but target columns modified,
            pattern in the data of that column replaced with supplied string.
    '''

    def __init__(self, pattern, repl_string, tar_com):
        super(Replace, self).__init__()
        self.tar_com = tar_com
        self.repl_string = repl_string
        self.pattern = pattern

    def process(self, element, *a, **b):
        from re import sub
        temp = {}
        for i in element.keys():
            if i == self.tar_com:
                temp[i] = sub(
                    self.pattern, self.repl_string, element[i].encode(
                        'utf-8'))
            else:
                temp[i] = element[i]
        # return (temp, "")
        yield (temp)


class Substr(beam.DoFn):
    '''
    input: A Pcollection with target column name,
            starting position and length from starting position.
    output: A Pollection with all input columns but target column modified,
            replaced with subtring by strating position and length of
            characters from that position.
    '''

    def __init__(self, tar_com, start_pos, length):
        super(Substr, self).__init__()
        self.tar_com = tar_com
        self.start_pos = start_pos
        self.length = length

    def process(self, element, *a, **b):
        temp = {}
        for i in element.keys():
            if i == self.tar_com:
                temp[i] = element[i].encode('utf-8')[
                    self.start_pos: self.length]
            else:
                temp[i] = element[i]
        # return (temp, "")
        yield (temp)


class RepFunc(beam.DoFn):
    '''
    input: A Pcollection with target column name,
            number of repetation and delemiter.
    output: A Pollection with all input columns but target column modified,
            replaced with data repeted with supplied number and seperated by
            supplied delemiter.
    '''

    def __init__(self, tar_com, n_rept, delim):
        super(RepFunc, self).__init__()
        self.tar_com = tar_com
        self.n_rept = n_rept
        self.delim = delim

    def process(self, element, *a, **b):
        temp = {}
        for i in element.keys():
            if i == self.tar_com:
                temp[i] = ""
                for j in range(self.n_rept):
                    temp[i] += element[i].encode('utf-8')
                    temp[i] += self.delim
                temp[i] = temp[i][:-1]
            else:
                temp[i] = element[i]
        # return (temp, "")
        yield (temp)


class WTrim(beam.DoFn):
    '''
    input: A Pcollection with target column name
    output: A Pollection with all input columns but target column modified,
            All white spaces are removed.
    '''

    def __init__(self, tar_com):
        super(WTrim, self).__init__()
        self.tar_com = tar_com

    def process(self, element, *a, **b):
        temp = {}
        for i in element.keys():
            if i == self.tar_com:
                temp[i] = "".join(element[i].encode('utf-8').split())
            else:
                temp[i] = element[i]
        # return (temp, "")
        yield (temp)


class STrim(beam.DoFn):
    '''
    input: A Pcollection with target column name
    output: A Pollection with all input columns but target column modified,
            white spaces from left and right are removed.
    '''

    def __init__(self, tar_com):
        super(STrim, self).__init__()
        self.tar_com = tar_com

    def process(self, element, *a, **b):
        temp = {}
        for i in element.keys():
            if i == self.tar_com:
                temp[i] = element[i].encode('utf-8').strip()
            else:
                temp[i] = element[i]
        # return (temp, "")
        yield (temp)


class LTrim(beam.DoFn):
    '''
    input: A Pcollection with target column name
    output: A Pollection with all input columns but target column modified,
            white spaces from left are removed.
    '''

    def __init__(self, tar_com):
        super(LTrim, self).__init__()
        self.tar_com = tar_com

    def process(self, element, *a, **b):
        temp = {}
        for i in element.keys():
            if i == self.tar_com:
                temp[i] = element[i].encode('utf-8').lstrip()
            else:
                temp[i] = element[i]
        # return (temp, "")
        yield (temp)


class RTrim(beam.DoFn):
    '''
    input: A Pcollection with target column name
    output: A Pollection with all input columns but target column modified,
            white spaces from right are removed.
    '''

    def __init__(self, tar_com):
        super(RTrim, self).__init__()
        self.tar_com = tar_com

    def process(self, element, *a, **b):
        temp = {}
        for i in element.keys():
            if i == self.tar_com:
                temp[i] = element[i].encode('utf-8').rstrip()
            else:
                temp[i] = element[i]
        # return (temp, "")
        yield (temp)


def bq_sink_creation(project, input_table, output_table):
    print "BQ sink creation for dataflow..."
    bq_client = bigquery.Client(project=project)
    table_ref = bq_client.get_table(input_table)
    table_schema = ",".join(["{0}:{1}".format(
        i.name, i.field_type) for i in table_ref.schema])
    dest_data = output_table.split(".")
    sink = beam.io.WriteToBigQuery(
        dest_data[
            2], project=dest_data[
            0], dataset=dest_data[
            1], schema=table_schema, write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
    return sink


def main():
    df_options = None
    with open("data_flow_config.json") as f:
        df_options = load(f)
    print "reading config file:"
    print type(df_options), df_options
    print "============================="
    print "creating pipeline:"
    pipeline_options = PipelineOptions(flag=[], **df_options["data-flow"])
    pipeline = beam.Pipeline(options=pipeline_options)
    query = "SELECT * FROM `{0}` WHERE place_name LIKE 'Dorrego'".format(
        df_options.get("big-query").get("input_table"))
    print "input query from BQ:", query
    sink = bq_sink_creation(df_options.get(
        "big-query").get("project"), df_options.get(
            "big-query").get("input_table"), df_options.get(
                "big-query").get("output_table"))
    print "executing pipeline:"
    pipeline\
        | "Reading BQ" \
        >> beam.io.Read(beam.io.BigQuerySource(\
                    query=query,
                     use_standard_sql=True))\
        | "To Upper"\
        >> beam.ParDo(ToUpper('operation'))\
        | "To Lower"\
        >> beam.ParDo(ToLower('country_name'))\
        | "FindLen"\
        >> beam.ParDo(FinLen('place_name'))\
        | "FinRev"\
        >> beam.ParDo(FinRev('property_type'))\
        | "Replace"\
        >> beam.ParDo(Replace('oza', '-OOP', 'state_name'))\
        | "Substr"\
        >> beam.ParDo(Substr('title', 0, 5))\
        | "RepFunc"\
        >> beam.ParDo(RepFunc('currency', 6, '-'))\
        | "WTrim"\
        >> beam.ParDo(WTrim('description',))\
        | "STrim"\
        >> beam.ParDo(STrim('col1',))\
        | "LTrim"\
        >> beam.ParDo(LTrim('col2',))\
        | "RTrim"\
        >> beam.ParDo(RTrim('col3',))\
        | "streamline data"\
        >> beam.Map(lambda line: dict(line))\
        | "Writing to BigQuery"\
        >> beam.io.Write(sink)
    '''
        | "Writing to GCS"
        >> beam.io.Write(beam.io.WriteToText(
            "gs://etl_accelerator/results/eta_output_check"
        ))
   '''
    pipeline.run().wait_until_finish()


if __name__ == "__main__":
    main()

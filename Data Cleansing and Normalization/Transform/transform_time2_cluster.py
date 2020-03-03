import os, datetime, logging, traceback
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection
class CleanseDateFn(beam.DoFn):
    def process(self, element):
        record = element
        event_id = record.get('event_id')
        begin_date = record.get('begin_date_time')
        end_date = record.get('end_date_time')
        year = record.get('year')
        month = record.get('month_name')

        begin_value = begin_date.split()
        end_value = end_date.split()
        new_begin = begin_value[0]
        new_end = end_value[0]

        return [(event_id, new_begin, new_end, year, month)]

# DoFn to perform on each element in the input PCollection
class MakeRecordFn(beam.DoFn):
    def process(self, element):
        event_id, new_begin, new_end, year, month = element
        record = {'event_id': event_id, 'new_begin_date': new_begin,
                  'new_end_date': new_end, 'year': year, 'month_name': month}
        return [record]

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/'

# run pipeline on Dataflow
options = {
    'runner': 'DataflowRunner',
    'job_name': 'cleansing-date-table',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-1', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
    'num_workers': 1
}

opts = PipelineOptions(flags=[], **options)

with beam.Pipeline('DataflowRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset2.time'))

    # Write PCollection to file
    query_results | 'Write to log 1' >> WriteToText(DIR_PATH+'timeinput.txt')

    # Apply a ParDo to the PCollection
    cleanse_pcoll = query_results | 'Extract Date' >> beam.ParDo(CleanseDateFn())

    # Write PCollection to file
    cleanse_pcoll | 'Write File' >> WriteToText(DIR_PATH+'dateoutput.txt')

    # Make BQ records
    out_pcoll = cleanse_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())

    qualified_table_name = PROJECT_ID + ':dataset2.time2'
    table_schema = 'event_id:INTEGER, new_begin_date:DATE, new_end_date:DATE, year:INTEGER, month_name:STRING'

    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                                          schema=table_schema,
                                                                          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                          write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
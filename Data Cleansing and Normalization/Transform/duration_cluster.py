import os, datetime, logging, traceback
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.gcp.internal.clients import bigquery
from datetime import date

class TransDurationFn(beam.DoFn):
    def process(self, element):
        record = element
        event_id = record.get('event_id')
        begin_date = record.get('new_begin_date')
        end_date = record.get('new_end_date')

        if begin_date == end_date:
            duration = 0
        else:
            f_date = date(int(end_date[0:4]), int(end_date[5:7]), int(end_date[8:10]))
            l_date = date(int(begin_date[0:4]), int(begin_date[5:7]), int(begin_date[8:10]))
            dates = (f_date - l_date)
            duration = dates.days

        return [(event_id, duration)]

class MakeRecordFn(beam.DoFn):
    def process(self,element):
        event_id, duration = element
        record = {'event_id': event_id, 'duration':duration}
        return [record]

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/'

options = {
    'runner': 'DataflowRunner',
    'job_name': 'transformation-date-table',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-1', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
    'num_workers': 1
}

opts = PipelineOptions(flags=[], **options)

with beam.Pipeline('DataflowRunner', options=opts) as p:
    query_result = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM workflow.time'))

    # Write file
    query_result | 'Write to log 1' >> WriteToText(DIR_PATH+'dateinput.txt')

    # Apply PCollection
    transfrom_pcol = query_result | 'Extract Date' >> beam.ParDo(TransDurationFn())

    # Write file
    transfrom_pcol | 'Write File' >> WriteToText(DIR_PATH+'dateoutput.txt')

    # Make BQ records
    out_pcoll = transfrom_pcol | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())

    qualified_table_name = PROJECT_ID + ':workflow.duration'
    table_schema = 'event_id:INTEGER, duration:INTEGER'

    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                                          schema=table_schema,
                                                                          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                          write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
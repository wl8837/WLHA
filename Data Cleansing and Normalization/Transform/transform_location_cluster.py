import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

from apache_beam.io.gcp.internal.clients import bigquery

# DoFn to perform on each element in the input PCollection.
class RangeFn(beam.DoFn):
    def process(self, element):
        record = element
        event_id = record.get('event_id')
        state = record.get('state')
        begin_lat = record.get('begin_lat')
        begin_lon = record.get('begin_lon')

        state_abbrev={
            'Alabama':'AL',
            'Alaska':'AK',
            'Arizona':'AZ',
            'Arkansas':'AR',
            'California':'CA',
            'Colorado':'CO',
            'Connecticut':'CT',
            'Delaware':'DE',
            'Florida':'FL',
            'Georgia':'GA',
            'Hawaii':'HI',
            'Idaho':'ID',
            'Illinois':'IL',
            'Indiana':'IN',
            'Iowa':'IA',
            'Kansas':'KS',
            'Kentucky':'KY',
            'Louisiana':'LA',
            'Maine':'ME',
            'Maryland':'MD',
            'Massachusetts':'MA',
            'Michigan':'MI',
            'Minnesota':'MN',
            'Mississippi':'MS',
            'Missouri':'MO',
            'Montana':'MT',
            'Nebraska':'NE',
            'Nevada':'NV',
            'New Hampshire':'NH',
            'New Jersey':'NJ',
            'New Mexico':'NM',
            'New York':'NY',
            'North Carolina':'NC',
            'North Dakota':'ND',
            'Ohio':'OH',
            'Oklahoma':'OK',
            'Oregon':'OR',
            'Pennsylvania':'PA',
            'Rhode Island':'RI',
            'South Carolina':'SC',
            'South Dakota':'SD',
            'Tennessee':'TN',
            'Texas':'TX',
            'Utah':'UT',
            'Vermont':'VT',
            'Virginia':'VA',
            'Washington':'WA',
            'West Virginia':'WV',
            'Wisconsin':'WI',
            'Wyoming':'WY'}            
        if state.title() in list(state_abbrev.keys()):
            stateabb=state_abbrev[state.title()]
        else:
            stateabb="NOT US STATE"
        return[(state, stateabb)]

# DoFn to perform on each element in the input PCollection
class MakeRecordFn(beam.DoFn):
    def process(self, element):
        state, stateabb = element
        record = {'event_id':event_id,'state': state, 'state_abbrev': stateabb, 'begin_lat':begin_lat, 'begin_lon':begin_lon}
        return [record]

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/'

options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-location',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-1', # machine types
    'num_workers': 1
    }
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DataflowRunner', options=opts) as p:

    # create a PCollection from the file contents.
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset2.location'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText(DIR_PATH+'input.txt')

    # apply a ParDo to the PCollection
    range_pcoll = query_results | 'Extract Range' >> beam.ParDo(RangeFn())

    # write PCollection to log file
    range_pcoll | 'Write file' >> WriteToText(DIR_PATH+'output.txt')

    # make BQ records
    out_pcoll = range_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())

    qualified_table_name = PROJECT_ID + ':dataset2.location_dataflow'
    table_schema = 'event_id:INTEGER,state:STRING,state_abbrev:STRING, begin_lat:FLOAT, begin_lon:FLOAT'

    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                                                            schema=table_schema,
                                                                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

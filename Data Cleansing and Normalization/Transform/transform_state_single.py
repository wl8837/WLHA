import os, datetime, logging, traceback
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

from apache_beam.io.gcp.internal.clients import bigquery

# DoFn to perform on each element in the input PCollection
class TransStateFn(beam.DoFn):
    def process(self, element):
        record = element
        event_id = record.get('event_id')
        state = record.get('state')
        begin_lat = record.get('begin_lat')
        begin_lon = record.get('begin_lon')

        us_state_abbrev = {
            'Alabama': 'AL',
            'Alaska': 'AK',
            'Arizona': 'AZ',
            'Arkansas': 'AR',
            'California': 'CA',
            'Colorado': 'CO',
            'Connecticut': 'CT',
            'Delaware': 'DE',
            'Florida': 'FL',
            'Georgia': 'GA',
            'Hawaii': 'HI',
            'Idaho': 'ID',
            'Illinois': 'IL',
            'Indiana': 'IN',
            'Iowa': 'IA',
            'Kansas': 'KS',
            'Kentucky': 'KY',
            'Louisiana': 'LA',
            'Maine': 'ME',
            'Maryland': 'MD',
            'Massachusetts': 'MA',
            'Michigan': 'MI',
            'Minnesota': 'MN',
            'Mississippi': 'MS',
            'Missouri': 'MO',
            'Montana': 'MT',
            'Nebraska': 'NE',
            'Nevada': 'NV',
            'New Hampshire': 'NH',
            'New Jersey': 'NJ',
            'New Mexico': 'NM',
            'New York': 'NY',
            'North Carolina': 'NC',
            'North Dakota': 'ND',
            'Ohio': 'OH',
            'Oklahoma': 'OK',
            'Oregon': 'OR',
            'Pennsylvania': 'PA',
            'Rhode Island': 'RI',
            'South Carolina': 'SC',
            'South Dakota': 'SD',
            'Tennessee': 'TN',
            'Texas': 'TX',
            'Utah': 'UT',
            'Vermont': 'VT',
            'Virginia': 'VA',
            'Washington': 'WA',
            'West Virginia': 'WV',
            'Wisconsin': 'WI',
            'Wyoming': 'WY',
        }

        if state.title() in list(us_state_abbrev.keys()):
            stateabb = us_state_abbrev[state.title()]
        else:
            stateabb = 'Not US state'

        return [(state, stateabb)]

class MakeRecordFn(beam.DoFn):
    def process(self,element):
        state, stateabb = element
        record = {'state': state, 'state_abbreviation':stateabb}
        return [record]

PROJECT_ID = os.environ['PROJECT_ID']

options = {
	'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

    query_result = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset2.location'))

    # Write file
    query_result | 'Write to log 1' >> WriteToText('locationinput.txt')

    # Apply PCollection
    transfrom_pcol = query_result | 'Extract State' >> beam.ParDo(TransStateFn())

    # Write file
    transfrom_pcol | 'Write File' >> WriteToText('locationoutput.txt')

    # Make BQ records
    out_pcoll = transfrom_pcol | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())

    qualified_table_name = PROJECT_ID + ':dataset2.state'
    table_schema = 'state:STRING, state_abbreviation:STRING'

    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                                          schema=table_schema,
                                                                          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                          write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
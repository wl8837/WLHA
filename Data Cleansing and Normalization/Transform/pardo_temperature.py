import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

from apache_beam.io.gcp.internal.clients import bigquery


# DoFn to perform on each element in the input PCollection.
class RangeFn(beam.DoFn):
	def process(self, element):
		record = element
		serialid = record.get('serialid')
		min_temp = record.get('min_temperature')
		max_temp = record.get('max_temperature')

		range = int(max_temp - min_temp)

		return range

# DoFn to perform on each element in the input PCollection
class MakeRecordFn(beam.DoFn):
	def process(self, element):
		serialid, range = element
		record = {'serialid': serialid, 'range': range}
		return [record]

PROJECT_ID = os.environ['PROJECT_ID']

options = {
	'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

	# create a PCollection from the file contents.
	query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset_1.temperature LIMIT 10000'))

	# write PCollection to log file
	query_results | 'Write to log 1' >> WriteToText('input.txt')

	# apply a ParDo to the PCollection
	range_pcoll = query_results | 'Extract Range' >> beam.ParDo(RangeFn())

	# write PCollection to log file
	range_pcoll | 'Write to file' >> WriteToText('output.txt')

	# make BQ records
	out_pcoll = range_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())

	qualified_table_name = PROJECT_ID + ':temperature_range'
	table_schema = 'serialid:INTEGER,range:INTEGER'

	out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
												schema=table_schema,
												create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
												write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

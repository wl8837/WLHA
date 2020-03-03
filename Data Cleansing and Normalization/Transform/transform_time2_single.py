import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

from apache_beam.io.gcp.internal.clients import bigquery

# DoFn to perform on each element in the input PCollection.
class change(beam.DoFn):
	def process(self, element):
            record = element
            event_id = record.get('event_id')
            year = record.get('year')
            end_date_time=record.get('end_date_time')
            begin_date_time = record.get('begin_date_time')
            month_name=record.get("month_name")
            
            
            begin_date = begin_date_time.split(' ')
            if len(begin_date) > 1:
                bdate = begin_date[0]
                record['begin_date_time'] = bdate

            end_date = end_date_time.split(' ')
            if len(end_date) > 1:
                edate = end_date[0]
                record['end_date_time'] = edate
                
                return[(bdate,edate)]
            
class MakeRecordFn(beam.DoFn):
	def process(self, element):
		begin__date, end__date = element
		record = {'begin_date_time': begin__date, "end_date_time":end__date}
		return [record]
	    
PROJECT_ID = os.environ['PROJECT_ID']

options = {
	'project': PROJECT_ID
}

opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

        # create a PCollection from the file contents.
        query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset2.time'))

        # write PCollection to log file
        query_results | 'Write to log 1' >> WriteToText('input.txt')

        # apply a ParDo to the PCollection
        range_pcoll = query_results | 'Extract Range' >> beam.ParDo(change())

        # write PCollection to log file
        range_pcoll | 'Write file' >> WriteToText('output.txt')

        # make BQ records
        out_pcoll = range_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())

        qualified_table_name = PROJECT_ID + ':dataset2.time_directrunner'
        table_schema = 'event_id:INTEGER,begin_date_time:DATE,end_date_time:DATE,year:INTEGER,month_name:STRING'

        out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                                                                schema=table_schema,
                                                                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

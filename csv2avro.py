import pandas as pd
from avro import schema, datafile, io
import argparse

__author__ = 'Karolina ALexiou'
__email__ = 'carolinegr@gmail.com'

def load_schema(schema_file):
	return schema.parse(open(schema_file).read())


def read_avro(filename):
   
    rec_reader = io.DatumReader()
    df_reader = datafile.DataFileReader(open(filename), rec_reader) 
    for record in df_reader:
        print record


def parse_data(row):
	try:
		rec = {}
		# parse data here
		#rec["field_name_in_avro_schema"] = row["field_name_in_csv"]
	except Exception as e:
		print "Error parsing ",row
		print e
		return None
	return rec


if __name__=="__main__":

	parser = argparse.ArgumentParser(description="""Convert csv data into an avro file
	 Example: python csv2avro.py -i records.csv -s schema.avsc -o record.avro""")
	parser.add_argument('-o', '--output', type=str, required=True, help='output avro file (will be truncated if it exists)')
	parser.add_argument('-i', '--input', type=str, required=True, help='input csv file (must have header)')
	parser.add_argument('-s', '--schema', type=str, required=True, help='avro schema definition file')
	args = parser.parse_args()

	INFILE = args.input
	OUTFILE = args.output
	SCHEMA_FILE = args.schema

	SCHEMA = load_schema(SCHEMA_FILE)
	outfile = open(OUTFILE, 'wb+')

	# load the file gently into memory
	chunks = pd.read_csv(INFILE, iterator=True, chunksize=50000)
	data =  pd.concat([chunk for chunk in chunks], ignore_index=True)
	write_size=1000
	rec_writer = io.DatumWriter(SCHEMA)
	df_writer = datafile.DataFileWriter(
		    # The file to contain
		    # the records
		    outfile,
		    # The 'record' (datum) writer
		    rec_writer,
		    # Schema, if writing a new file
		    # (aka not 'appending')
		    # (Schema is stored into
		    # the file, so not needed
		    # when you want the writer
		    # to append instead)
		    writers_schema = SCHEMA,
		    # An optional codec name
		    # for compression
		    # ('null' for none)
		    codec = 'deflate')

	written = 0
	it = data.iterrows()
	while(True):
		try:
			index,row = next(it)
			
	
		except:
			print "Finished. Written {} rows".format(written)
			break

		# this changes depending on the input
		rec = parse_data(row)
		if(rec is not None):
		#no way to write avro in batches at the moment
			df_writer.append(rec)
		written+=1
	

	# Close to ensure writing is complete
	df_writer.close()
	# for verifying what was written 
	#read_avro(OUTFILE)




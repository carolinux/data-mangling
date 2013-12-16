#!/usr/bin/evn python2

import sys
import argparse

__author__ = 'Karolina ALexiou'
__email__ = 'karolina.alexiou@teralytics.ch'

if __name__=="__main__":

	# parse arguments 
	parser = argparse.ArgumentParser(description="""Create quick loading script for csv file onto postgres database table.
	 Example: python csv2sql.py -t new_table -i test.csv -o load_test_db.sql --types "integer" "float" "geometry"'""")
	parser.add_argument('-t', '--table', type=str, required=True, help='table name')
	parser.add_argument('-i', '--input', type=str, required=True, help='input csv file (must have header)')
	parser.add_argument('-o', '--output', type=str, required=True, help='output sql script')
	parser.add_argument('-d', '--delim', type=str, default=',', help='csv delimiter')
	parser.add_argument('--types', nargs='+', type=str, required=True, help="SQL types of the column names in the csv")
	parser.add_argument('--drop', action='store_true', help="Drop existing table with the same name")

	args = parser.parse_args()
	table_name = args.table
	delimiter = args.delim
	_input =args.input
	_output = args.output
	column_types = args.types
	drop_table = args.drop

	# parse column names from csv file
	f = open(_input,"r")
	header = f.readline().rstrip() # strip newline
	column_names = header.split(delimiter)
	f.close()

	try:
		assert(len(column_names)== len(column_types))
	except:
		print "Mismatched number of column types({}) and column names({})".format(len(column_types),len(column_names))
		sys.exit(1)

	query=""
	# create the drop table statement
	if(drop_table):
		query+="DROP TABLE {};\n".format(table_name)


	# create the create table statement
	first_col=True
	query+= "CREATE TABLE {} (".format(table_name)
	for col_name,col_type in zip(column_names,column_types):
		if(not first_col):
			query+=", "
		query+="{} {}".format(col_name,col_type)
		if(first_col):
			first_col=False
		
	query+=");\n"
	
	# call copy
	query+="\COPY {} FROM '{}' DELIMITER '{}' CSV HEADER;".format(table_name,_input,delimiter)

	f = open(_output,"w")
	f.write(query)
	f.close()

	print "Query created. Run:\n psql -d [dbname] -f {}".format(_output)

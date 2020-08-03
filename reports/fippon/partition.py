import sqlite3
import pandas as pd
from fippon import generateReport
def main():
	conn=sqlite3.connect('TestDb.db')
	c=conn.cursor()
	c.execute('SELECT last_modified_date_time FROM gps_data WHERE gps_for between "punchIn" and "punchOut"')
	terminals=c.fetchall()
	#BELOW LINE IS TEMPORARY FOR INCOMPLETE DATA
	terminals.pop()
	i=0
	while i<len(terminals):
		start_date=terminals[i][0]
		end_date=terminals[i+1][0]
		#c.execute("CREATE VIEW activity AS SELECT * FROM gps_data WHERE last_modified_date_time BETWEEN "+start_date+" AND "+end_date)
		generateReport.main(start_date,end_date)
		#c.execute('DROP VIEW activity')
		i+=2
	

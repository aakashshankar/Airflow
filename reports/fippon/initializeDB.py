import pandas as pd
from pandas import DataFrame
import sqlite3
from datetime import datetime,timedelta
import os
import time


def convert(inDateTime):
	d=datetime.strptime(inDateTime,"%d-%m-%Y %H:%M:%S")
	return d.strftime("%Y-%m-%d %H:%M:%S")


def main():
	#start=time.time()
	conn=sqlite3.connect('TestDb.db')
	c=conn.cursor()
	
	c.execute('''CREATE TABLE gps_data([gps_id] TEXT, [user_id] TEXT, [latitude] REAL, [longitude] REAL, [date_time] TEXT, [speed] REAL, [bearing] REAL, [accuracy] REAL, [provider] TEXT, [address] TEXT, [driving_distance] REAL, [gps_for] TEXT, [idle_status] TEXT, [last_modified_date_time] TEXT)''')

	#os.system("sed -Ei 's/.$//' /home/airflow/fippon/fippon_futura_gps_data_id_1355.csv")
	csv_data=pd.read_csv(r"/home/airflow/fippon/fippon_futura_gps_data_id_1355.csv")
	csv_data.to_sql('gps_data',conn,if_exists='replace',index=False)	
	c.execute('''SELECT * FROM gps_data''')
	rows=c.fetchall()	
	user_id=rows[0][1]
	for row in rows:
		date_time=row[4]+":00"
		last_modified_date_time=row[13]+":00"
		c.execute('UPDATE gps_data SET date_time=?,last_modified_date_time=? WHERE gps_id=?',(convert(date_time),convert(last_modified_date_time),row[0],))


	c.execute("CREATE TABLE user_"+str(user_id)+"(date TEXT,idle_time INTEGER, non_idle_time INTEGER,locations TEXT)")
	conn.commit()
	#View to group each day of the user's activity
	#c.execute('CREATE VIEW activity AS SELECT * from gps_data GROUP BY date(date_time)')
	conn.close()
#	print(time.time()-start)

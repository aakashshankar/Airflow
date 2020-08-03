import sqlite3
import pandas as pd
from pandas import DataFrame

def main():
	conn=sqlite3.connect('TestDb.db')
	c=conn.cursor()

	c.execute('SELECT user_id FROM gps_data');
	user_id=str(c.fetchall()[0][0])
	df=pd.read_sql_query(f"select * from user_{user_id}",conn)
	#df=DataFrame(c.fetchall(),columns=['date','idle_time','non_idle_time'])
	df.to_excel(r"/home/airflow/reports/report.xslx",index=None,header=True)
	#Destroy Database Once exported
	conn.close()
	c.execute('DELETE FROM gps_data')

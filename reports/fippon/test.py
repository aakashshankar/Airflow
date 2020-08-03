import pandas as pd
import sqlite3
from pandas import DataFrame
import convert_date as cd
import os

conn=sqlite3.connect('TestDB.db')
c=conn.cursor()
#CSV data has to be cleaned and then made into sql form. Below bash command removes the last comma from each line
#os.system("sed -Ei 's/.$//' fippon_futura_gps_data_id_1355.csv")
#clean.clean('fippon_futura_gps_data_id_1355.csv')
c.execute('CREATE TABLE gps_data([gps_id] TEXT, [user_id] TEXT, [latitude] REAL, [longitude] REAL, [date_time] TEXT, [speed] REAL, [bearing] REAL,[accuracy] REAL, [provider] TEXT, [address] TEXT, [driving_distance] REAL, [gps_for] TEXT, [idle_status] TEXT, [last_modified_date_time] TEXT)')

csv_data=pd.read_csv(r'fippon_futura_gps_data_id_1355.csv')		#user_id calculation to be implemented
csv_data.to_sql('gps_data',conn,if_exists='replace',index=False)

c.execute('SELECT * FROM gps_data')

rows=c.fetchall()

for row in rows:
	date_time=row[4]+":00"
	last_modified_date_time=row[13]+":00"
	c.execute("UPDATE gps_data SET date_time=?,last_modified_date_time=? WHERE gps_id=?",(cd.convert(date_time),cd.convert(last_modified_date_time),row[0],))
conn.commit()




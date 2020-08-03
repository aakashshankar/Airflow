import sqlite3
import pandas as pd
from pandas import DataFrame

conn=sqlite3.connect('TestDb.db')
c=conn.cursor()
csv_df=pd.read_csv(r'/home/airflow/reports/GPSData_cleaned.csv')
csv_df.to_sql('gps_data',conn,if_exists='replace',index=False)
csv_df=pd.read_sql_query("SELECT * FROM gps_data ORDER BY user_id",conn)
j=1
users=[]
while j<len(csv_df['user_id']):
        i=j-1
        while j<len(csv_df['user_id']) and csv_df['user_id'][j]==csv_df['user_id'][i]:
                j+=1
        #csv_df[i:j].to_csv(r'/home/airflow/reports/user_separated_csv/fippon_futura_gps_data_id_{0}.csv'.format(csv_df['user_id'][i]),index=False)
#       initializeDB(csv_df['user_id'][i])
        users.append(csv_df['user_id'][i])
        j+=1
c.execute('CREATE TABLE active_report(date TEXT, idle_time INTEGER, non_idle_time INTEGER, locations TEXT, total_distance_travelled INT)')

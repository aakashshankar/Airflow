
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
import sqlite3
import pandas as pd
from pandas import DataFrame
import os
#Establish connection to the database
conn=sqlite3.connect('TestDb.db')
c=conn.cursor()
test_default_args={
	'owner':'airflow',
	'depends_on_past':False,
	'retries':1,
	'retry_delay':timedelta(minutes=1)
}
def convert(inDateTime):
        d=datetime.strptime(inDateTime,"%d-%m-%Y %H:%M:%S")
        return d.strftime("%Y-%m-%d %H:%M:%S")

        #start=time.time()

c.execute('''CREATE TABLE gps_data([gps_id] TEXT, [user_id] TEXT, [latitude] REAL, [longitude] REAL, [date_time] TEXT, [speed] REAL, [bearing] REAL, [accuracy] REAL, [provider] TEXT, [address] TEXT, [driving_distance] REAL, [gps_for] TEXT, [idle_status] TEXT, [last_modified_date_time] TEXT)''')

        #os.system("sed -Ei 's/.$//' /home/airflow/fippon/fippon_futura_gps_data_id_1355.csv")
csv_data=pd.read_csv(r"/home/airflow/airflow/dags/test2.csv")
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
#       print(time.time()-start)


def generateReport(start_date,end_date):
	c.execute('''select user_id,gps_for,idle_status,last_modified_date_time from gps_data where last_modified_date_time between ? and ?''',(start_date,end_date,))
	rows=c.fetchall()
	user_id=rows[0][0]
	date=rows[0][3]
        #Remove lines
	start_date_time=rows.pop(0)
	start_date_time=datetime.strptime(start_date_time[3],"%Y-%m-%d %H:%M:%S")
	end_date_time=rows.pop()
	end_date_time=datetime.strptime(end_date_time[3],"%Y-%m-%d %H:%M:%S")
	total_time=(end_date_time-start_date_time)
	total_time=(total_time.days//(24*60)+total_time.seconds//60)
	idle_time=0
	i=0
	while i<len(rows):
		if rows[i][2]=='Idle' and rows[i][1]!='activity':
			start=i
			while i<len(rows) and rows[i][2]=="Idle":
				i+=1
			end=i-1
			initial=datetime.strptime(rows[start][3],"%Y-%m-%d %H:%M:%S")
			final=datetime.strptime(rows[end][3],"%Y-%m-%d %H:%M:%S")
			diff=final-initial
			idle_time+=(diff.days//(24*60)+diff.seconds//60)
		else:
			i+=1
	non_idle_time=total_time-idle_time
	c.execute('select address from gps_data where last_modified_date_time between ? and ? and gps_for="activity"',(start_date,end_date,))
	rows=c.fetchall()
	addr=""
	for row in rows:
		addr+=(row[0]+";")
	c.execute('INSERT INTO user_'+str(user_id)+' VALUES(date(?),?,?,?)',(date,idle_time,non_idle_time,addr,))
	conn.commit()
	

def partition():
	c.execute('SELECT last_modified_date_time FROM gps_data WHERE gps_for between "punchIn" and "punchOut"')
	terminals=c.fetchall()
	#BELOW LINE IS TEMPORARY FOR INCOMPLETE DATA
	terminals.pop()
	i=0
	while i<len(terminals):
		start_date=terminals[i][0]
		end_date=terminals[i+1][0]
                #c.execute("CREATE VIEW activity AS SELECT * FROM gps_data WHERE last_modified_date_time BETWEEN "+start_date+" AND "+end_date)
		generateReport(start_date,end_date)
                #c.execute('DROP VIEW activity')
		i+=2


def exportAndDestroy():
	c.execute('SELECT user_id FROM gps_data');
	user_id=str(c.fetchall()[0][0])
	df=pd.read_sql_query(f"select * from user_{user_id}",conn)
        #df=DataFrame(c.fetchall(),columns=['date','idle_time','non_idle_time']
	df.to_excel(r"/home/airflow/airflow/dags/report.xslx",index=None,header=True)
	#Destroy Database Once exporte
	c.execute('DELETE FROM gps_data')
	conn.close()


with DAG('trial_dag',start_date=datetime(2020,2,3),schedule_interval='@once',default_args=test_default_args,catchup=False) as dag:
	waiting_data_task=FileSensor(task_id='waiting_data_task',fs_conn_id='fs_default',filepath="/home/airflow/airflow/dags/test2.csv",poke_interval=5)	
	generate_task=PythonOperator(task_id='generate_task',python_callable=partition)
	export_task=PythonOperator(task_id="export_task",python_callable=exportAndDestroy)
	waiting_report_task=FileSensor(task_id="waiting_report_task",fs_conn_id="fs_default",filepath="/home/airflow/airflow/dags/report.xlsx",poke_interval=5)
	"""
	success_task=PythonOperator(task_id="success_task",python_callable=success)
	failure_task=PythonOperator(task_id="failure_task",python_callable=failure)
	branch_task=BranchPythonOperator(task_id="branch_task",python_callable=test)
	crash_task=PythonOperator(task_id="crash_task",python_callable=crash)
	"""
	#dependencies
	waiting_data_task>>initializeDB_task>>generate_task>>export_task>>waiting_report_task
        #waiting_report_task>>branch_task
       # branch_task>>success_task
       # branch_task>>failure_task
       # branch_task>>crash_task

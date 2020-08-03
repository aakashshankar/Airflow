from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import TaskInstance
import pandas as pd
from pandas import DataFrame
import sqlite3
from datetime import datetime, timedelta


# TODO:
# 1. Fix issue which causes trigger of main_dag but main_dag doesn't execute even though there is a dag_run object created.


conn=sqlite3.connect("/home/airflow/reports/MainDB.db")
c=conn.cursor()
default_args={
	'owner':'airflow',
	'depends_on_past':False,
	'email':['manofsteelsuperman949@gmail.com'],
	'email_on_retry':False,
	'email_on_failure':False,
	'retries':1,
	'rety_delay':timedelta(minutes=1)
}

# Create all necessary tables in the MainDB database
def create_tables(**context):
	c.execute("DROP TABLE IF EXISTS gps_data")
	c.execute("DROP TABLE IF EXISTS active_report")
	c.execute("DROP TABLE IF EXISTS user")
	c.execute("CREATE TABLE gps_data([gps_id] TEXT, [user_id] TEXT, [latitude] REAL, [longitude] REAL, [date_time] TEXT, [speed] REAL, [bearing] REAL, [accuracy] REAL, [provider] TEXT,[address] TEXT, [driving_distance] REAL, [gps_for] TEXT, [idle_status] TEXT, [last_modified_date_time] TEXT)")
	c.execute("CREATE TABLE user([gps_id] TEXT, [user_id] TEXT, [latitude] REAL, [longitude] REAL, [date_time] TEXT, [speed] REAL, [bearing] REAL, [accuracy] REAL, [provider] TEXT, [address] TEXT, [driving_distance] REAL, [gps_for] TEXT, [idle_status] TEXT, [last_modified_date_time] TEXT)")
	c.execute("CREATE TABLE active_report(user_id INTEGER, date TEXT, idle_time INTEGER, non_idle_time INTEGER, locations TEXT, total_distance_travelled REAL)")


# This function is to be written in case data needs cleanup and pre processing
def cleanup(**kwargs):
	pass

# Retrieve each user_id and write it to users.txt
def list_users(**kwargs):
	# Ideally, it should be pd.read_csv("/home/airflow/reports/data/gps_data_{today's date}.csv")
	execution_date=kwargs['execution_date']
	gps_data=pd.read_csv("/home/airflow/reports/data/gps_data_2020-01-03.csv")
	users=list(set(gps_data.user_id))
	users=sorted(users)
	fw=open("/home/airflow/reports/data/users.txt",'w')
	data=''
	for user in users:
		data+=(str(user)+'\n')
	data+='END\n'
	fw.write(data)
	fw.close()

# Retrieve and store (CSV) each user's data for a single day in a separate directory
def separate_users(**context):
	# Execution date to be used in below line in place of 2020-01-03
	csv_df=pd.read_csv(r'/home/airflow/reports/data/gps_data_2020-01-03.csv')
	csv_df.to_sql('gps_data',conn,if_exists='replace',index=False)
	csv_df=pd.read_sql_query("SELECT * FROM gps_data ORDER BY user_id",conn)
	j=1
	# users=[]
	#user_id=1742

	while j<len(csv_df['user_id']):
		i=j-1
		while j<len(csv_df['user_id']) and csv_df['user_id'][j]==csv_df['user_id'][i]:
			j+=1
		csv_df[i:j].to_csv(r'/home/airflow/reports/user_separated_csv/fippon_futura_gps_data_id_{0}.csv'.format(csv_df['user_id'][i]),index=False)
		# initializeDB(csv_df['user_id'][i])
        # users.append(csv_df['user_id'][i])
		j+=1

# Function to ignite the main dag
def conditionally_trigger(context,dag_run_obj):
	condition_param=TaskInstance(waiting_data_task,context['execution_date']).current_state()
	#condition_param='success'
	print('Condition is {0}'.format(condition_param))
	if condition_param=='success':
		dag_run_obj.payload={'message':context['params']['message']}
		# pprint.pprint(dag_run_obj)
	return dag_run_obj

# Init DAG
with DAG('init_dag',start_date=datetime(2020,4,28),schedule_interval='@once',default_args=default_args,catchup=False) as dag:
	# Add jinja in the below line's filepath to retrieve the execution date
	waiting_data_task=FileSensor(task_id='waiting_data_task',fs_conn_id='fs_default',filepath='/home/airflow/reports/data/gps_data_2020-01-03.csv',poke_interval=5)
	list_user_task=PythonOperator(task_id='list_user_task',python_callable=list_users,provide_context=True)
	create_tables_task=PythonOperator(task_id='create_tables_task',python_callable=create_tables,provide_context=True)
	separate_users_task=PythonOperator(task_id="separate_users_task",python_callable=separate_users,provide_context=True)
	# cleanup_task=PythonOperator(task_id='cleanup_task',python_callable='cleanup',provide_context=True)
	trigger_dag_run_task=TriggerDagRunOperator(task_id='ignite_main_dag_task',trigger_dag_id='main_dag',python_callable=conditionally_trigger,params={'message':'success'})

	waiting_data_task >> list_user_task >> create_tables_task >> separate_users_task >> trigger_dag_run_task

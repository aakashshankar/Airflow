from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.email_operator import EmailOperator
from airflow.models import TaskInstance
import email,smtplib,ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import sqlite3
import pandas as pd
from pandas import DataFrame
import os
import pprint

conn=sqlite3.connect('/home/airflow/reports/TestDb.db')
c=conn.cursor()
html=""
users=[]
scheduling_default_args={
	'owner':'airflow',
	'depends_on_past':True,
	'email':['manofsteelsuperman949@gmail.com'],
	'email_on_retry':False,
	'email_on_failure':False,
	'retries':1,
	'rety_delay':timedelta(minutes=1)
}


def convert(inDateTime):
	d=datetime.strptime(inDateTime,"%d-%m-%Y %H:%M:%S")
	return d.strftime("%Y-%m-%d %H:%M:%S")

"""
def initializeCallback():
	for user in users:
		initializeDB(user)
"""
csv_df=pd.read_csv(r'/home/airflow/reports/GPSData_cleaned.csv')
csv_df.to_sql('gps_data',conn,if_exists='replace',index=False)
csv_df=pd.read_sql_query("SELECT * FROM gps_data ORDER BY user_id",conn)
j=1
#user_id=1742

while j<len(csv_df['user_id']):
        i=j-1
        while j<len(csv_df['user_id']) and csv_df['user_id'][j]==csv_df['user_id'][i]:
                j+=1
        csv_df[i:j].to_csv(r'/home/airflow/reports/user_separated_csv/fippon_futura_gps_data_id_{0}.csv'.format(csv_df['user_id'][i]),index=False)
#       initializeDB(csv_df['user_id'][i])
        users.append(csv_df['user_id'][i])
        j+=1

j=0

def initializeDB(user_id,**kwargs):
	#c.execute('CREATE TABLE gps_data([gps_id] TEXT, [user_id] TEXT, [latitude] REAL, [longitude] REAL, [date_time] TEXT, [speed] REAL, [bearing] REAL, [accuracy] REAL, [provider] TEXT, [address] TEXT, [driving_distance] REAL, [gps_for] TEXT, [idle_status] TEXT, [last_modified_date_time] TEXT)')
#	os.system("sed 's/.$//' /home/airflow/reports/fippon_futura_gps_data_id_1356.csv"
	csv=pd.read_csv(r'/home/airflow/reports/user_separated_csv/fippon_futura_gps_data_id_{0}.csv'.format(user_id))
	csv.to_sql('user',conn,if_exists='replace',index=False)
	"""
	c.execute('SELECT * FROM gps_data')
	rows=c.fetchall()
	user_id=rows[0][1]
	for row in rows:
		date_time=row[4]+":00"
		last_modified_date_time=row[14]+":00"
		c.execute('UPDATE gps_data SET date_time=?,last_modified_date_time=? WHERE gps_id=?',(convert(date_time),convert(last_modified_date_time),row[0],))
	"""	
	#c.execute('CREATE TABLE report_'+str(user_id)+'(date TEXT, idle_time INTEGER, non_idle_time INTEGER, locations TEXT)')
	html="<strong>PFA report of user_id </strong>"+str(user_id)
	conn.commit()

def generateReport(start_date,end_date):
	#--Fetching Idle and Non Idle Time--
	c.execute('SELECT user_id,gps_for,idle_status,last_modified_date_time from user WHERE last_modified_date_time BETWEEN ? AND ?',(start_date,end_date,))
	rows=c.fetchall()
	user_id=rows[0][0]
	date=rows[0][3]
	start_date_time=rows.pop(0)
	start_date_time=datetime.strptime(start_date_time[3],"%Y-%m-%d %H:%M:%S")
	end_date_time=rows.pop()
	end_date_time=datetime.strptime(end_date_time[3],"%Y-%m-%d %H:%M:%S")
	total_time=end_date_time-start_date_time
	total_time=(total_time.days)//(24*60) + (total_time.seconds)//60
	idle_time=0
	i=0
	while i<len(rows):
		if rows[i][2]=='Idle' and rows[i][1]!='activity':
			start=i
			while i<len(rows) and rows[i][2]=='Idle':
				i+=1
			end=i-1
			initial=datetime.strptime(rows[start][3],"%Y-%m-%d %H:%M:%S")
			final=datetime.strptime(rows[end][3],"%Y-%m-%d %H:%M:%S")
			diff=final-initial
			idle_time+=(diff.days//(24*60)+diff.seconds//60)
		else:
			i+=1
	non_idle_time=total_time-idle_time
	#--End of Fetching Idle and Non Idle Time--
	#--Fetching Locations--
	c.execute('SELECT address FROM user WHERE last_modified_date_time BETWEEN ? AND ? AND gps_for="activity"',(start_date,end_date,))
	rows=c.fetchall()
	addr=""
	for row in rows:
		addr+=(row[0]+";")
	addrList=addr.split(";")
	addrCount=[(x,addrList.count(x)) for x in addrList]
	addrCount=list(set(addrCount))
	#print(addrCount)
	#print(str(addrCount))
	addrCount.remove(('',1))
	#--End of Fetching Locations--
	#--Fetching total distance travelled--
	c.execute('SELECT SUM(driving_distance) FROM user where last_modified_date_time BETWEEN ? AND ?',(start_date,end_date,))
	c.execute('INSERT INTO report_'+str(user_id)+' VALUES(date(?),?,?,?,?)',(date,idle_time,non_idle_time,str(addrCount),c.fetchall()[0][0],))
	conn.commit()

def partition():
	c.execute('SELECT gps_for,last_modified_date_time FROM user WHERE gps_for BETWEEN "punchIn" AND "punchOut"')
	terminals=c.fetchall()
	#PROCESSING THE TERMINALS
	if terminals[0][1]=='punchOut':
		terminals.pop()
	#BELOW LINE IS TEMPORARY 
	#terminals.pop()
	i=0
	while i<len(terminals):
		start_date=terminals[i][1]
		try:
			end_date=terminals[i+1][1]
		except IndexError:
			if terminals[-1][0]=='punchIn':
				c.execute(f"SELECT last_modified_date_time FROM user WHERE last_modified_date_time>=?",(terminals[-1][1],))
				end_date=c.fetchall()[-1][0]
		generateReport(start_date,end_date)
		i+=2

def exportAndDestroy(user_id,**kwargs):
	c.execute("SELECT * FROM report_"+str(user_id))
	df=DataFrame(c.fetchall(),columns=['date','idle_time','non_idle_time','locations','total_distance_travelled'])
	df.to_excel(r"/home/airflow/reports/user_reports/report_{0}.xlsx".format(user_id))
	
	c.execute("DELETE FROM gps_data")
	c.execute("DELETE FROM report_"+str(user_id))
	c.execute("DELETE FROM user")
	os.system("rm /home/airflow/reports/user_separated_csv/fippon_futura_gps_data_id_{0}.csv".format(user_id))
	conn.commit()
	conn.close()

#os.system("rm /home/airflow/reports/TestDb.db")


def branch(**kwargs):
	#crash task to be implemented here using SLA
	ti=TaskInstance(waiting_report_task,kwargs['execution_date'])
	print(ti)
	print(kwargs)
	if ti.current_state()=='success':
		return 'send_email_task'
	else:
		return 'failure_task'

#Below python callable is used when "sned-email_task" is implemented as a python operator as opposed to an email operator
def sendEmail(**kwargs):
	subject="Report"
	body="Please see attached for report of employee"
	sender="www.aakashrules217@gmail.com"
	receiver="racenshoot2@gmail.com"
	pwd="foolhardy"
	message=MIMEMultipart()
	message["From"]=sender
	message["To"]=receiver
	message["Subject"]=subject
	message["Bcc"]=receiver
	
	message.attach(MIMEText(body,"plain"))
	filename="/home/airflow/reports/report.xlsx"
	with open(filename,"rb") as attachment:
		part=MIMEBase("application","octet-stream")
		part.set_payload(attachment.read())
	encoders.encode_base64(part)
	
	part.add_header("Content-Disposition",f"attachment; filename={filename}")
	message.attach(part)
	text=message.as_string()
	context=ssl.create_default_context()
	with smtplib.SMTP_SSL("smtp.gmail.com",465,context=context) as server:
		server.login(sender,pwd)
		server.sendmail(sender,receiver,text)

def conditionally_trigger(context,dag_run_obj):
	condition_param='success'
	print('Condition is {0}'.format(condition_param))
	if condition_param=='success':
		dag_run_obj.payload={'message':context['param']['message']}
		pprint.pprint(dag_run_obj)
	return dag_run_obj		

"""
def get_task_state(**kwargs):
	ti=TaskInstance(send_email_task,kwargs['execution_date'])
	return ti.current_state()

Below python callables are used if success and failure are implemented as python operators instead of dummy operators 
def success():
	return

def failure():
	return 	
"""
user_id=users[j]
j+=1
with DAG('scheduling_dag',start_date=datetime(2020,2,12),schedule_interval='@once',default_args=scheduling_default_args,catchup=False) as dag:
	#for user_id in users:
	waiting_data_task=FileSensor(task_id="waiting_data_task",fs_conn_id='fs_default',filepath='/home/airflow/reports/user_separated_csv/fippon_futura_gps_data_id_{0}.csv'.format(user_id),poke_interval=5)
	initialize_DB_task=PythonOperator(task_id='initialize_DB_task',python_callable=initializeDB,provide_context=True,op_kwargs={"user_id":user_id})
	generate_report_task=PythonOperator(task_id="generate_report_task",python_callable=partition)
	export_task=PythonOperator(task_id="export_and_destroy_task",python_callable=exportAndDestroy,provide_context=True,op_kwargs={"user_id":user_id})
	waiting_report_task=FileSensor(task_id="waiting_report_task",fs_conn_id='fs_default',filepath='/home/airflow/reports/user_reports/report_{0}.xlsx'.format(user_id),poke_interval=5)
	branching_task=BranchPythonOperator(task_id="branching_task",python_callable=branch,provide_context=True)
	#temporary operators
	success_task=DummyOperator(task_id="success_task")
	failure_task=DummyOperator(task_id="failure_task")
	#email mechanism
	send_email_task=EmailOperator(task_id="send_email_task",to="racenshoot2@gmail.com",subject="Test",html_content=html,files=['/home/airflow/reports/user_reports/report_{0}.xlsx'.format(user_id)],cc=None,bcc=None,mime_subtype='mixed',mime_charset='us_ascii')
	#send_email_task=PythonOperator(task_id="send_email_task",python_callable=sendEmail,provide_context=True)
	trigger_dag_task=TriggerDagRunOperator(task_id="trigger_dag_task",trigger_dag_id="scheduling_dag",python_callable=conditionally_trigger,params={'message':str(user_id)})
	#DEPENDENCIES
	waiting_data_task>>initialize_DB_task>>generate_report_task>>export_task>>waiting_report_task>>branching_task
	branching_task>>send_email_task>>trigger_dag_task>>success_task
	branching_task>>failure_task

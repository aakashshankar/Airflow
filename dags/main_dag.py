from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import TaskInstance
from airflow.utils.decorators import apply_defaults
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

# TODO:
#
# 1. All the paths used in the code below are with respect to the machine that it was tried and tested on. Changes need to be made to generalize these paths.
#
# 2. All the emails used are replaced with 'enter_email'.
#
# 3. Contents of the final report must have more fields.
#
# 4. Fix some bugs that cause failure of tasks for particular user.
#
# 5. Optimize the algorithm used to find idle/non_idle time for each user.
#
# 6. Make changes to filenames such that incoming data is of the form 'gps_data_{todays_date}.csv' and stored in the data folder.
#
# 7. Make changes to decide which group of users' reports must be sent to which mailing list, i.e, classify groups of users.
#
# 8. Fix bugs which cause negative idle/non_idle time to be displayed
#
# 9. Fix bugs that cause locations to be empty for certain users and total_distance_travelled is either too large or very small.
#
# 10. Edit the airflow.cfg file with the required email credentials.

conn=sqlite3.connect('/home/airflow/reports/MainDB.db')
c=conn.cursor()
# html=""
# path="/home/airflow/reports/user_reports/"
scheduling_default_args={
	'owner':'airflow',
	'depends_on_past':False,
	# 'email':['enter_email'],
	# 'email_on_retry':False,
	# 'email_on_failure':False,
	'retries':1,
	'rety_delay':timedelta(minutes=1)
}
# Retrieve user_id of each user that is stored separately in a file.
def getUserId(**kwargs):
	with open('/home/airflow/reports/data/users.txt','r') as f:
		users=f.readlines()
	if users[0].strip()=='END':
		return 'END'
	user_id=int(users.pop(0))
	with open('/home/airflow/reports/data/users.txt','w') as f:
		for user in users:
			if user!=str(user_id):
				f.write(user)
	return user_id

# Fill the database with data of a current user
def initializeDB(**kwargs):
	#c.execute('CREATE TABLE gps_data([gps_id] TEXT, [user_id] TEXT, [latitude] REAL, [longitude] REAL, [date_time] TEXT, [speed] REAL, [bearing] REAL, [accuracy] REAL, [provider] TEXT, [address] TEXT, [driving_distance] REAL, [gps_for] TEXT, [idle_status] TEXT, [last_modified_date_time] TEXT)')
#	os.system("sed 's/.$//' /home/airflow/reports/fippon_futura_gps_data_id_1356.csv"
	user_id=kwargs['ti'].xcom_pull(task_ids='get_user_id_task')
	csv=pd.read_csv(r'/home/airflow/reports/user_separated_csv/fippon_futura_gps_data_id_{0}.csv'.format(user_id))
	csv.to_sql('user',conn,if_exists='replace',index=False)
	#c.execute('CREATE TABLE active_report+str(date)+str(branch)(user_id INTEGER, date TEXT, idle_time INTEGER, non_idle_time INTEGER, locations TEXT, total_distance_travelled REAL)')
	# html="<strong>PFA report of user_id </strong>"
	conn.commit()

# Calculate idle/non_idle time, locations, total_distance_travelled.
def generateReport(user_id,start_date,end_date):
	#--Fetching Idle and Non Idle Time--
	c.execute('SELECT user_id,gps_for,idle_status,last_modified_date_time FROM user WHERE last_modified_date_time BETWEEN ? AND ?',(start_date,end_date,))
	rows=c.fetchall()
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
	c.execute('INSERT INTO active_report VALUES(?,date(?),?,?,?,?)',(user_id,date,idle_time,non_idle_time,str(addrCount),c.fetchall()[0][0],))
	conn.commit()


# Retrieve 'punchIn' and 'punchOut' datetime for each user on a particular day.
def partition(**kwargs):
	user_id=kwargs['ti'].xcom_pull(task_ids='get_user_id_task')
	c.execute('SELECT gps_for,last_modified_date_time FROM user WHERE gps_for BETWEEN "punchIn" AND "punchOut"')
	pre_terminals=c.fetchall()

	#PROCESSING THE TERMINALS
	if pre_terminals[0][0]=='punchOut':
		del pre_terminals[0]
	#BELOW LINE IS TEMPORARY
	#terminals.pop()
	#Removing duplicate punchIn or punchOut
	terminals=pre_terminals.copy()
	for i in range(len(pre_terminals)-1):
		if pre_terminals[i][0]==pre_terminals[i+1][0]:
			del terminals[i+1]
	i=0
	while i<len(terminals):
		start_date=terminals[i][1]
		try:
			end_date=terminals[i+1][1]
		except IndexError:
			if terminals[-1][0]=='punchIn':
				c.execute(f"SELECT last_modified_date_time FROM user WHERE last_modified_date_time>=?",(terminals[-1][1],))
				end_date=c.fetchall()[-1][0]
		generateReport(user_id,start_date,end_date)
		i+=2

# Once report is generated, remove all intermediate data.
def exportAndDestroy(**kwargs):
#	global path
	user_id=kwargs['ti'].xcom_pull(task_ids='get_user_id_task')
	c.execute("SELECT * FROM active_report")
	df=DataFrame(c.fetchall(),columns=['user_id','date','idle_time','non_idle_time','locations','total_distance_travelled'])
	df.to_excel(r"/home/airflow/reports/user_reports/Report.xlsx",index=False)
#	path+="report_{0}.xlsx".format(str(user_id))
	#c.execute("DELETE FROM gps_data")
	#c.execute("DELETE FROM active_report")
	c.execute("DELETE FROM user")
	os.system("rm -r /home/airflow/reports/user_separated_csv/*")
	conn.commit()
	conn.close()

#os.system("rm /home/airflow/reports/TestDb.db")

# To export and destroy if all users are completed or to coninue inserting data into report.
def branch(**kwargs):
	#crash task to be implemented here using SLA
	#ti=TaskInstance(get_user_id_task,kwargs['execution_date'])
	#print(ti)
	#print(kwargs)
	if kwargs['ti'].xcom_pull(task_ids="get_user_id_task")=='END':
		return 'export_and_destroy_task'
	else:
		return 'trigger_dag_task'

#Below python callable is used when "sned-email_task" is implemented as a python operator as opposed to an email operator
# def sendEmail(**kwargs):
# 	user_id=kwargs['ti'].xcom_pull(task_ids='get_user_id_task')
# 	subject="Report"
# 	body="Please see attached for report of employee having user_id <strong>{0}</strong>".format(str(user_id))
# 	sender="enter_sender_email"
# 	receiver="enter_recipient_email"
# 	pwd="enter_passwd"
# 	message=MIMEMultipart()
# 	message["From"]=sender
# 	message["To"]=receiver
# 	message["Subject"]=subject
# 	message["Bcc"]=receiver
#
# 	message.attach(MIMEText(body,"plain"))
# 	filename="/home/airflow/reports/user_reports/report_{0}.xlsx".format(str(user_id))
# 	with open(filename,"rb") as attachment:
# 		part=MIMEBase("application","octet-stream")
# 		part.set_payload(attachment.read())
# 	encoders.encode_base64(part)
#
# 	part.add_header("Content-Disposition",f"attachment; filename={filename}")
# 	message.attach(part)
# 	text=message.as_string()
# 	context=ssl.create_default_context()
# 	with smtplib.SMTP_SSL("smtp.gmail.com",465,context=context) as server:
# 		server.login(sender,pwd)
# 		server.sendmail(sender,receiver,text)

# To trigger the dag again but for the next user
def conditionally_trigger(context,dag_run_obj):
	condition_param=TaskInstance(get_user_id_task,context['execution_date']).current_state()
	#condition_param='success'
	print('Condition is {0}'.format(condition_param))
	if condition_param=='success':
		dag_run_obj.payload={'message':context['params']['message']}
		# pprint.pprint(dag_run_obj)
	return dag_run_obj

# main DAG
with DAG('main_dag',start_date=datetime(2020,3,30),schedule_interval=None,default_args=scheduling_default_args,catchup=False) as dag:
	#for user_id in users:
	# waiting_data_task=FileSensor(task_id="waiting_data_task",fs_conn_id='fs_default',filepath='/home/airflow/reports/GPSData_cleaned.csv',poke_interval=5)
	get_user_id_task=PythonOperator(task_id='get_user_id_task',python_callable=getUserId,provide_context=True)
	initialize_DB_task=PythonOperator(task_id='initialize_DB_task',python_callable=initializeDB,provide_context=True)
	insert_into_report_task=PythonOperator(task_id="insert_into_report_task",python_callable=partition,provide_context=True)
	export_and_destroy_task=PythonOperator(task_id="export_and_destroy_task",python_callable=exportAndDestroy,provide_context=True)
	waiting_report_task=FileSensor(task_id="waiting_report_task",fs_conn_id='fs_default',filepath='/home/airflow/reports/user_reports/Report.xlsx',poke_interval=5)
	branching_task=BranchPythonOperator(task_id="branching_task",python_callable=branch,provide_context=True)
	#temporary operators
	success_task=DummyOperator(task_id="success_task")
	#failure_task=DummyOperator(task_id="failure_task")
	#email mechanism
	send_email_task=EmailOperator(task_id="send_email_task",to="enetr_email",subject="Test",html_content="<span style='font-weight:bold; font-family:helvetica'>PFA</span>",files=['/home/airflow/reports/user_reports/Report.xlsx'],cc=None,bcc=None,mime_subtype='mixed',mime_charset='us_ascii')
	#send_email_task=PythonOperator(task_id="send_email_task",python_callable=sendEmail,provide_context=True)
	# send_email_task=BashOperator(task_id="send_email_task",bash_command="echo 'to:manofsteelsuperman949@gmail.com\nsubject: Analysis Report'| (cat -&& uuencode /home/airflow/reports/user_reports/report.xlsx Report.xlsx)| ssmtp manofsteelsuperman949@gmail.com")
	trigger_dag_task=TriggerDagRunOperator(task_id="trigger_dag_task",trigger_dag_id="main_dag",python_callable=conditionally_trigger,params={'message':'{{ task_instance.xcom_pull(task_ids="get_user_id_task") }}'})
	#DEPENDENCIES
	get_user_id_task>>branching_task>>trigger_dag_task>>initialize_DB_task>>insert_into_report_task
	branching_task>>export_and_destroy_task>>waiting_report_task>>send_email_task>>success_task

import sqlite3
from datetime import datetime,timedelta

def main(start_date,end_date):
	conn=sqlite3.connect('TestDb.db')
	c=conn.cursor()

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
	"""
	c.execute('''select latitude,longitude from gps_data where gps_for ='activity' and last_modified_date_time between ? and ?''',(start_date,end_date,))
	rows=c.fetchall()
	locations=[]
	for row in rows:
		addr=""
		coordinates=tuple(map(float,row))
		res=rg.search(coordinates)
		addr+=(res[0]['name']+','+res[0]['admin1']+','+res[0]['admin2']+','+res[0]['cc'])
		locations.append(addr)	
	"""
	c.execute('select address from gps_data where last_modified_date_time between ? and ? and gps_for="activity"',(start_date,end_date,))
	rows=c.fetchall()
	addr=""
	for row in rows:
		addr+=(row[0]+";") 
	c.execute('INSERT INTO user_'+str(user_id)+' VALUES(date(?),?,?,?)',(date,idle_time,non_idle_time,addr,))
	conn.commit()
	conn.close()

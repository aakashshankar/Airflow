import pandas as pd
from pandas import DataFrame

gps_data=pd.read_csv('GPSData_cleaned.csv')

users=list(set(gps_data.user_id))
users=sorted(users)
fw=open('users.txt','w')
#fr=open('users.txt','r')

data=''
for user in users:
	data+=(str(user)+'\n')
data+='END\n'
fw.write(data)
fw.close()

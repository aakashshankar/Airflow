from datetime import datetime, timedelta

def convert(inDateTime):
	d=datetime.strptime(inDateTime,"%d-%m-%Y %H:%M:%S")
	return d.strftime("%Y-%m-%d %H:%M:%S")

def func():
	with open('users.txt','r') as f:
		data=f.readlines()
	user_id=int(data.pop(0))
func()
with open('users.txt','w') as f:
	for datum in data:
		if datum!=str(user_id):
			f.write(datum)


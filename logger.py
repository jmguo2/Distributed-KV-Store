import os.path

def log(pid, method, key, timestamp, reqresp, value):
	file_name = 'log' + str(pid) + '.txt'
	abs_path = (os.getcwd() + '/' + file_name)
	if(os.path.exists(abs_path)):
		f = open(file_name, 'a')
	else:
		f = open(file_name, 'w')
	delimited_log = '555, ' + str(pid) + ', ' + method + ', ' + str(key) + ', ' + str(timestamp) + ', ' + reqresp + ', '
	if(value):
		delimited_log += str(value) 
	f.write(delimited_log + "\n")
	f.close()

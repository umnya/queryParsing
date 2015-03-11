import multiprocessing as mp
import glob
from pymongo import Connection
from pymongo import database
import re
import datetime
import bson
import multiprocessing as mp

#Function devide_file(process_cnt,file_path)
# input 
#
def devide_file(process_cnt):
	file_list=glob.glob('./log/*log*')
	file_cnt=len(file_list)
#	process_cnt=3
	file_idx=1

	devide_cnt=int(round(file_cnt/float(process_cnt)))
	file_name='job_'+str(file_idx) + ".txt"
	f=open(file_name,'w')
	wrt_cnt=0

	for i in file_list:
		f.writelines(i+'\n')
		wrt_cnt+=1
		if wrt_cnt==devide_cnt:
			f.close()
			file_idx+=1
			file_name='job_'+str(file_idx) + ".txt"
			f=open(file_name,'w')
			wrt_cnt=0

def db_conn():
    hostname='localhost'
    port=28017
    user='cubrid'
    password='cubrid123'

    newdb='statdb'

    conn=Connection(hostname,port)
    db=database.Database(conn, newdb)
    return db

def multi_job():
	pass

def query_parse(job_file):
	db=db_conn()
	file_list=open(job_file)

	for file_name in file_list.read().splitlines():
		file=open(file_name)

		print file_name + "....."

		file_cont=file.read().splitlines()
		file_line=len(file_cont)
		i=0
		while i <= file_line-1:
			chk_date=' '.join(file_cont[i].split()[0:2])
			if re.search('execute_all srv_h_id',file_cont[i]):
				ls=file_cont[i].split()
				query=' '.join(ls[6:])
				op1=query.find('/* ')
				op2=query.find(' */')
				sql_id=query[op1:op2+3]
				st_date=' '.join(ls[0:2])
				st_year=int(float('2014'))
				st_month=int(float(st_date[0:2]))
				st_day=int(float(st_date[3:5]))
				st_hour=int(float(st_date[6:8]))
				st_min=int(float(st_date[9:11]))
				st_sec=int(float(st_date[12:14]))
				st_msec=int(float('0'+st_date[14:])*1000000)
#            print sql_id
				dict={
					'file':file_name,
					'start_date':datetime.datetime(year=st_year,month=st_month,day=st_day, hour=st_hour,minute=st_min, second=st_sec, microsecond=st_msec),
					'tranid':ls[2],
					'process':ls[3],
					'sql_id':sql_id,
					'qry':' '.join(ls[6:])
					}
				j=1
				while re.search('bind',file_cont[i+j]):
					ls=file_cont[i+j].split()
					dict['bind '+str(j)]=' '.join(ls[6:])
					j+=1

				if re.search('execute_all',file_cont[i+j]):
					ls=file_cont[i+j].split()
					ed_date=' '.join(ls[0:2])
					ed_year=int(float('2014'))
					ed_month=int(float(ed_date[0:2]))
					ed_day=int(float(ed_date[3:5]))
					ed_hour=int(float(ed_date[6:8]))
					ed_min=int(float(ed_date[9:11]))
					ed_sec=int(float(ed_date[12:14]))
					ed_msec=int(float('0'+ed_date[14:])*1000000)
					dict['end_date']=datetime.datetime(year=ed_year,month=ed_month,day=ed_day, hour=ed_hour,minute=ed_min, second=ed_sec, microsecond=ed_msec)
					dict['tuple']=ls[6]
					dict['time']=ls[8]
					dict['error']=ls[4]
					db.query.insert(dict)

				i=i+j
			i+=1


multi_proc_cnt=20

devide_file(multi_proc_cnt)
pool=mp.Pool(processes=multi_proc_cnt)
job_files=glob.glob('*job*txt')

start_date=datetime.datetime.now()

reslut=pool.map(query_parse,job_files)

end_date=datetime.datetime.now()
print "=" *30
print end_date-start_date
print "=" *30
	


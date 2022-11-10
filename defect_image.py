from json import dumps,loads
import numpy as np
import pandas as pd
import cx_Oracle,time,os
from datetime import datetime,timedelta
from ftplib import FTP
from multiprocessing import Pool
F68XEUS='APOLLO/vs.rocky2@F68_XEUS_LOCAL'
#F68UDB='f68yasview/f68yasview,1234@udbprod'
F68UDB='UDB/udb@F68_UDB.WORLD'

filePath=os.path.dirname(__file__)
if not os.path.exists(filePath + '/photo'):
	os.mkdir(filePath + '/photo')
imagePath=filePath+'/photo/'
klarityhosts=["f68pxap400n1.f68prod.mfg.intel.com","f68pxap400n2.f68prod.mfg.intel.com","f68pxap401n1.f68prod.mfg.intel.com","f68pxap401n2.f68prod.mfg.intel.com"]



def makeWhereIn(valueList,cate):
	result=''
	if len(valueList)==0:
		return 'ERROR_EmptyList'
	else:
		if cate=='str':
			for item in valueList:
				result=result+"'"+str(item)+"',"
		elif cate=='int':
			for item in valueList:
				result=result+str(item)+","
		else:
			return 'ERROR_WrongCate'
		result=result[0:len(result)-1]
		return result
def unTuple2Array(data):
	result=[]
	for row in data:
		temp=[]
		for cell in row:
			temp.append(cell)
		result.append(temp)
	return result
def DBColumn2List(DBResult,index):
	result=[]
	for row in DBResult:
		result.append(row[index])
	return result

def OracleQueryDBHandler(conn,sql):
	cursor=conn.cursor()
	try:
		cursor.execute(sql)
		result=pd.read_sql(sql,conn)
		#result=cursor.fetchall()
	except:
		return {'result':'Failed'}
	finally:
		conn.close()
	return result

def XEUSDBHandler(sql):
	conn=cx_Oracle.connect(F68XEUS)
	return OracleQueryDBHandler(conn,sql)

def UDBDBHandler(sql):
	conn=cx_Oracle.connect(F68UDB)
	return OracleQueryDBHandler(conn,sql)


def ftpconnect(host, username, password):
	ftp = FTP()
	ftp.connect(host, 21)
	ftp.login(username, password)
	return ftp

def downloadfile(ftp, remotepath, localpath):
	bufsize = 1024
	succeed=1
	try:
		cb_path=os.path.split(localpath)[0]
		if not os.path.exists(cb_path):
			os.makedirs(cb_path+'/')
		print(localpath)
		fp = open(localpath, 'wb')
		ftp.retrbinary('RETR ' + remotepath, fp.write, bufsize)
		ftp.set_debuglevel(0)
	except Exception as e:
		print(e)
		succeed=0
		fp.close()
		remove(localpath)
	finally:
		return succeed


def KlarfImagesDownLoad(hosts,files):
	errorMessage=''
	FTPFileCount=0
	succeed=1
	nodeSucceedNum=len(hosts)
	for host in hosts:
		try:
			ftp = ftpconnect(host, "f68klaftp_11", "f68KLAftp11!")
			for key in files:
				if downloadfile(ftp,key,files[key])==0:
					errorMessage=errorMessage+key+','
					succeed=0
				else:
					FTPFileCount=FTPFileCount+1
			ftp.quit()
			break
		except:
			nodeSucceedNum=nodeSucceedNum-1

	if len(errorMessage)>0:
		errorMessage=errorMessage[0:len(errorMessage)-1]
	if nodeSucceedNum==0:
		succeed=0
		errorMessage='ANF'
	return {'succeed':succeed,'errorMessage':errorMessage,'FTPFileCount':FTPFileCount}

def createCBDir():
	CB_sql='''
	select * from insp_decode where parm_key=2
	'''
	CB_df = UDBDBHandler(CB_sql)
	CB_df['folder']=CB_df.apply(lambda x: str(x['CODE'])+'#'+x['NAME'].replace('/','&'),axis=1)
	CB_dict=dict(zip(CB_df.CODE,CB_df.folder))

	return CB_dict

def createlayerDir(filepath):
	layer_sql='''
	select distinct(layer_id) from insp_wafer_summary where layer_id not like '%PRB%'
	'''
	layer_df=UDBDBHandler(layer_sql)
	for i in layer_df['LAYER_ID'].values.tolist():
		if not os.path.exists(filepath + str(i)):
			os.mkdir(filepath+str(i))


def main(sysdate_minus_start,sysdate_minus_end):
	select_summaryClassBin = '''
	select class_number,defect_id,A.* from insp_defect d,(
	select recipe_key,to_char(last_update,'mm/dd/yyyy hh24:mi:ss') as last_update,wafer_key,defects,device,lot_id,wafer_id,layer_id,inspection_time from insp_wafer_summary)A 
	where d.inspection_time=A.inspection_time and d.wafer_key=A.wafer_key and d.inspection_time<=sysdate-{} and d.inspection_time>=sysdate-{} and class_number not in (0,99,999,9999) and layer_id not like '%PRB%'
	order by A.inspection_time
	'''
	selectDefectImageLocation = '''
	select defect_id,image_id,image_filespec,inspection_time from insp_wafer_image where wafer_key=%s and inspection_time=to_date('%s','yyyy-mm-dd,hh24:mi:ss') and defect_id=%s and image_id!=1
	order by 1,2
	'''
	CB_dict=createCBDir()
	imageinfo=[]

	select_summaryClassBin=select_summaryClassBin.format(str(sysdate_minus_start), str(sysdate_minus_end))
	print(select_summaryClassBin)
	result=UDBDBHandler(select_summaryClassBin)
	print(result)
	#pool=Pool(os.cpu_count())
	for index,i in result.iterrows():
		imageDict = {}
		sql = selectDefectImageLocation % (i['WAFER_KEY'],i['INSPECTION_TIME'],i['DEFECT_ID'])
		ImageResult = UDBDBHandler(sql)
		ImageResult['inspection_time_ss'] = ImageResult['INSPECTION_TIME'].apply(lambda x:str(time.mktime((datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S')).timetuple())))
		ImageResult=ImageResult.sample(n=1)
		for key,row in ImageResult.iterrows():
			try:
				imageName = imagePath + str(i['LAYER_ID']) + '/' + str(CB_dict[i['CLASS_NUMBER']]) + '/' + '%s_%s_%s_%s_%s' % (str(i['CLASS_NUMBER']), row['inspection_time_ss'], str(i['WAFER_KEY']), row['DEFECT_ID'], row['IMAGE_ID']) + '.jpg'
			except:
				continue
			if not os.path.exists(imageName):
				imageDict[row['IMAGE_FILESPEC']] = imageName
				imageinfo.append([str(i['LOT_ID']),str(i['WAFER_ID']),str(i['LAYER_ID']),str(i['DEVICE']),row['inspection_time_ss'], str(i['WAFER_KEY']), row['DEFECT_ID'], row['IMAGE_ID'],imageName])
		image_result = KlarfImagesDownLoad(klarityhosts, imageDict)
		print(image_result)
		if len(imageinfo)>20:
			print("writing to dataframe")
			df = pd.DataFrame(imageinfo)
			df.to_csv('imageinfo'+'.csv',mode='a', header=False)
			imageinfo=[]
main(0,0.1)

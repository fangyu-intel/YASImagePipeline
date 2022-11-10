import pandas as pd
import os
from datetime import datetime
import cx_Oracle
import time
df=pd.read_csv('imageinfo.csv',header=None)
print(df)
print(len(df))
list1=[]
import os

path = 'D:\Defect_image\photo'
F68XEUS='APOLLO/vs.rocky2@F68_XEUS_LOCAL'
#F68UDB='f68yasview/f68yasview,1234@udbprod'
F68UDB='UDB/udb@F68_UDB.WORLD'


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

def UDBDBHandler(sql):
	conn=cx_Oracle.connect(F68UDB)
	return OracleQueryDBHandler(conn,sql)

def get_filelist(dir):
    Filelist = []
    for home, dirs, files in os.walk(path):
        for filename in files:
            # 文件名列表，包含完整路径
            Filelist.append(os.path.join(home, filename))
            # # 文件名列表，只包含文件名
            # Filelist.append( filename)
    return Filelist
def file_path_info(file_df):
    info = os.path.split(file_df['path'])[1].split('_')
    file_df['path']=file_df['path'].replace('\\','/')
    file_df['WAFER_KEY'] = info[2]
    timestamp = int(float(info[1]))
    time_local = time.localtime(timestamp)

    file_df['INSPECTION_TIME'] = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
    file_df['CLASS_NUMBER'] = info[0]
    file_df['DEFECT_ID']=info[3]
    file_df['inspection_time_ss']=info[1]
    file_df['IMAGE_ID']=info[-1].split('.')[0]

    return file_df

if __name__ == "__main__":
    Filelist = get_filelist(dir)
    file_not_in_csv=[]
    list1=df[9].values.tolist()
    for file in Filelist:
        if file.replace('\\','/') not in list1:
            file_not_in_csv.append(file)
    if len(file_not_in_csv)!=0:
        print(" Remain %d files not in imageinfo.csv"%len(file_not_in_csv))
        file_df=pd.DataFrame()
        file_df['path']=file_not_in_csv
        print(file_df)
        file_df=file_df.apply(lambda x: file_path_info(x),axis=1)

        sql='''
        select * from (select class_number,defect_id,A.* from insp_defect d,(
    select recipe_key,to_char(last_update,'mm/dd/yyyy hh24:mi:ss') as last_update,wafer_key,defects,device,lot_id,wafer_id,layer_id,inspection_time from insp_wafer_summary)A 
    where d.inspection_time=A.inspection_time and d.wafer_key=A.wafer_key and class_number not in (0,99,999,9999) and layer_id not like '%PRB%'
    )C where 
        '''
        sql_list=[]
        for index,f in file_df.iterrows():
            sql_list.append(''' (C.wafer_key=%s and C.inspection_time=to_date('%s','yyyy/mm/dd hh24:mi:ss') and class_number=%s and defect_id=%s) '''%(f['WAFER_KEY'],f['INSPECTION_TIME'],f['CLASS_NUMBER'],f['DEFECT_ID']))

        sql+=" OR ".join(sql_list)
        df=UDBDBHandler(sql)
        df=df.sort_values(['WAFER_KEY','INSPECTION_TIME']).astype(str)
        file_df=file_df.sort_values(['WAFER_KEY','INSPECTION_TIME']).astype(str)
        imageinfo=pd.merge(file_df,df,on=['WAFER_KEY','INSPECTION_TIME','DEFECT_ID','CLASS_NUMBER'])
        imageinfo=imageinfo[['LOT_ID', 'WAFER_ID','LAYER_ID','DEVICE','inspection_time_ss','WAFER_KEY','DEFECT_ID','IMAGE_ID','path']]
        imageinfo.to_csv('imageinfo'+'.csv',mode='a', header=False)
    else:
        print("Imageinfo.csv includes all files' information ")



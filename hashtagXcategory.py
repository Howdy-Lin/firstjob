# 以下為自動化coding
# 讀取raw data

import pymysql
import pandas as pd
import time
import os
pd.set_option('display.max_columns', None)
start=time.time()
#------------------------------------先讀取localDB裡的table hashtagXcategory----------------------------------

try:
    db = pymysql.connect(host='192.168.5.50', port=3305, user='root', passwd='root', db='hi8')
    cursor = db.cursor(cursor=pymysql.cursors.DictCursor)
    print("已連線資料庫：", db)
except pymysql.err.OperationalError as oe:
    print("mysql連線失敗：", oe)
    print("結束程式！")

cursor = db.cursor()
SQLCode = ' SELECT vid'
SQLCode += ' FROM hi8.hashtagXcategory'
cursor.execute(SQLCode)
vidDataS_vid = cursor.fetchall()
# 找到hashtagXcategory裡最後一支更新影片的vid
vidDataS_last_vid = vidDataS_vid[len(vidDataS_vid) - 1]
vidDataS_last_vid = vidDataS_last_vid[0]


end=time.time()
print('程式時間戳1:',end-start)
#----------------------------------------讀取mainDB的portfolio ------------------------------------------------
# 讀取raw data
try:
    db = pymysql.connect(host='34.81.116.125', user='hi8dev-View', passwd='hi8dev27332018', db='hi8')
    cursor = db.cursor(cursor=pymysql.cursors.DictCursor)
    print("已連線資料庫：", db)
except pymysql.err.OperationalError as oe:
    print("mysql連線失敗：", oe)
    print("結束程式！")

cursor = db.cursor()
SQLCode = ' SELECT  id,m_id,description_decode,category,created_at,updated_at'
SQLCode += f' FROM hi8.portfolio where id >{vidDataS_last_vid}'    #為portfolio新更新影片的資訊
cursor.execute(SQLCode)
vidDataS1 = cursor.fetchall()    #vidDataS1為portfolio新更新影片的資訊



#-----------------轉成dataframe  df2 在這代表欲加入hashtagXcategory但還未經去空白處理過的dataframe---------------------
try:
    df2=pd.DataFrame(vidDataS1)
    df2.columns=['vid','m_id','description_decode','category','created_at','updated_at']
    hashtag2= df2['description_decode']
except:
    print('未讀取到新的影片資訊')
    os._exit(0)
end=time.time()
print('程式時間戳2:',end-start)
#----------------------------------------------去空白處理---------------------------------------------------------
import time

#把description_decode欄位內的 \n 與 ' ' 去掉
#hashtag 在這定義為整理過後的description_decode
df2=df2.replace('\n','', regex=True)
df2=df2.replace(' ','',regex=True)

end = time.time()
print('程式時間戳3：',end - start,'秒')


#---------------------------去空白處理過後的新影片資訊---------------------------------
hashtag2= df2['description_decode']
hashtag2

# 先把要存入新table的欄位 用list的方式挑出來  於是建立空的list
test=[]
vid_plus=[]
m_id_plus=[]
title_plus=[]
hashtag_2_plus=[]
created_at_plus=[]
updated_at_plus=[]
category_plus=[]

# 寫兩個迴圈目的在於把hashtag拆成一個hashtag佔一列  然後重新建立一個dataframe
for i in range(len(hashtag2)):
    hashtag_split=hashtag2[i].split('#')
    for n in range(1,len(hashtag_split)):
        hashtag_1=''
        #print(hashtag_split[n])
        hashtag_1+=('#'+hashtag_split[n])
        vid_plus.append((df2['vid'])[i])
        m_id_plus.append((df2['m_id'])[i])
        title_plus.append(hashtag_split[0])
        hashtag_2_plus.append(hashtag_1)
        category_plus.append((df2['category'])[i])
        created_at_plus.append((df2['created_at'])[i])
        updated_at_plus.append((df2['updated_at'])[i])
    #test.append(hashtag_1)
    #print(hashtag_1)
#建立新的dataframe
new_table={
    "vid":vid_plus,
    "m_id":m_id_plus,
    "title":title_plus,
    "hashtag":hashtag_2_plus,
    "category":category_plus,
    "created_at":created_at_plus,
    "updated_at":updated_at_plus
}
hashtagXcategory_plus = pd.DataFrame(new_table)


#--------------------------------從localDB裡挑出舊的hashtagXcategory-------------------------------

import pandas as pd
import pymysql
from sqlalchemy import create_engine
import time

start = time.time()
pymysql.install_as_MySQLdb()


try:
    # 寫入localDB
    DB_STRING = 'mysql+mysqldb://root:root@192.168.5.50:3305/hi8?charset=utf8mb4'
    conn = create_engine(DB_STRING)
    print('已連結資料庫：', DB_STRING)
except:
    print('連線失敗')

pd.io.sql.to_sql(hashtagXcategory_plus, 'hashtagXcategory', con=conn, schema='hi8', if_exists='append', chunksize=1000,
                 index=False)

end = time.time()
print('程式總執行時長：',end - start,'秒')


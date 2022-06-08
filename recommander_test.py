import pymysql
import pandas as pd

try:
    db = pymysql.connect(host='34.81.150.19', user='root', passwd='tyz7hzckwJOeNJcb', db='lhu')
    cursor = db.cursor(cursor=pymysql.cursors.DictCursor)
    print("已連線資料庫：", db)
except pymysql.err.OperationalError as oe:
    print("mysql連線失敗：", oe)
    print("結束程式！")

cursor = db.cursor()
SQLCode = ' SELECT * '
# SQLCode+=' ,SUBSTRING(re_portfolio.sharers,re_get_portfolio.sharers+1) as sharers '
# SQLCode+=' ,SUBSTRING(re_portfolio.likers,re_get_portfolio.likers+1) as likers '
# SQLCode+=' ,SUBSTRING(re_portfolio.communications_source,re_get_portfolio.communications_source+1) as communications_source '
SQLCode += ' FROM re_userdata '
# SQLCode+=' INNER JOIN re_get_portfolio ON '
# SQLCode+=' re_portfolio.id=re_get_portfolio.vid '
# SQLCode+=' where LENGTH(re_portfolio.finish_rate_data)!=re_get_portfolio.finish_rate_data'
cursor.execute(SQLCode)
vidDataS = cursor.fetchall()
vidDataS

import mysql.connector

mydb = mysql.connector.connect(
    host="192.168.5.50",
    user="root",
    password="root",
    database="hi8"
)
mycursor = mydb.cursor()
sql = "INSERT INTO new_table (id,uid,vid,sharers,likers,communications_source,finish_rate_data,updated_at) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
val = vidDataS
mycursor.executemany(sql, val)
mydb.commit()
print('successfully')

print('error')
cursor.close()
import pymysql
import pandas as pd


try:
    db = pymysql.connect(host='192.168.5.50', port=3306, user='root', passwd='root', db='hi8')
    cursor = db.cursor(cursor=pymysql.cursors.DictCursor)
    print("已連線資料庫：", db)
except pymysql.err.OperationalError as oe:
    print("mysql連線失敗：", oe)
    print("結束程式！")



cursor = db.cursor()
sql='select finish_rate_data from hi8.portfolio where portfolio.id=1134'
cursor.execute(sql)

data=cursor.fetchall()
print(data)

cursor.close()
db.close()

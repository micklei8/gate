import json
import sqlite3
import os
import sys
import time
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess

#检查币是否存在数据库内
def check(db_name, table_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    sql = '''SELECT tbl_name FROM sqlite_master WHERE type = 'table' '''
    cursor.execute(sql)
    values = cursor.fetchall()
    tables = []
    for v in values:
        tables.append(v[0])
    if table_name not in tables:
        return False  # 可以建表
    else:
        return True  # 不能建表

#增加到交易数据库
def zengjia(ID, TIME, BI, JIAGE, SY):
    conn = sqlite3.connect('currency2.db')
    cur = conn.cursor()
    if (check("currency2.db", "jilu") == False):
        sql_text_1 = f'''CREATE TABLE "jilu"
                                    (ID NUMBER,
                                        TIME NUMBER,
                                        BI NUMBER,
                                        JIAGE NUMBER,
                                        SY NUMBER);'''  # 使用记录
        # 执行sql语句
        cur.execute(sql_text_1)
    S_ID = ID
    S_TIME = TIME
    S_BI = BI
    S_JIAGE = JIAGE
    S_SY = SY
    data = [(S_ID, S_TIME, S_BI, S_JIAGE, S_SY)]
    sql_text_2 = f'''INSERT INTO 'jilu' VALUES (?,?,?,?,?)'''
    cur.executemany(sql_text_2, data)
    conn.commit()
    cur.close()
    conn.close()


# 自动运行
def job():
    try:
        # 调用subprocess模块的run函数来运行.exe文件
        #result = subprocess.run(exe_path)
        # 打开json数据文件读取gate数据
        host = "https://api.gateio.ws"
        prefix = "/api/v4"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        url = '/spot/tickers'
        query_param = ''
        r = requests.request('GET', host + prefix + url, headers=headers)
        data = r.json()

        # 时间戳
        timenow = int(time.time())
        print("当前时间戳为：", timenow)

        USDTdata = data
        print(USDTdata)
        conn = sqlite3.connect('currency.db')
        cur = conn.cursor()

        for i in range(USDTdata.__len__()):
            # print(USDTdata[i])
            tablename = f"leilei{USDTdata[i]['currency_pair']}"
            panduanzl = str(USDTdata[i]["currency_pair"]).split("_")
            # print(panduanzl[1], panduanzl)
            if (panduanzl[1] == 'USDT'):
                print(panduanzl[0], panduanzl[1])
                if (check("currency.db", tablename) == False):
                    S_zdb=0
                    sql_text_1 = f'''CREATE TABLE {tablename}
                            (ID NUMBER,
                                TIME NUMBER,
                                BI NUMBER,
                                JIAGE NUMBER,
                                H24H NUMBER,
                                L24H NUMBER,
                                CJL NUMBER,
                                XJL NUMBER,
                                ZDB NUMBER,
                                CBW NUMBER);'''
                    # 执行sql语句
                    cur.execute(sql_text_1)

                S_time = timenow
                S_currency_pair = USDTdata[i]['currency_pair']
                S_last = USDTdata[i]['last']
                S_high_24h = USDTdata[i]['high_24h']
                S_low_24h = USDTdata[i]['low_24h']
                S_base_volume = USDTdata[i]['base_volume']
                S_quote_volume = USDTdata[i]['quote_volume']

                # sql_text_3 = "SELECT * FROM leileiBTC_USDT ORDER BY '涨跌比zdb' DESC LIMIT 1;"
                sql_text_3 = f'''SELECT * FROM {tablename} ORDER BY ID DESC LIMIT 1;'''
                cur.execute(sql_text_3)
                result2 = cur.fetchall()
                if (len(result2) == 0):
                    S_zdb = 0
                    S_ID = 1
                else:
                    S_last_last = result2[0][3]
                    S_ID = int(result2[0][0]) + 1
                    if (float(S_last_last) != 0):
                        S_zdb = (float(S_last) - float(S_last_last)) / float(S_last_last)
                # 涨跌比
                if (S_zdb >= 0.05) :
                    S_chao = 1
                    #zengjia(S_ID, S_time, S_currency_pair, S_last)  #一次超百分之五记录
                    if (result2[0][9] == 1):
                        S_chao = 2
                    zengjia(S_ID, S_time, S_currency_pair, S_last, S_chao)
                else:
                    S_chao = 0

                data = [(S_ID, S_time, S_currency_pair, S_last, S_high_24h, S_low_24h, S_base_volume, S_quote_volume ,S_zdb ,S_chao)]
                sql_text_2 = f'''INSERT INTO {tablename} VALUES (?,?,?,?,?,?,?,?,?,?)'''
                cur.executemany(sql_text_2, data)
                conn.commit()
                # cur.close()
                # conn.close()
                print("成功!")

        if result.returncode == 0:
            print("程序成功运行！")
        else:
            print("程序运行失败！返回值为", result.returncode)
    except FileNotFoundError as e:
        print(".exe文件不存在或无法访问：", str(e))
    except Exception as e:
        print("发生了错误：", str(e))


# BlockingScheduler
scheduler = BlockingScheduler()
scheduler.add_job(job, 'interval', seconds=180)  #调整记录时间
scheduler.start()

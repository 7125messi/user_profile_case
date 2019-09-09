#!/usr/bin/python
# -*- coding: UTF-8 -*-
import MySQLdb #pymysql
import sys
import datetime

data_date_1 = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
data_date_2 = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

def main():
    host = "10.xx.xx.191"
    db = MySQLdb.connect(host=host, port=3306, user="username", passwd="password",
                          db="userprofile", charset="utf8")
    cursor = db.cursor()
    sql_1 = ''' SELECT remark \
	            FROM userprofile.t_step_log \
	          WHERE data_date = '{}' \
	            AND step = 0\
        '''.format(data_date_1)
    sql_2 = ''' SELECT task_stat \
                  FROM userprofile.t_sys_log \
                 WHERE task_name like '%job_dw_user%' \
                   AND task_stat = 2 \
                   AND date_format(task_sign,'%Y-%m-%d') = '{}' \
            '''.format(data_date_2)
    cursor.execute(sql_1)
    raws = cursor.fetchall()   # 查询出来存在的表
    print(len(raws))
    cursor.execute(sql_2)
    number = cursor.fetchall()
    print(len(number))

    if len(raws) == 1 & len(number) ==1:
        print('All table done')
    else:
        raise RuntimeError("No task is done")

if __name__== '__main__':
    main()



# -*- coding:utf-8 -*-
import pymysql
import re

def connection():
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='admin',
        db='circuit',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    return conn

def get_job():
    conn = connection()
    cursor = conn.cursor()
    sql_str = 'SELECT * FROM `job_shanghai_clean` '
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def update_data(job_id,main_mess,job_class):
    conn = connection()
    cursor = conn.cursor()
    sql = "update `job_shanghai_clean_tiqu` set job_class = '{}',job_zhuanye='{}' where job_id ='{}'".format(job_class , main_mess,job_id)
    print(sql)
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

def find_chinese(file):
    pattern = re.compile(r'[^\u4e00-\u9fa5]')
    chinese = re.sub(pattern, '', file)
    # print(chinese)
    return chinese

def main():
    results = get_job()
    for rs in results:
        job_detail = str(rs['job_detail']).strip()
        job_id = str(rs['job_id'])
        detail_list = job_detail.split('\n')
        l = len(detail_list)
        # job_class = re.findall('职能类别：(.+?)关键字',str(job_detail ))
        # print(detail_list)
        main_mess = ''
        job_class = ''
        class_check = False
        for i in range(l-1,0,-1):
            d = detail_list[i]
            if '职能类别' in d:
                job_class = d[5:]
                break
        d_2_list = re.split('[；，。 ]', job_detail)
        for d2 in d_2_list:
            if '专业' in d2:
                main_mess = main_mess + d2
        main_mess_str = find_chinese(main_mess)

        print(job_id,main_mess_str, job_class)
        try:
            update_data(job_id,main_mess_str,job_class)
        except Exception as e:
            continue
        # break

if __name__ == '__main__':
    main()
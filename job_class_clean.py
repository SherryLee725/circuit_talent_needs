# -*- coding:utf-8 -*-
import pymysql
import re
import csv


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


def get_job(table_name):
    conn = connection()
    cursor = conn.cursor()
    sql_str = 'SELECT * FROM `{}` '.format(table_name)
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results


def clean_kuohao(name_str):
    name_new = re.sub("\(.*?\)||（.*?\）", "", name_str)
    return name_new


def update_data(job_class_old, job_class_new):
    conn = connection()
    cursor = conn.cursor()
    sql_str = "update job_shanghai_clean_tiqu_3 set job_class = '{}'where job_class ='{}'".format(job_class_new,
                                                                                               job_class_old)
    cursor.execute(sql_str)
    conn.commit()
    cursor.close()
    conn.close()


def main():
    # results = get_job('job_shanghai_clean_tiqu')
    # # f0 = open('job_class_xiegang.csv', 'a', newline='', encoding='utf-8')
    # # csv0 = csv.writer(f0)
    # for rs in results:
    #     job_class_old = rs['job_class'].strip()
    #     job_class_new = clean_kuohao(job_class_old)
    #     print(job_class_old,job_class_new)
    #     # if '\\' in job_class_old:
    #     #     print(job_class_old)
    #     #     csv0.writerow([job_class_old])
    #     update_data(job_class_old, job_class_new)
    #
    # results_1 = get_job('job_shanghai_clean_tiqu_2')
    # class_dict = {}
    # for rs in results_1:
    #     job_class_new = rs['job_class']
    #     # if '/' in job_class_new:
    #     #     print(job_class_new)
    #     #     class_dict[job_class_new] = class_dict.get(job_class_new, 0) + 1
    #     class_dict[job_class_new] = class_dict.get(job_class_new, 0) + 1

    results_2 = get_job('job_shanghai_clean_tiqu_3')
    class_dict = {}
    for r in results_2:
        job_class = r['job_class'].strip()
        job_class_list = job_class.split('、')
        for j in job_class_list:
            class_dict[j] = class_dict.get(j, 0) + 1

    f1 = open('job_class_all_for1_final.csv', 'a', newline='', encoding='utf-8')
    csv1 = csv.writer(f1)
    csv1.writerow(['job_class', 'sum'])
    for key, value in class_dict.items():
        csv1.writerow([str(key), value])
    f1.close()

def get_job_class_by_name(table_name, job_class_name):
    conn = connection()
    cursor = conn.cursor()
    sql_str = "SELECT * FROM `{}` where job_class like '%{}%' ".format(table_name,job_class_name)
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def main_1(job_name,job_name_new):
    results = get_job_class_by_name('job_shanghai_clean_tiqu_3', job_name)
    for rs in results:
        job_class_old = rs['job_class'].strip()
        job_class_new = job_class_old.replace(job_name,job_name_new)
        print('前：{}    后：{}'.format(job_class_old,job_class_new))
        update_data(job_class_old,job_class_new)

if __name__ == '__main__':
    main_1('电池工程师/技术员','电源开发工程师/技术员')
    # 物业管理维修员 物业管理设施管理人员 物业管理管理经理/主管
    # main()

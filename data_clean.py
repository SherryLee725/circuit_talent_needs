# -*- coding:utf-8 -*-
import pymysql

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
    sql_str = "SELECT * FROM `job_shanghai` "
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def cal_salary(salary_cut,unit=1):
    salary_list = salary_cut.split('-')
    salary_sum = 0
    for s in salary_list:
        salary_sum = salary_sum + eval(s)
    salary_mean = salary_sum*unit/2
    return salary_mean

def update_salary(salary_mean,job_id):
    conn = connection()
    cursor = conn.cursor()
    sql_str =  """update `job_shanghai_clean` set job_salary='{}' WHERE job_id='{}'""".format(str(salary_mean),job_id)
    cursor.execute(sql_str)
    conn.commit()
    cursor.close()
    conn.close()


def clean_salary():
    job_rs = get_job()
    for rs in job_rs:
        job_id = rs['job_id']
        job_salary = rs['job_salary']
        if "千/月" in job_salary:
            unit = 1000
            salary_cut = job_salary[:-3]
            salary_mean = cal_salary(salary_cut,unit)
        elif "万/月" in job_salary:
            unit = 10000
            salary_cut = job_salary[:-3]
            salary_mean = cal_salary(salary_cut, unit)
        elif "万/年" in job_salary:
            unit = 10000
            salary_cut = job_salary[:-3]
            salary_mean = cal_salary(salary_cut, unit)/12
        elif "月/年" in job_salary:
            unit = 1000
            salary_cut = job_salary[:-3]
            salary_mean = cal_salary(salary_cut, unit) / 12
        elif "None" in job_salary:
            salary_mean = ''
        else:
            salary_mean = job_salary
            with open('salary_problem.txt','a') as f1:
                f1.write(job_id)
        print(salary_mean)
        update_salary(salary_mean,job_id)

if __name__ == '__main__':
    clean_salary()
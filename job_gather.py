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

def check_id(job_id):
    conn = connection()
    cursor = conn.cursor()
    sql_str = "SELECT * FROM `job_gather` where job_id='{}'".format(job_id)
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_detail():
    conn = connection()
    cursor = conn.cursor()
    sql_str = 'SELECT * FROM `job_detail_all_clean_add` '
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_shortmess_all():
    conn = connection()
    cursor = conn.cursor()
    sql_str = 'SELECT * FROM `job_shortmess_all` '
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_shortmess(job_id):
    conn = connection()
    cursor = conn.cursor()
    sql_str = "SELECT DISTINCT * FROM `job_shortmess_all` where job_id= '{}'".format(job_id)
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def insert_data(ga):
    conn = connection()
    cursor = conn.cursor()
    cursor.execute(
        """insert into job_gather(job_id, job_name, job_salary, com_name,job_detail, com_detail, job_address,
                   job_experience, job_study, job_num, com_id, job_workarea_id, com_type_text, job_welf, com_size_text,
                   com_ind_text)
         values( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", ga)
    conn.commit()
    cursor.close()
    conn.close()

def output_job_id(job_id):
    with open('no_job_id.txt','w') as f1:
        f1.write(job_id+'\n')

def main():
    # get detaill
    detail_rs = get_detail()
    # detail_rs = get_shortmess_all()
    for ds in detail_rs:
        job_id = str(ds['job_id']).strip()
        check_rs = check_id(job_id)
        # print(len(check_rs))
        if len(check_rs) > 0:
            print('已有job_id')
            continue
        job_name = str(ds['job_name']).strip()
        job_salary = str(ds['job_salary']).strip()
        com_name = str(ds['com_name']).strip()
        job_detail = str(ds['job_detail']).strip()
        com_detail = str(ds['com_detail']).strip()
        job_address = str(ds['job_address']).strip()
        job_experience = str(ds['job_experience']).strip()
        job_study = str(ds['job_study']).strip()
        job_num = str(ds['job_num']).strip()
        shortmess_rs = get_shortmess(job_id)
        if len(shortmess_rs) > 0 :
            shortmess_job = shortmess_rs[0]
            com_id = str(shortmess_job['com_id']).strip()
            job_workarea_id = str(shortmess_job['job_workarea_id']).strip()
            com_type_text = str(shortmess_job['com_type_text']).strip()
            job_welf = str(shortmess_job['job_welf']).strip()
            com_size_text = str(shortmess_job['com_size_text']).strip()
            com_ind_text = str(shortmess_job['com_ind_text']).strip()
        else:
            output_job_id(job_id)
            com_id = ''
            job_workarea_id = ''
            com_type_text = ''
            job_welf = ''
            com_size_text = ''
            com_ind_text = ''

        data_ga = [job_id, job_name, job_salary, com_name, job_detail, com_detail, job_address,
                   job_experience, job_study, job_num, com_id, job_workarea_id, com_type_text, job_welf, com_size_text,
                   com_ind_text]
        # print(len(check_rs))
        insert_data(data_ga)

        # break


if __name__ == '__main__':
    main()
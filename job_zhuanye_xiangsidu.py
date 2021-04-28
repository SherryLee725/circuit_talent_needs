# -*- coding:utf-8 -*-
import pymysql
import re
import jieba
import jieba.posseg as pseg

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

def get_job_detail():
    conn = connection()
    cursor = conn.cursor()
    sql_str = "SELECT * from job_shanghai_clean_xiangsi WHERE job_detail_clean is not null"
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def load_stopwords(filename):
    with open(filename,'r', encoding='UTF-8') as f1:
        readlines = f1.readlines()
        stopwords = [line.strip() for line in readlines]
    return stopwords

def update_data(job_detail, job_id):
    conn = connection()
    cursor = conn.cursor()
    sql_str = "update job_shanghai_clean_xiangsi set job_detail_clean = '{}'where job_id ='{}'".format(job_detail, job_id)
    cursor.execute(sql_str)
    conn.commit()
    cursor.close()
    conn.close()

def change_cn(name_str):
    name_cn =''
    for w in name_str:
        if ('\u4e00' <= w <= '\u9fa5'):
            name_cn+=w
    return name_cn

def detail_clean():
    results = get_job_detail()
    name_dict = {}
    pesg_list = ['f', 's', 't', 'nr', 'ns', 'nt', 'nw', 'vd', 'a', 'ad', 'an', 'd', 'm', 'q',  'r', 'p', 'c', 'u',  'xc', 'w', 'PER', 'LOC',  'ORG', 'TIME']
    for rs in results:
        job_detail = rs['job_detail']
        if job_detail is None:
            continue
        job_detail_words = pseg.cut(job_detail)
        job_id = rs['job_id']
        job_detail_clean = ''
        stopwords = load_stopwords('哈工大停用词表.txt')
        stopwords_js = load_stopwords('集成电路停用词表_1.txt')
        for word, flag in job_detail_words:
            if word not in stopwords_js and word not in stopwords:
                if flag in pesg_list:
                    print('词：{}，词性：{}'.format(word, flag))
                    continue
                word = change_cn(word)
                if len(word) >=1:
                    job_detail_clean += word
                    job_detail_clean += ' '
                    name_dict[word] = name_dict.get(word, 0) + 1
        print(job_detail_clean)
        update_data(job_detail_clean, job_id)
        # break
    with open('job_detail_0.txt', 'a', encoding='UTF-8') as f1:
        for key, value in name_dict.items():
            f1.write(key + '\t' + str(value) + '\n')

def detail_clean_clean():
    results = get_job_detail()
    for rs in results:
        job_detail_clean = rs['job_detail_clean']
        job_id = rs['job_id']

        job_detail_clean = job_detail_clean.replace('工程师','')
        job_detail_list =job_detail_clean.split()
        job_detail_clean_new = list(set(job_detail_list))
        job_detail_clean_new_str = ''
        for j in job_detail_clean_new:
            job_detail_clean_new_str += j
            job_detail_clean_new_str += ' '
        print(job_detail_clean)
        print(job_detail_clean_new_str)
        print('==================================================')
        update_data(job_detail_clean_new_str, job_id)
        # break

if __name__ == '__main__':
    # detail_clean()
    detail_clean_clean()
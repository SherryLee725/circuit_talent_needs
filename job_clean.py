# -*- coding:utf-8 -*-
import pymysql
import jieba
import re
from string import digits
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

def job_sum():
    job_rs = get_job()
    name_dict = {}
    for rs in job_rs:
        job_name = rs['job_name']
        name_dict[job_name] = name_dict.get(job_name,0)+1

    with open('job_name_sum.txt','a', encoding='UTF-8') as f1 :
        for key,value in name_dict.items():
            f1.write(key+'\t'+str(value)+'\n')

def load_stopwords(filename):
    with open(filename,'r', encoding='UTF-8') as f1:
        readlines = f1.readlines()
        stopwords = [line.strip() for line in readlines]
    return stopwords

def name_clean(name_str):
    name_1 = re.sub("\\（.*?）|\\{.*?}|\\[.*?]|\\【.*?】", "", name_str)
    remove_digits = str.maketrans('', '', digits)
    name_2 = name_1.translate(remove_digits)
    name_3 = name_2.strip().lower()
    name_4 = change_cn(name_3)
    return name_4

def change_cn(name_str):
    name_cn =''
    for w in name_str:
        if ('\u4e00' <= w <= '\u9fa5'):
            name_cn+=w
    return name_cn

def words_cut():
    job_rs = get_job()
    name_dict = {}
    stopwords = load_stopwords('哈工大停用词表.txt')
    stopwords_diming = load_stopwords('中国地名停用词表.txt')
    for rs in job_rs:
        job_name = rs['job_name']
        job_name_new = name_clean(job_name)
        job_name_cut = jieba.cut(job_name_new)
        job_name_clean = ''
        for jc in job_name_cut:
            print(jc)
            if jc not in stopwords and jc not in stopwords_diming:
                # job_name_clean += jc.strip().lower()
                # job_name_clean += ''
                name_dict[jc.strip().lower()] = name_dict.get(jc, 0) + 1
    with open('job_name_cut_sum_clean_english_diming.txt','a', encoding='UTF-8') as f1 :
        for key,value in name_dict.items():
            f1.write(key+'\t'+str(value)+'\n')


if __name__ == '__main__':
    words_cut()

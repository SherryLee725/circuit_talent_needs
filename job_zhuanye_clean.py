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

def get_job():
    conn = connection()
    cursor = conn.cursor()
    sql_str = "SELECT * FROM `job_shanghai_clean_xiangsi_api_1` where job_zhuanye != ''"
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_job_class_by_name(table_name, job_class_name):
    conn = connection()
    cursor = conn.cursor()
    sql_str = "SELECT * FROM `{}` where job_class like '%{}%' ".format(table_name,job_class_name)
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

def update_data(job_zhuanye,job_zhuanye_clean):
    conn = connection()
    cursor = conn.cursor()
    sql_str = "update job_shanghai_clean_xiangsi_api_1 set job_zhuanye_clean_1 = '{}'where job_zhuanye_clean ='{}'".format(job_zhuanye_clean,
                                                                                                             job_zhuanye)
    cursor.execute(sql_str)
    conn.commit()
    cursor.close()
    conn.close()

def cal_cipin():
    results = get_job()
    name_dict = {}
    for rs in results:
        job_zhuanye = rs['job_zhuanye_clean_1']
        job_zhuanye_list = job_zhuanye.split()
        for word in job_zhuanye_list:
            name_dict[word] = name_dict.get(word, 0) + 1
    print(name_dict)
    with open('job_zhuanye_0401.txt', 'a', encoding='UTF-8') as f1:
        for key, value in name_dict.items():
            f1.write(key + '\n')

def change_zhuanye(zhuanye_name,zhuanye_name_new):
    results = get_job_class_by_name('job_shanghai_clean_tiqu_4', zhuanye_name)
    for rs in results:
        job_class_old = rs['job_class'].strip()
        job_class_new = job_class_old.replace(zhuanye_name,zhuanye_name_new)
        print('前：{}    后：{}'.format(job_class_old,job_class_new))
        update_data(job_class_old,job_class_new)

def zhuanye_clean():
    results = get_job()
    name_dict = {}
    for rs in results:
        job_zhuanye = rs['job_zhuanye_clean'].split()
        job_zhuanye_words = job_zhuanye
        job_zhuanye_clean = ''
        stopwords_js = load_stopwords('集成电路停用词表.txt')
        for word in job_zhuanye_words:
            if word not in stopwords_js or '相关' in word:
                job_zhuanye_clean += word
                job_zhuanye_clean += ' '
                name_dict[word] = name_dict.get(word, 0) + 1
        print('前：{}，后：{}'.format(job_zhuanye,job_zhuanye_clean))
            # print('词：{}，词性：{}'.format(word,flag))
            # print(jc)
            # if jc not in stopwords:
            #     job_name_clean += jc.strip().lower()
            #     job_name_clean += ''
                # name_dict[jc.strip().lower()] = name_dict.get(jc, 0) + 1
        if job_zhuanye != job_zhuanye_clean:
            update_data(job_zhuanye,job_zhuanye_clean)
    with open('job_zhuanye_2.txt', 'a', encoding='UTF-8') as f1:
        for key, value in name_dict.items():
            f1.write(key + '\t' + str(value) + '\n')

def zhuanye_clean_0():
    results = get_job()
    name_dict = {}
    pesg_list=['a','ad','an','e','f','m','mq','p','q','r','u','y']
    for rs in results:
        job_zhuanye = rs['job_zhuanye']
        job_zhuanye_words = pseg.cut(job_zhuanye)
        job_zhuanye_clean = ''
        stopwords = load_stopwords('哈工大停用词表.txt')
        stopwords_js = load_stopwords('集成电路停用词表_0.txt')
        for word,flag in job_zhuanye_words:
            if word not in stopwords_js and word not in stopwords:
                if flag in pesg_list:
                    print('词：{}，词性：{}'.format(word, flag))
                    continue
                job_zhuanye_clean += word
                job_zhuanye_clean += ' '
                name_dict[word] = name_dict.get(word, 0) + 1
        print('前：{}，后：{}'.format(job_zhuanye, job_zhuanye_clean))
        update_data(job_zhuanye, job_zhuanye_clean)
        # print('词：{}，词性：{}'.format(word,flag))
        # print(jc)
        # if jc not in stopwords:
        #     job_name_clean += jc.strip().lower()
        #     job_name_clean += ''
        # name_dict[jc.strip().lower()] = name_dict.get(jc, 0) + 1
        # if job_zhuanye != job_zhuanye_clean:
        #     update_data(job_zhuanye, job_zhuanye_clean)
    with open('job_zhuanye_0.txt', 'a', encoding='UTF-8') as f1:
        for key, value in name_dict.items():
            f1.write(key + '\t' + str(value) + '\n')


def zhaunye_clean_clean():
    results = get_job()
    stopwords = load_stopwords('stopwords.txt')
    for rs in results:
        job_zhuanye_clean = rs['job_zhuanye_clean']
        clean_list = job_zhuanye_clean.split()
        clean_new = ''
        for c in clean_list:
            c = c.replace('相关','').replace('专业','')
            if c not in stopwords:
                if c not in clean_new:
                    clean_new += c
                    clean_new +=' '
        print(job_zhuanye_clean)
        print(clean_new)
        print('='*20)
        update_data(job_zhuanye_clean,clean_new)
        # break


if __name__ == '__main__':
    # zhuanye_clean_0()
    # zhuanye_clean()
    cal_cipin()
    # zhaunye_clean_clean()
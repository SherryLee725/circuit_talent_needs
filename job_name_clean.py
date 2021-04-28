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
    sql_str = 'SELECT * FROM `job_shanghai_clean_final_1` '
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def update_data(job_id,job_name):
    conn = connection()
    cursor = conn.cursor()
    sql = "update `job_shanghai_clean_final_1` set job_name_new='{}' where job_id = '{}'".format(job_name,job_id)
    # print(sql)
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

def load_stopwords(filename):
    with open(filename,'r', encoding='UTF-8') as f1:
        readlines = f1.readlines()
        stopwords = [line.strip() for line in readlines]
    return stopwords

def name_remove_digit(name_str):
    remove_digits = str.maketrans('', '', digits)
    name_new = name_str.translate(remove_digits)
    return name_new

def name_remove_gangweibianhao(name_str):
    name_new = re.sub("（.*?）|{.*?}|\[.*?\]|【.*?】|\(.*?\)|\(.*?）|（.*?\)", "", name_str)
    # name_end = name_new+' end'
    # name_new = re.sub("/.*? end", "", name_end)
    name_end = name_new + ' end'
    name_new = re.sub("-.*? end", "", name_end)
    name_end = name_new + ' end'
    name_new = re.sub(":.*? end", "", name_end)
    name_end = name_new + ' end'
    name_new = re.sub("：.*? end", "", name_end)
    name_new= name_new.replace(' end','').strip()
    return name_new



def name_remove_diming_other(name_str):
    job_name_cut = jieba.cut(name_str)
    stopwords = load_stopwords('哈工大停用词表.txt')
    stopwords_diming = load_stopwords('中国地名停用词表.txt')
    job_name_clean = ''
    for jc in job_name_cut:
        # print(jc)
        if jc not in stopwords and jc not in stopwords_diming:
            job_name_clean += jc.strip().lower()
            job_name_clean += ' '
    return job_name_clean

def check_cn(name_str):
    for n in name_str:
        if ('\u4e00' <= n <= '\u9fa5'):
            return True
    return False

def main():
    job_rs = get_job()
    print('get ok')
    for rs in job_rs:
        job_name = rs['job_name']
        job_id = rs['job_id']
        try:
            job_name_1 = name_remove_digit(job_name)
            job_name_2 = name_remove_gangweibianhao(job_name_1)
            job_name_3 = job_name_2.split(' ')
            job_name_4 =''
            for j in job_name_3:
                if_cn = check_cn(j)
                if not if_cn:
                    continue
                job_name_4 = j +' '
            print("清洗前：{}，清洗后：{}".format(job_name, job_name_4))
        except Exception as e:
            print('出错 没有清洗')
            with open ('name_clean_problem','a',encoding='utf-8') as f1:
                f1.write(job_name+'\n')
            continue
        update_data(job_id,job_name_2)
        # break


main()
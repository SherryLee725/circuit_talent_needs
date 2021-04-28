import pymysql
import jieba

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

def load_stopwords(filename):
    with open(filename,'r', encoding='UTF-8') as f1:
        readlines = f1.readlines()
        stopwords = [line.strip() for line in readlines]
    return stopwords


def main():
    results = get_job('job_shanghai_clean_xiangsi_api_1')
    detail_words = {}
    for rs in results:
        # job_id = rs['job_id']
        job_detail = rs['job_detail_clean']
        if job_detail is None:
            continue
        job_detail_list = job_detail.strip().split(' ')
        for j in job_detail_list:
            detail_words[j] = detail_words.get(j,0) + 1
        # break
    with open('job_detail_words.txt','a') as f0:
        for key,value in detail_words.items():
            f0.write(str(key)+','+str(value)+'\n')


if __name__ == '__main__':
    main()


# -*- coding:utf-8 -*-
import requests
import json
import pymysql
import time
import random
from bs4 import BeautifulSoup


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


def insert_data(job_detail):
    conn = connection()
    cursor = conn.cursor()
    cursor.execute(
        """insert into job_detail_words(job_id, job_name, job_salary, com_name, com_href, job_detail, com_detail, job_address, job_experience, job_study, job_num,job_need_str)
         values( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", job_detail)
    conn.commit()
    cursor.close()
    conn.close()


def get_job_id():
    conn = connection()
    cursor = conn.cursor()
    sql_str = 'SELECT * FROM `job_shortmess_words` '
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results


def get_job_id_state():
    conn = connection()
    cursor = conn.cursor()
    sql_str = 'SELECT * FROM `job_shortmess_tag` where id > 25677 '
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results


def read_html(job_id, content):
    job_name = ''
    job_salary = ''
    job_need = ''
    job_need_list = ''
    com_name = ''
    com_href = ''
    job_detail = ''
    com_detail = ''
    job_address = ''
    job_experience = ''
    job_study = ''
    job_num = ''
    job_need_str = ''
    soup = BeautifulSoup(content, "html.parser")
    try:

        job_name = soup.find('div', attrs={'class': 'cn'}).find('h1').text.strip()
        # print(job_name)
        job_salary = soup.find('div', attrs={'class': 'cn'}).find('strong').text.strip()
        # print(job_salary)
        job_need = soup.find('div', attrs={'class': 'cn'}).find('p', attrs={'class': "msg ltype"}).text.strip()
        # print(job_need)
        job_need_list = job_need.split("|")
        if len(job_need_list) >= 4:
            job_address = job_need_list[0].strip()
            job_experience = job_need_list[1].strip()
            job_study = job_need_list[2].strip()
            job_num = job_need_list[3].strip()
            print(job_address, job_experience, job_study, job_num)
        else:
            for j in job_need_list:
                job_need_str = job_need_str + " " + str(j)
            print(job_need_str)

        com_name = str(soup.find('a', attrs={'class': 'catn'}).get('title')).strip()
        # print(com_name)
        com_href = str(soup.find('a', attrs={'class': 'catn'}).get('href')).strip()
        # print(com_href)
        job_detail = soup.find('div', attrs={'class': 'bmsg job_msg inbox'}).text.strip()
        # print(job_detail)
        com_detail = soup.find('div', attrs={'class': 'tmsg inbox'}).text.strip()
        # print(com_detail)


        job_lists = [job_id, job_name, job_salary, com_name, com_href, job_detail, com_detail, job_address,
                     job_experience, job_study, job_num, job_need_str]
        print(job_lists)
        return job_lists
    except Exception as e:
        print("解析失败：" + str(e))
        with open('problem_job_id_1.txt', 'a') as f1:
            f1.write(str(job_id) + '\n')
        with open('problem_1.txt', 'a') as f2:
            f2.write(str(e) + '\n')
        return None


def get_problem_id():
    with open('problem_url.txt', 'r')as f1:
        url_lists = f1.readlines()
    print(url_lists)
    return url_lists


def get_job_detail():
    # try:
    #     with open('out_state_detail.txt', 'r') as f0:
    #         last_state = f0.readlines()[-1]
    #     if len(last_state.strip()) != 0:
    #         job_id = str(last_state)
    #         print('从上次结束的异常id开始重新获取'.format(job_id))
    #     else:
    #         job_id = ""
    # except Exception as e:
    #     print("无状态文件：" + str(e))
    # f0 = open('out_state_detail.txt', 'w')
    # f0.close()
    # p = 1

    job_id_lists = get_job_id_state()
    # job_id_lists = get_job_id()
    # job_id_lists = get_problem_id()
    for j in job_id_lists:
        job_id = j['job_id']
        url = j['job_href']
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Host': 'jobs.51job.com',
            'Referer': 'https://search.51job.com/list/000000,000000,0000,02,9,99,+,2,1.html?lang=c&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&ord_field=0&dibiaoid=0&line=&welfare=',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
            'Cookie': 'guid=f2ec4823be2a0d34fcbc46cd7d161e80; nsearch=jobarea%3D%26%7C%26ord_field%3D%26%7C%26recentSearch0%3D%26%7C%26recentSearch1%3D%26%7C%26recentSearch2%3D%26%7C%26recentSearch3%3D%26%7C%26recentSearch4%3D%26%7C%26collapse_expansion%3D; search=jobarea%7E%60000000%7C%21ord_field%7E%600%7C%21recentSearch0%7E%60000000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA02%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch1%7E%60000000%A1%FB%A1%FA000000%A1%FB%A1%FA7300%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch2%7E%60020000%A1%FB%A1%FA020100%A1%FB%A1%FA7300%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch3%7E%60020000%A1%FB%A1%FA000000%A1%FB%A1%FA7300%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch4%7E%60020000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21collapse_expansion%7E%601%7C%21'
        }
        try:
            response = requests.request("GET", url, headers=headers)
            page_text = response.content.decode('gbk')
            job_lists = read_html(job_id, page_text)
            if job_lists is not None:
                insert_data(job_lists)
                # return
                print('{}数据插入完成'.format(job_id))
            else:
                continue
        except Exception as e:
            print("无效请求：" + str(e))
            # with open('problem_job_id_1.txt', 'a') as f1:
            #     f1.write(str(job_id) + '\n')
            with open('problem_job_url_1.txt', 'a') as f1:
                f1.write(str(url) + '\n')
            with open('problem_1.txt', 'a') as f2:
                f2.write(str(e) + '\n')
            continue
        time.sleep(random.randint(0, 1) * 3)


if __name__ == '__main__':
    get_job_detail()
    # get_problem_id()

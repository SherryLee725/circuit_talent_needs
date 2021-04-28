# -*- coding:utf-8 -*-
import requests
import json
import pymysql
import time
import random


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


def insert_data(job_list):
    conn = connection()
    cursor = conn.cursor()
    cursor.executemany(
        """insert into job_shortmess_tag(job_id, com_id, job_effect, job_is_special, job_href, job_name, job_title, com_href,
                             com_name, job_salary, job_workarea_id, job_workarea_text, job_updatedate, com_type_text,
                             job_degreefrom, job_workyear, job_issuedate, job_welf , job_attr, com_size_text, com_ind_text)
         values( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", job_list)
    conn.commit()
    cursor.close()
    conn.close()


def get_job_list():
    # p = 0
    params = {
        'lang': 'c',
        'postchannel': '0000',
        'workyear': '99',
        'cotype': '99',
        'degreefrom': '99',
        'jobterm': '99',
        'companysize': '99',
        'ord_field': '0',
        'dibiaoid': '0',
        'line': '',
        'welfare': ''
    }

    # https://search.51job.com/list/000000,000000,0000,02,9,99,+,2,1.html
    # url_1 = "https://search.51job.com/list/000000,000000,7300,00,9,99,+,2,1.html"
    # response_1 = requests.request("GET", url_1, headers=headers, params=params)
    # response_json_1 = json.loads(response_1.text)
    # jobid_count = response_json_1["jobid_count"]  # 共几条职位数据
    # print("共{}条职位信息".format(jobid_count))

    jobid_count = '100000'
    # 先保存第一页数据
    # job_list1 = read_json(response_json_1)
    # if job_list1 is not None:
    #     insert_data(job_list1)

    # 爬取其他页数据
    page_num = eval(jobid_count) // 50 + 1
    try:
        with open('out_state.txt', 'r') as f0:
            last_state = f0.readlines()[-1]
        if len(last_state.strip()) != 0:
            p = eval(last_state)
            print('从上次结束的第{}页开始重新获取'.format(p))
        else:
            p = 1
    except Exception as e:
        print("无状态文件：" + str(e))
        f0 = open('out_state.txt', 'w')
        f0.close()
        p = 1

    for page in range(p, page_num + 1):
        print("已爬取第{}页".format(page))
        #      https://search.51job.com/list/020000,000000,0000,02,9,99,+,2,1.html?lang=c&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&ord_field=0&dibiaoid=0&line=&welfare=
        url = "https://search.51job.com/list/020000,000000,0000,02,9,99,+,2,{}.html".format(page)
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Connection': 'keep-alive',
            'Host': 'search.51job.com',
            'Referer': url,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
            'X-Requested-With': 'XMLHttpRequest',
            'Cookie': 'guid=f2ec4823be2a0d34fcbc46cd7d161e80; nsearch=jobarea%3D%26%7C%26ord_field%3D%26%7C%26recentSearch0%3D%26%7C%26recentSearch1%3D%26%7C%26recentSearch2%3D%26%7C%26recentSearch3%3D%26%7C%26recentSearch4%3D%26%7C%26collapse_expansion%3D; search=jobarea%7E%60000000%7C%21ord_field%7E%600%7C%21recentSearch0%7E%60000000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA02%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch1%7E%60000000%A1%FB%A1%FA000000%A1%FB%A1%FA7300%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch2%7E%60020000%A1%FB%A1%FA020100%A1%FB%A1%FA7300%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch3%7E%60020000%A1%FB%A1%FA000000%A1%FB%A1%FA7300%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch4%7E%60020000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21collapse_expansion%7E%601%7C%21'
        }
        try:
            response = requests.request("GET", url, headers=headers, params=params)
            response_json = json.loads(response.text)
            job_list = read_json(response_json)
            if job_list is not None:
                insert_data(job_list)
                # return
                time.sleep(random.randint(2, 4) * 0.5 + 1)
            else:
                print("解析出错，request状态码{}".format(response.status_code))
                print(response.text)
                with open('out_state.txt', 'a') as f1:
                    f1.write(str(page) + '\n')
                    break
            print("=============数据获取完成===============")
        except Exception as e:
            print("无效请求：" + str(e))
            with open('out_state.txt', 'a') as f1:
                f1.write(str(page) + '\n')
            break


def read_json(response_json):
    search_result = response_json["engine_search_result"]
    if search_result is not None:
        job_num = len(search_result)
        job_list = []
        for j in range(0, job_num):
            job_mes = search_result[j]
            job_id = str(job_mes['jobid'])
            com_id = str(job_mes['coid'])
            job_effect = str(job_mes['effect'])
            job_is_special = str(job_mes['is_special_job'])
            job_href = str(job_mes['job_href'])
            job_name = str(job_mes['job_name'])
            job_title = str(job_mes['job_title'])
            com_href = str(job_mes['company_href'])
            com_name = str(job_mes['company_name'])
            job_salary = str(job_mes['providesalary_text'])
            job_workarea_id = str(job_mes['workarea'])
            job_workarea_text = str(job_mes['workarea_text'])
            job_updatedate = str(job_mes['updatedate'])
            com_type_text = str(job_mes['companytype_text'])
            job_degreefrom = str(job_mes['degreefrom'])
            job_workyear = str(job_mes['workyear'])
            job_issuedate = str(job_mes['issuedate'])
            job_welf = str(job_mes['jobwelf'])
            job_attr = str(job_mes['attribute_text'])
            com_size_text = str(job_mes['companysize_text'])
            com_ind_text = str(job_mes['companyind_text'])

            job_list.append([job_id, com_id, job_effect, job_is_special, job_href, job_name, job_title, com_href,
                             com_name, job_salary, job_workarea_id, job_workarea_text, job_updatedate, com_type_text,
                             job_degreefrom, job_workyear, job_issuedate, job_welf, job_attr, com_size_text,
                             com_ind_text])
        print(job_list)
        return job_list
    else:
        return None


if __name__ == '__main__':
    get_job_list()

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


def insert_data(subject_rs_list_all):
    conn = connection()
    cursor = conn.cursor()
    cursor.executemany(
        """insert into knowladge_subject(knowladge,subject)values( %s, %s)""", subject_rs_list_all)
    conn.commit()
    cursor.close()
    conn.close()


def get_knowledge():
    conn = connection()
    cursor = conn.cursor()
    sql_str = 'SELECT * FROM `knowladge` where id > 387'
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results


def get_subject(word):
    headers = {
        # 'Host': 'xueshu.baidu.com',
        # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0',
        # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        # 'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        # 'Accept-Encoding': 'gzip, deflate, br',
        # 'Referer': 'https://xueshu.baidu.com/',
        # 'Connection': 'keep-alive',
        # 'Cookie': 'Hm_lvt_43115ae30293b511088d3cbe41ec099c=1607320537,1608517750; Hm_lpvt_43115ae30293b511088d3cbe41ec099c=1608517869; BAIDUID=83ABCDF322ABB47AD367B0043E8624CF:FG=1; BIDUPSID=229A2CCC09D8661621EAB9069DE6EA0F; PSTM=1576134585; Hm_lvt_f28578486a5410f35e6fbd0da5361e5f=1607513800,1607566391,1607877234,1608511678; Hm_lvt_35de9cae3c15edd5b8706c3f6166966d=1583598153; SC_BATCH=3; Hm_lvt_43115ae30293b511088d3cbe41ec099c=1607513799,1607566391,1607877233,1608511678; MCITY=-%3A; BDRCVFR[w2jhEs_Zudc]=mbxnW11j9Dfmh7GuZR8mvqV; delPer=0; BDSVRTM=192; BD_HOME=0; H_PS_PSSID=; Hm_lpvt_43115ae30293b511088d3cbe41ec099c=1608517864; Hm_lpvt_f28578486a5410f35e6fbd0da5361e5f=1608517864; BD_CK_SAM=1; PSINO=5; kleck=7573b6192e9d761f4e06b5eda7388f64',
        # 'Upgrade-Insecure-Requests': '1',
        # 'Cache-Control': 'max-age=0'
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'Connection': 'keep-alive',
        'Cookie': 'Hm_lvt_43115ae30293b511088d3cbe41ec099c=1619093972; Hm_lpvt_43115ae30293b511088d3cbe41ec099c=1619095059; BAIDUID=83ABCDF322ABB47AD367B0043E8624CF:FG=1; BIDUPSID=229A2CCC09D8661621EAB9069DE6EA0F; PSTM=1576134585; Hm_lvt_f28578486a5410f35e6fbd0da5361e5f=1619093972; Hm_lvt_43115ae30293b511088d3cbe41ec099c=1619093972; MCITY=-289%3A; BDUSS=ViajRvTlNCaDlOVy1pQkQ3NEFSUHg0TzZDMzBpd3JzUXpPYzEzWTFsV2FYaHBnRVFBQUFBJCQAAAAAAAAAAAEAAABjLC0v46boqujysLIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAZWJjMjg3M2U1MTQ0ODBl; BDRCVFR[w2jhEs_Zudc]=mbxnW11j9Dfmh7GuZR8mvqV; delPer=0; BDSVRTM=497; BD_HOME=0; Hm_lpvt_43115ae30293b511088d3cbe41ec099c=1619095030; Hm_lpvt_f28578486a5410f35e6fbd0da5361e5f=1619095030; BD_CK_SAM=1; PSINO=5; antispam_data=c200ed4bfb960455e4dc1c709f4a1b7b7a54c1084d35f01e1bd05f1200860647e45f2632badb08a9932819b9f8dec6a16416a6a73678b602ffc398b61e7f36e6875512169b57424bcd7a2c2d5f65862ac0f8fc4381c3ce601e8afca1792fa119; antispam_key_id=45; antispam_sign=33bbb1f8; antispam_site=ae_xueshu_paper',
        'Host': 'www.baidu.com',
        'Referer': 'https://xueshu.baidu.com/',
        'Sec-Fetch-Dest': 'script',
        'Sec-Fetch-Mode': 'no-cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36 Edg/87.0.664.66'
    }
    params = {
        'wd': word,
        'rsv_bp': '0',
        'tn': 'SE_baiduxueshu_c1gjeupa',
        'rsv_spt': '3',
        'ie': 'utf-8',
        'f': '8',
        'rsv_sug2': '0',
        'sc_f_para': 'sc_tasktype%3D{firstSimpleSearch}',
        'rsv_n': '2'
    }
    url = 'https://xueshu.baidu.com/s'
    response = requests.request("GET", url, headers=headers, params=params)
    html = response.text
    # print(html)
    soup = BeautifulSoup(html, "html.parser")
    subject_a_list_div = soup.find('div', attrs={'id': 'content_leftnav'}).find_all('div',
                                                                                    attrs={'class': 'leftnav_item'})
    # .find_all('div', attrs={'id': 'leftnav_item'}).find('div', attrs={'id': 'leftnav_list_cont'}).find_all('a')
    print(len(subject_a_list_div))
    subject_rs_list_all = []
    for s in subject_a_list_div:
        s_str = str(s)
        if '领域' in s_str:
            subject_a_list = s.find('div').find_all('a')
            # print(subject_a_list)
            for sa in subject_a_list:
                sa_str = str(sa)
                if 'title' not in sa_str:
                    continue
                # print(sa)
                subject_title = sa['title'].strip()
                # print(subject_title)
                subject_rs_list = [word, subject_title]
                subject_rs_list_all.append(subject_rs_list)
            print(subject_rs_list_all)
            break
    return subject_rs_list_all


def main():
    knowledge_rs = get_knowledge()
    for krs in knowledge_rs:
        knowledge = krs['knowladge']
        subject_rs_list_all = get_subject(knowledge)
        insert_data(subject_rs_list_all)
        time.sleep(random.randint(0, 1) * 3 + 1)


if __name__ == '__main__':
    main()

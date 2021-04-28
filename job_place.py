import requests
import json
import time
import random
import pandas as pd
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
    sql_str = "SELECT DISTINCT com_name FROM job_shanghai_clean_final_3 WHERE job_address = '上海'"
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_part_name(org_name):
    url = "https://restapi.amap.com/v3/place/text"
    params = {
        'key': '1f950b2dd2067775c0a1de7be920cda0',
        'keywords': org_name,
        'types': '科教文化服务',
        'city': '上海',
        # 'offset': 10,
        # 'output':'JSON',
    }
    headers = {
        'Cookie': 'BAIDUID=FBAA261874C5A1FCE6DBA9B6FA4F06B5:FG=1'
    }
    try:
        response = requests.request("GET", url, headers=headers, params=params,timeout=10)
        # print(response.text)
        rs_json = json.loads(response.text)
        # print(rs_json)

        part_name = str(rs_json['pois'][0]['adname'])
        # print(part_name)
        # print('所在行政区{}'.format(part_name))
        return part_name
    except Exception as e:
        print(e)
        return ''

def update_data(com_name, job_address):
    conn = connection()
    cursor = conn.cursor()
    sql = "update `job_shanghai_clean_final_3` set job_address='{}' where com_name = '{}'".format(
        job_address, com_name)
    # print(sql)
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

def main():
    results = get_job()
    print('数据输入完成{}'.format(len(results)))
    i=0
    for rs in results:
        # org_id = row['org_id']
        org_name = rs['com_name']
        if type(org_name) is float:
            print('无中文')
            part_name=''
        else:
            org_name = org_name.strip().replace('。','')
            # lng,lat = get_lng_lat(org_name)
            part_name = get_part_name(org_name)
            if '[' in part_name or part_name =='':
                print('{}找不到地区'.format(org_name))
                part_name = ''
                continue
        part_name = '上海-'+part_name
        print("{}是：{}".format(org_name, part_name))
        # update_date_lng_lat(shop_id,lng,lat)
        # print(lng,lat)
        update_data(org_name, part_name)
        # break
        time.sleep(0.4)

main()
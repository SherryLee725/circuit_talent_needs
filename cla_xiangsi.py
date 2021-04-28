import time
from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.nlp.v20190408 import nlp_client, models
import pymysql
import pandas as pd
import json
def cal_xiangsi(SrcText,TargetText):
    try:
        cred = credential.Credential("AKIDUjGWWFLxiBRSDpgdquITPSWLza4K29ye", "b3F7j9IDLSHoo7A3jkeYDAmxwWv9zayg")
        httpProfile = HttpProfile()
        httpProfile.endpoint = "nlp.tencentcloudapi.com"

        clientProfile = ClientProfile()
        clientProfile.httpProfile = httpProfile
        client = nlp_client.NlpClient(cred, "ap-guangzhou", clientProfile)

        req = models.TextSimilarityRequest()
        params = {
            "SrcText": SrcText, #翻译
            "TargetText": [TargetText] #信号 系统 应用 工程学 电路 测试计划 芯片 版图 集成电路 混合 配合 电子 流片 测试 信号处理 微电子学 数模 模块 软件 数字
        }
        req.from_json_string(json.dumps(params))

        resp = json.loads(client.TextSimilarity(req).to_json_string())
        print(resp)
        resp_score = resp['Similarity'][0]['Score']
        print(resp_score)
        # {
        #     "Response": {
        #         "Similarity": [
        #             {
        #                 "Score": 0.5026428584962127,
        #                 "Text": "信号 系统 应用 工程学 电路 测试计划 芯片 版图 集成电路 混合 配合 电子 流片 测试 信号处理 微电子学 数模 模块 软件 数字"
        #             }
        #         ],
        #         "RequestId": "2e8e41e0-6b6a-48d8-8dab-f986a5c17bc5"
        #     }
        # }
        return resp_score
    except TencentCloudSDKException as err:
        print(err)
        return

def cal_xiangsi_word(SrcText,TargetText):
    # print('SrcText:'+SrcText)
    # print('TargetText:' + TargetText)
    try:
        cred = credential.Credential("AKIDUjGWWFLxiBRSDpgdquITPSWLza4K29ye", "b3F7j9IDLSHoo7A3jkeYDAmxwWv9zayg")
        httpProfile = HttpProfile()
        httpProfile.endpoint = "nlp.tencentcloudapi.com"

        clientProfile = ClientProfile()
        clientProfile.httpProfile = httpProfile
        client = nlp_client.NlpClient(cred, "ap-guangzhou", clientProfile)

        req = models.WordSimilarityRequest()
        params = {
            "SrcWord": SrcText,
            "TargetWord": TargetText
        }
        req.from_json_string(json.dumps(params))

        resp = json.loads(client.WordSimilarity(req).to_json_string())
        print(resp)
        resp_score = resp['Similarity']
        print(resp_score)
        return  resp_score

    except Exception as err:
        print(err)
        return


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

def get_job_detail_clean():
    conn = connection()
    cursor = conn.cursor()
    sql_str = "SELECT * from job_shanghai_clean_xiangsi WHERE job_detail_clean is not null"
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def load_zhuanye_josn(filename):
    zy_json={}
    df_zy = pd.read_excel(filename,header=None)
    for idx, row in df_zy.iterrows():
        zy_class = row[0]
        zhuanye_str = ''
        for i in range(1,11):
            zhuanye_name = row[i]
            if type(zhuanye_name) is str:
                # print(zhuanye_name)
                zhuanye_str  += zhuanye_name
                zhuanye_str  += ' '
        zy_json[zy_class] = zhuanye_str
        # break
    # print(zy_json)
    return zy_json

def update_data(id,fimiliar_josn):
    conn = connection()
    cursor = conn.cursor()
    sql_str = "update jjob_shanghai_clean_xiangsi_api set zhuanye_fimifiliar = '{}'where id ={}".format(fimiliar_josn, id)
    cursor.execute(sql_str)
    conn.commit()
    cursor.close()
    conn.close()

def main_sentence():
    results = get_job_detail_clean()
    zy_json = load_zhuanye_josn('zhuanye_list_jicheng.xlsx')
    for rs in results:
        job_detail_clean = rs['job_zhuanye_clean'].strip()
        id = rs['id']
        fimiliar_josn ={}
        for key,value in zy_json.items():
            zy_name = value
            zy_class = key
            fimiliar_index = cal_xiangsi(zy_name, job_detail_clean)
            print('{}、{}的相似度为：{}'.format(job_detail_clean,zy_class,fimiliar_index))
            if fimiliar_index is not None:
                fimiliar_josn[zy_class] = fimiliar_index
            else:
                with open('xiangsi_erro.txt','a',newline='') as f1:
                    f1.write(str(id))
                print('出错，程序停止，检查')
                return
        print(str(fimiliar_josn))
        # update_data(id,str(fimiliar_josn))
        break

def load_txt(filename):
    with open(filename,'r',encoding='utf-8') as f1:
        zhuanye_one = f1.readlines()
    return zhuanye_one

def sum_index(fimiliar_josn):
    zhuanye_one = load_txt('zhuanye_list_one.txt')
    zhuanye_final = {}
    for z in zhuanye_one:
        zhuanye = z.strip()
        sum_z = 0
        for key,value in fimiliar_josn.items():
            sum_z += value[zhuanye]
        zhuanye_final[zhuanye] = sum_z
        # break
    return zhuanye_final

def main_words():
    zhuanye_one = load_txt('zhuanye_list_one.txt')
    # print(zhuanye_one)
    results = get_job_detail_clean()
    for rs in results:
        job_detail_clean = rs['job_zhuanye_clean'].strip().split()
        id = rs['id']
        fimiliar_josn = {}
        for s in job_detail_clean:
            word_fimiliar_josn = {}
            for w in zhuanye_one:
                w = w.strip()
                fimiliar_index = cal_xiangsi_word(w, s)
                print('{}、{}的相似度为：{}'.format(s, w, fimiliar_index))
                if fimiliar_index is not None:
                    word_fimiliar_josn[w] = fimiliar_index
                else:
                    with open('xiangsi_erro.txt', 'a', newline='') as f1:
                        f1.write(str(id))
                    print('出错，程序停止，检查')
                    return
                time.sleep(0.5)
            print(word_fimiliar_josn)
            fimiliar_josn[s] = word_fimiliar_josn
            # break
            time.sleep(1)
        print(fimiliar_josn)
        # zhuanye_sum_js = sum_index(fimiliar_josn)
        # update_data(id, str(fimiliar_josn))
        # print(zhuanye_sum_js)
        break

if __name__ == '__main__':
    # detail_clean()
    main_sentence()
    # zy_json = load_zhuanye_josn('zhuanye_list_jicheng.xlsx')
    # for key, value in zy_json.items():
    #     zy_name = value
    #     zy_class = key
    #     print(key)
    #     print(value)
    main_words()




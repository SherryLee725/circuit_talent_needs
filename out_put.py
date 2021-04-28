import pymysql
from pyecharts.charts import WordCloud
import pyecharts.options as opts
from pyecharts.globals import SymbolType

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
    sql_str = "SELECT * FROM `job_shanghai_jishu_clean` "
    cursor.execute(sql_str)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results


def cal_job_class():
    name_dict = {}
    results = get_job()
    for rs in results:
        job_class = rs['job_class']
        # print(job_class)
        job_class_list = job_class.strip().strip('、').split('、')
        job_num = rs['job_num'].strip()
        for j in job_class_list:
            name_dict[j] = name_dict.get(j, 0) + eval(job_num)
    print(name_dict)
    with open('job_class_sum_0419.txt', 'a') as f0:
        for key, value in name_dict.items():
            f0.write(str(key) + ',' + str(value) + '\n')


def cal_xueli():
    # xueli_dict = {}
    results = get_job()
    for rs in results:
        job_study = rs['job_study'].strip()
        job_id = rs['job_id'].strip()
        if job_study == 'None':
            continue
        job_class_up_list = rs['job_class_up'].strip().strip('、').split('、')
        job_cu_l = len(job_class_up_list)
        job_class_list = rs['job_class'].strip().strip('、').split('、')
        job_c_l = len(job_class_list)
        job_num = rs['job_num'].strip()
        if job_c_l == job_cu_l:
            # continue
            with open('xueli_class_up_num_final.txt', 'a', newline='') as f0:
                for j in range(0, job_c_l):
                    job_class_up = job_class_up_list[j]
                    job_class = job_class_list[j]
                    mess = job_study + ',' + job_class_up + ',' + job_class + ',' + job_num
                    print(mess)
                    f0.write(mess + '\n')
        else:
            print('岗位与大类数组长度不一致')
            job_class = rs['job_class'].strip()  # .strip('、')
            with open('xueli_class_up_num_2_byy.txt', 'a', newline='') as f0:
                for j in range(0, job_cu_l):
                    job_class_up = job_class_up_list[j]
                    mess = job_study + ',' + job_class_up + ',' + job_class + ',' + job_num
                    print(mess)
                    f0.write(mess + '\n')


def cal_jingyan():
    # xueli_dict = {}
    results = get_job()
    for rs in results:
        job_experience = rs['job_experience'].strip()
        job_id = rs['job_id'].strip()
        job_class_up_list = rs['job_class_up'].strip().strip('、').split('、')
        job_cu_l = len(job_class_up_list)
        job_class_list = rs['job_class'].strip().strip('、').split('、')
        job_c_l = len(job_class_list)
        job_num = rs['job_num'].strip()
        if job_c_l == job_cu_l:
            # continue
            with open('jingyan_class_up_num_final.txt', 'a', newline='') as f0:
                for j in range(0, job_c_l):
                    job_class_up = job_class_up_list[j]
                    job_class = job_class_list[j]
                    mess = job_experience + ',' + job_class_up + ',' + job_class + ',' + job_num
                    print(mess)
                    f0.write(mess + '\n')
        else:
            print('岗位与大类数组长度不一致')
            job_class = rs['job_class'].strip()  # .strip('、')
            with open('jingyan_class_up_num_byy.txt', 'a', newline='') as f0:
                for j in range(0, job_cu_l):
                    job_class_up = job_class_up_list[j]
                    mess = job_experience + ',' + job_class_up + ',' + job_class + ',' + job_num
                    print(mess)
                    f0.write(mess + '\n')


def cal_xinzi():
    # xueli_dict = {}
    results = get_job()
    for rs in results:
        job_salary = rs['job_salary'].strip()
        job_id = rs['job_id'].strip()
        job_class_up_list = rs['job_class_up'].strip().strip('、').split('、')
        job_cu_l = len(job_class_up_list)
        job_class_list = rs['job_class'].strip().strip('、').split('、')
        job_c_l = len(job_class_list)
        job_num = rs['job_num'].strip()
        with open('xinzi_class_up_num_final.txt', 'a', newline='') as f0:
            for j in range(0, job_c_l):
                job_class_up = job_class_up_list[j]
                job_class = job_class_list[j]
                mess = job_class_up + ',' + job_class + ',' + job_num + ',' + job_salary
                print(mess)
                f0.write(mess + '\n')


def draw_wc(data):
    wc = (
        WordCloud()
            .add(series_name="", data_pair=data, word_size_range=[12, 60])
            .set_global_opts(
            title_opts=opts.TitleOpts(
                title="", title_textstyle_opts=opts.TextStyleOpts(font_size=23)
            ),
            tooltip_opts=opts.TooltipOpts(is_show=True),
        )
            .render("wordcloud_all.html")
    )

def cal_cipin():
    results = get_job()
    words_dict = {}
    for rs in results:
        job_zhuanye_clean_1 = rs['job_zhuanye_clean_1'].strip()
        job_class_up_list = rs['job_class_up'].strip().strip('、').split('、')
        job_cu_l = len(job_class_up_list)
        job_class_list = rs['job_class'].strip().strip('、').split('、')

        job_num = rs['job_num'].strip()
        job_zhuanye_list = job_zhuanye_clean_1.split(' ')

        for l in range(job_cu_l):
            with open ('job_zhuanye_class.txt','a') as f0:
                f0.write(job_class_up_list[l]+','+ job_class_list[l] +','+job_zhuanye_clean_1+','+job_num+'\n')
            for j in job_zhuanye_list:
                words_dict[j] = words_dict.get(j,0)+eval(job_num)
    print(words_dict)
    word_zip =zip(words_dict.keys(),words_dict.values())

    for key,value in words_dict.items():
        with open('cipin.txt','a') as f1:
            f1.write(key+','+str(value)+'\n')
    words_list = [w for w in word_zip]
    return words_list

def cal_class_up_zhishi():
    results = get_job()
    words_dict = {}
    for rs in results:
        job_zhuanye_clean_1 = rs['job_zhuanye_clean_1'].strip()
        job_class_up_list = rs['job_class'].strip().strip('、').split('、')
        job_cu_l = len(job_class_up_list)
        job_num = rs['job_num'].strip()
        job_zhuanye_list = job_zhuanye_clean_1.split(' ')
        for l in range(job_cu_l):
            for j in job_zhuanye_list:
                if len(j)!=0:
                    key_name = job_class_up_list[l].strip()+'-'+j.strip()
                    words_dict[key_name] = words_dict.get(key_name,0)+eval(job_num)
    print(words_dict)
    for key,value in words_dict.items():
        with open('name_cipin.txt','a') as f1:
            f1.write(key+','+str(value)+'\n')

def cal_company():
    # xueli_dict = {}
    results = get_job()
    for rs in results:
        # job_salary = rs['job_salary'].strip()
        com_type_text = rs['com_type_text'].strip()
        com_size_text = rs['com_size_text'].strip()
        com_ind_text = rs['com_ind_text'].strip()
        job_class_up_list = rs['job_class_up'].strip().strip('、').split('、')
        job_cu_l = len(job_class_up_list)
        job_class_list = rs['job_class'].strip().strip('、').split('、')
        job_c_l = len(job_class_list)
        job_num = rs['job_num'].strip()
        with open('com_class_up_num_final.txt', 'a', newline='') as f0:
            for j in range(0, job_c_l):
                job_class_up = job_class_up_list[j]
                job_class = job_class_list[j]
                mess = job_class_up + ',' + job_class + ',' + job_num + ',' + com_type_text+','+com_size_text+','+com_ind_text
                print(mess)
                f0.write(mess + '\n')

if __name__ == '__main__':
    # cal_job_class()
    # cal_xueli()
    # cal_jingyan()
    # cal_xinzi()
    # word = ['底盘工程师/技术员', '电源开发工程师/技术员', '仿真应用工程师/技术员', '飞行器设计与制造', '工程/机械绘图员', '工程设备经理/主管', '工业工程师/技术员', '光伏系统工程师/技术员',
    #         '光源与照明工程', '机电工程师/技术员', '机械工程师/技术员', '家用电器/数码产品研发', '建筑机电工程师/技术员', '结构工程师/技术员', '楼宇自动化', '配置管理工程师/技术员',
    #         '汽车安全性能工程师/技术员', '汽车标定工程师/技术员', '汽车设计工程师/技术员', '汽车试验工程师/技术员', '设备工程师/技术员', '设备经理/主管', '数控操机', '系统工程师/技术员',
    #         '系统架构设计师', '医疗器械研发', '仪器/仪表/计量分析师', '照明设计', '智能驾驶工程师/技术员', '自动控制工程师/技术员']
    # frequent = [9, 212, 87, 30, 256, 174, 120, 15, 120, 333, 983, 10, 65, 582, 24, 31, 26, 10, 18, 9, 1218, 40, 180,
    #             840, 281, 139, 64, 23, 14, 353]
    # words_fre = [i for i in zip(word, frequent)]
    # print(words_fre)
    # draw_wc(words_fre)
    # words_list = cal_cipin()
    # draw_wc(words_list)
    # cal_class_up_zhishi()
    cal_company()

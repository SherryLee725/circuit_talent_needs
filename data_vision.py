from pyecharts import options as opts
from pyecharts.charts import Funnel,Pie,Map,Geo,Bar
import pymysql
from pyecharts.globals import ChartType
from pyecharts.globals import GeoType
from pyecharts.faker import Faker
import pandas as pd
def draw_funnal(data):
    c = (
        Funnel(init_opts=opts.InitOpts(width="800px", height="600px"))
            .add(
            "学历",
            [list(z) for z in data],sort_='none',
            gap=2,
            tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b} : {c}"),
            label_opts=opts.LabelOpts(is_show=True, position="inside",font_size = 14),
            itemstyle_opts=opts.ItemStyleOpts(border_color="#fff", border_width=1),
        )
            .set_global_opts(title_opts=opts.TitleOpts(title="学历结构"))
            .render("show/gangwei_fenji.html")
    )

def draw_pie(data):
    data.sort(key=lambda x: x[1])
    (
        Pie(init_opts=opts.InitOpts(width="1600px", height="800px"))
            .add(
            series_name="企业类型",
            data_pair=data,

            label_opts=opts.LabelOpts(is_show=False, position="inside",font_size = 24),
        )
            .set_global_opts(
            title_opts=opts.TitleOpts(
                title="企业类型",
                pos_left="center",
                pos_top="20",
                title_textstyle_opts=opts.TextStyleOpts(),
            ),
            legend_opts=opts.LegendOpts(is_show=True),
        )
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(
                trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"
            ),
            label_opts=opts.LabelOpts(color="rgba(255, 255, 255, 0.3)"),
        )
            .set_global_opts(title_opts=opts.TitleOpts(title="企业类型"))
            .render("show/gongsi_leixing.html")
    )

def draw_map():
    city1 = ['嘉兴', '杭州', '上海', '金华', '湖州', '苏州', '宣城', '芜湖', '合肥']
    values1 = [84, 2286, 1257, 85, 61, 706, 4, 90, 955]

    # city=['浙江','江苏','上海','安徽']
    # values2 = [107, 38, 63, 21]
    # print(Faker.provinces)
    # print(Faker.values())
    c = (
        Map(init_opts=opts.InitOpts(width="1600px", height="800px"))
            .add("机构数", [list(z) for z in zip(city1, values1)], "china-cities")
            .set_global_opts(
            title_opts=opts.TitleOpts(title="长三角"),
            visualmap_opts=opts.VisualMapOpts(
                min_=4,
                max_=2286,
                range_text=["High", "Low"],
                is_calculable=True,
                range_color=["lightskyblue", "yellow", "orangered"],),
        )
            .render("show/map_guangdong.html")
    )

def data():
    # xueli = ['硕士','大专','本科','中专','高中','不限','中技','初中及以下','博士']
    # xueli = ['博士','硕士','本科','大专','中专','高中','初中及以下']
    # leixing = ['合资', '民营公司', '外资（欧美）', '创业公司', '国企', '上市公司', '外资（非欧美）', '非营利组织', '事业单位', '外企代表处']
    # leixing_sum = [3212,15002,2569,525,1581,2664,3433,4,137,59]
    # xueli_sum = [65,1688,14288,9660,1459,867,169]
    # xueli_sum = [1688,9660,14288,1142,867,1021,317,169,65]

    # xinnengyuan = ['博士', '硕士', '本科', '在校生/应届生', '大专', '高中', '中技', '中专', '初中及以下', '10年以上经验', '8-9年经验', '5-7年经验', '3-4年经验', '2年经验', '1年经验', '无需经验']
    # xinnengyuan_sum = [413, 4561, 54136, 96, 38841, 1249, 775, 3181, 253, 2, 5, 58, 261, 223, 211, 558]

    gangwei = ['专业技术', '普工/职员', '管理', '其他', '研发']
    gangwei_sum = [19279, 10178, 6963, 749, 136]

    data = [list(z) for z in zip(gangwei, gangwei_sum)]
    print(data)
    return data

def sum(sql,engine):
    df = pd.read_sql_query(sql,con=engine)
    df_xueli = df.groupby('job_study').count()
    print(df_xueli)

def draw_bar():
    values1 = [84, 2286, 1257, 85, 61, 706, 4, 90, 955]
    city1 = ['嘉兴', '杭州', '上海', '金华', '湖州', '苏州', '宣城', '芜湖', '合肥']
    c = (
        Bar(init_opts=opts.InitOpts(width="600px", height="600px"))
            .add_xaxis(city1)
            .add_yaxis('', values1)
            .set_series_opts(label_opts=opts.LabelOpts(is_show=True, position='top',font_size=18))
            .set_global_opts(
            title_opts=opts.TitleOpts(title="各城市机构数"),
            visualmap_opts=opts.VisualMapOpts(
                is_show=False,
                min_=4,
                max_=2286,
                range_color=["lightskyblue", "yellow", "orangered"])
        )
            # .reversal_axis()
            .render("show/bar_jigoushu.html")
    )



if __name__ == '__main__':
    # draw_map()
    data = data()
    draw_funnal(data)
    # draw_pie(data)
    # draw_bar()


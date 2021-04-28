#关系可视化
from pyecharts import options as opts
from pyecharts.charts import Graph
from pyecharts.globals import ThemeType




def load_js():
    with open("data_0305.json") as js1:
        data_str = js1.read()
        data_js = eval(data_str)
        # print(type(data_js))
    return data_js

def  set_nodes(data_js):
    # 生成大量节点
    nodes = data_js["nodes"]
    nodes_graph = []
    for i in nodes:
        # node={"name":"节点"+str(i),"symbolsize":20}
        node = opts.GraphNode(name=i['name'],
                              symbol_size=20 + i['value'],  # 节点大小
                              symbol='circle'
                              # ★★★★★ 节点样式可选：'circle', 'rect', 'roundRect', 'triangle', 'diamond', 'pin', 'arrow', 'none', 'image://url'
                              )
        nodes_graph.append(node)
    return nodes_graph

def set_links(data_js):
    # 连接所有节点
    links = data_js["links"]
    links_new = []
    for l in links:
        source_node_name = l["source"]
        target_node_name = l["target"]
        linestyle_opt = opts.LineStyleOpts(is_show=True,
                                            width=5+(l["times"]-1)*5,
                                            opacity=0.6,
                                            curve=0.3,
                                            type_="solid",
                                            color="source"
                                            )
        links_new.append({"source": source_node_name, "target": target_node_name, "lineStyle": linestyle_opt})
    return links_new

def set_initial(nodes,links,html_name):
    # 主要设置
    # InitOpts：初始化配置项（在图形创建开始时即可设置)
    init_opts = opts.InitOpts(width="100%",  # 图宽
                              height="2000px",  # 图高
                              renderer="canvas",  # 渲染模式 svg 或 canvas，即 RenderType.CANVAS 或 RenderType.SVG
                              page_title="Pyecharts Graph关系图",  # 网页标题
                              theme=ThemeType.WHITE,
                              # 主题风格可选：WHITE,LIGHT,DARK,CHALK,ESSOS,INFOGRAPHIC,MACARONS,PURPLE_PASSION,ROMA,ROMANTIC,SHINE,VINTAGE,WALDEN,WESTEROS,WONDERLAND
                              # bg_color="#333333",    #背景颜色
                              js_host=""  # js主服务位置 留空则默认官方远程主服务
                              )
    # ToolboxOpts：工具栏配置（可实现图片保存等功能）
    toolbox_opts = opts.ToolboxOpts(is_show=True,  # 是否显示工具栏
                                    orient="vertical",  # 工具栏工具摆放方向
                                    pos_left="right")  # 工具栏左边位置

    # 通过opts.ItemStyleOpts，设置node节点样式（颜色、大小、透明度)
    itemstyle_opts = opts.ItemStyleOpts(color="black",  # 节点颜色
                                        border_width=0,  # 节点边线宽度
                                        opacity=0.9)  # 节点透明度
    categories={}
    c = (
        Graph(init_opts)
            .add("", nodes,links, layout="circular",repulsion=8000, itemstyle_opts=itemstyle_opts,  is_draggable=True,
                 is_rotate_label = True,
                 categories=categories,
                 linestyle_opts=opts.LineStyleOpts(color="source", curve=0.3),
                 label_opts=opts.LabelOpts(position="right",font_size=35))
            .set_global_opts(title_opts=opts.TitleOpts(title="关系图"), toolbox_opts=toolbox_opts)
            .render(html_name)
    )
    # make_snapshot(snapshot,html_name, "final.jpg")


def main():
    data_js = load_js()
    nodes = set_nodes(data_js)
    links = set_links(data_js)
    html_name = 'test_0305.html'
    set_initial(nodes,links,html_name)

if __name__ == '__main__':
    main()

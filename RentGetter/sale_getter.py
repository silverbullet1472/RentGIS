# @Time    : 2019/12/11 16:39
# @Author  : Jiaqi Ding 
# @Email   : jiaqiding.ricky@foxmail.com


from bs4 import BeautifulSoup
from requester import xun_request
import db_option
import info_parser
from multiprocessing import Pool
import time


def sale_post_extract(post_url):
    # 获取本条post 如有异常允许抛出后pass
    try:
        post_r = xun_request(post_url, allow_exception=True)
        post_value_list = info_parser.sale_parser(post_r)
        print("本条post信息:" + str(post_value_list))
    except Exception as e:
        print("获取本条post时出错:" + str(e))
        return [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]
    else:
        return post_value_list


def sale_post_collect(post_page_url):
    # 获取本页所有post的链接
    post_page_r = xun_request(post_page_url)
    post_url_list_bs = BeautifulSoup(post_page_r.text, 'lxml')
    # 获取到这一页所有发布信息的详情页链接
    post_url_list = [x['href'] for x in post_url_list_bs.select('.ershoufang-list .title a')]
    print("本页所有post url:" + str(post_url_list))
    return post_url_list


def get_sale(city, db_code):
    # 创建该城市数据表
    db_option.create_table_sale(db_code)
    # 进入城市选择页面
    index_r = xun_request('http://www.ganji.com/index.htm')
    index_bs = BeautifulSoup(index_r.text, 'lxml')
    city_href = index_bs.find('a', text=city)['href']
    print("获取到此城市链接:" + city)
    # 进入城市一级区域链接
    city_r = xun_request(city_href + '/ershoufang/')
    city_bs = BeautifulSoup(city_r.text, 'lxml')
    first_list = [[x['href'] + 'pn1/', x.get_text(strip=True)] for x in city_bs.select('.thr-list a')][1:]
    print("获取到此城市一级区域列表:" + str(first_list))
    for first in first_list:
        first_href = first[0]
        first_name = first[1]
        # 进入城市二级区域
        first_r = xun_request(first_href)
        first_bs = BeautifulSoup(first_r.text, "lxml")
        second_list = [[x['href'], x.get_text(strip=True)] for x in first_bs.select('.fou-list a')]
        print("获取到此一级区域的二级区域列表:" + str(second_list))
        # 新建set set内存储此一级区域中去重后url
        post_url_set = set()
        # 将所有二级区域子页面放在一个list中
        post_page_list = []
        for second in second_list:
            second_index_href = second[0]
            second_name = second[1]
            print("进入二级区域:" + second_name)
            second_index_r = xun_request(second_index_href)
            second_index_bs = BeautifulSoup(second_index_r.text, 'lxml')
            # 二级页面首页中获取此区域总页数
            pages_node = second_index_bs.select('.pageBox a span')
            if pages_node:
                total_page = pages_node[-2].get_text(strip=True)
            else:
                total_page = 1
            print("此二级区域总页数:" + str(total_page))
            # 拼接得到此二级区域所有页面url
            post_page_list = post_page_list + [second_index_href + "pn" + str(x) + "/" for x in range(1, int(total_page) + 1)]
        print("获取到此城市一级区域:" + first_name + "之下二级区域中所有页面, 总量:" + str(len(post_page_list)))
        print(post_page_list)
        # 记录并发用时
        t1 = time.time()
        # 新建页面收集池 并发获取页面中所有url
        collect_pool = Pool()
        # 获得到所有url并组合成table(table中url_list中含重复url)
        post_url_table_all = collect_pool .map(func=sale_post_collect, iterable=post_page_list)
        # 对table进行去重
        for post_url_list in post_url_table_all:
            post_url_set.update(post_url_list)
        collect_pool.close()
        collect_pool.join()
        print("获取到此城市一级区域:" + first_name + "下所有url set, 总量:" + str(len(post_url_set)))
        print(post_url_set)
        t2 = time.time()
        print("collect并发用时:"+str((t2 - t1)))

        # 记录并发用时
        t1 = time.time()
        # 对此一级区域所有去重后的url进行收集
        print("开始收集此一级区域信息:" + first_name)
        # 进程池 并行收集
        extract_pool = Pool()
        post_value_table = extract_pool .map(func=sale_post_extract, iterable=post_url_set)
        db_option.insert_table_sale(db_code, post_value_table, city, first_name)
        extract_pool.close()
        extract_pool.join()
        print("此一级区域信息入库:" + first_name)
        t2 = time.time()
        print("extract并发用时:"+str((t2 - t1)))


if __name__ == "__main__":
    get_sale("白山", "baishan")

#                    _ooOoo_
#                   o8888888o
#                   88" . "88
#                   (| -_- |)
#                    O\ = /O
#                ____/`---'\____
#              .   ' \\| |// `.
#               / \\||| : |||// \
#             / _||||| -:- |||||- \
#               | | \\\ - /// | |
#             | \_| ''\---/'' | |
#              \ .-\__ `-` ___/-. /
#           ___`. .' /--.--\ `. . __
#        ."" '< `.___\_<|>_/___.' >'"".
#       | | : `- \`.;`\ _ /`;.`/ - ` : | |
#         \ \ `-. \_ __\ /__ _/ .-` / /
# ======`-.____`-.___\_____/___.-`____.-'======
#                    `=---='
#          佛祖保佑             永无BUG

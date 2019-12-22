# @Time    : 2019/12/11 16:39
# @Author  : Jiaqi Ding 
# @Email   : jiaqiding.ricky@foxmail.com


from bs4 import BeautifulSoup
from requester import ultimate_request
import db_option
import info_parser
from multiprocessing import Pool
import datetime
from tqdm import tqdm
import random
import time


def sale_post_extract(url_list):
    time.sleep(10)
    # 获取本条post 如有异常允许抛出后pass
    try:
        post_r = ultimate_request(url_list[1], allow_exception=True, referer=url_list[0])
        post_value_list = info_parser.sale_parser(post_r, url_list[1])
        print("本条post信息:" + str(post_value_list))
    except Exception as e:
        print("获取本条post时出错:" + str(e) + str(url_list[1]))
        return [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, url_list[1][0:200], None]
    else:
        return post_value_list


def sale_post_collect(post_page_url):
    time.sleep(10)
    # 获取本页所有post的链接 http://bj.ganji.com/zufang/pn2/
    page_num = int(post_page_url[-1])
    if page_num > 1:
        refer_url = post_page_url[:-2] + str(page_num-1)
    else:
        refer_url = None
    post_page_r = ultimate_request(post_page_url, referer=refer_url)
    post_url_list_bs = BeautifulSoup(post_page_r.text, 'lxml')
    # 获取到这一页所有发布信息的详情页链接
    post_url_list = [[post_page_url, x['href']] for x in post_url_list_bs.select('.ershoufang-list .title a')]
    print("本页所有post url:" + str(post_url_list))
    if not post_url_list:
        print("这一页没有post?来看看吧: " + str(post_page_url))
    return post_url_list


def get_sale(city, db_code, start=None, end=None):
    # 进入城市选择页面
    index_r = ultimate_request('http://www.ganji.com/index.htm')
    index_bs = BeautifulSoup(index_r.text, 'lxml')
    city_href = index_bs.find('a', text=city)['href']
    print("获取到此城市链接:" + city)
    # 进入城市一级区域链接
    city_r = ultimate_request(city_href + 'ershoufang/')
    city_bs = BeautifulSoup(city_r.text, 'lxml')
    first_list = [[x['href'] + 'pn1', x.get_text(strip=True)] for x in city_bs.select('.thr-list a')][1:]
    print("获取到此城市" + city +"一级区域列表:" + str(first_list))
    for first in first_list[start:end]:
        time.sleep(8)
        first_href = first[0]
        first_name = first[1]
        # 进入城市二级区域
        first_r = ultimate_request(first_href, referer=city_href + 'ershoufang/')
        first_bs = BeautifulSoup(first_r.text, "lxml")
        second_list = [[x['href'], x.get_text(strip=True)] for x in first_bs.select('.fou-list a')]
        print("获取到此一级区域的二级区域列表:" + str(second_list))
        # 将所有二级区域子页面放在一个list中
        post_page_list = []
        for second in second_list:
            time.sleep(5)
            second_index_href = second[0]
            second_name = second[1]
            print("进入二级区域:" + second_name)
            second_index_r = ultimate_request(second_index_href, referer=first_href)
            second_index_bs = BeautifulSoup(second_index_r.text, 'lxml')
            # 二级页面首页中获取此区域总页数
            pages_node = second_index_bs.select('.pageBox a span')
            if pages_node:
                total_page = pages_node[-2].get_text(strip=True)
            else:
                total_page = 1
            print("此二级区域总页数:" + str(total_page))
            # 拼接得到此二级区域所有页面url
            post_page_list = post_page_list + [second_index_href + "pn" + str(x) for x in range(1, int(total_page) + 1)]
        print("获取到此城市一级区域:" + first_name + "之下二级区域中所有页面, 总量:" + str(len(post_page_list)))
        print(post_page_list)
        # 新建set set内存储此一级区域中去重后url
        # post_url_set = set()
        post_url_table = []
        # 记录并发用时
        print(datetime.datetime.now().strftime('%H:%M:%S'))
        t1 = datetime.datetime.now()
        # 新建页面收集池 并发获取页面中所有url
        with Pool(1) as collect_pool:
            length = len(post_page_list)
            with tqdm(total=length, miniters=1, mininterval=0) as collect_bar:
                for i, post_url_list in tqdm(enumerate(collect_pool.imap_unordered(func=sale_post_collect, iterable=post_page_list))):
                    # post_url_set.update(post_url_list) 在数据库里去重
                    post_url_table.extend(post_url_list)
                    collect_bar.update()
                    print("任务进度:"+str(i)+"/"+str(length))
        print("获取到此城市一级区域:" + first_name + "下所有url set, 总量:" + str(len(post_url_table)))
        print(post_url_table)
        print(datetime.datetime.now().strftime('%H:%M:%S'))
        t2 = datetime.datetime.now()
        print("extract并发用时:"+str((t2 - t1)))
        post_url_set = set()
        for post in post_url_table:
            if post[1] in post_url_set:
                post_url_table.remove(post)
            else:
                post_url_set.add(post[1])
        # 新建value set存储属性值
        post_value_table = []
        # 记录并发用时
        print(datetime.datetime.now().strftime('%H:%M:%S'))
        t1 = datetime.datetime.now()
        # 对此一级区域所有去重后的url进行收集
        print("开始收集此一级区域信息:" + first_name)
        # 进程池 并行收集
        with Pool(1) as extract_pool:
            length = len(post_url_table)# 更改为不去重的
            with tqdm(total=length, miniters=1, mininterval=0) as extract_bar:
                for i, post_value_list in tqdm(enumerate(extract_pool.imap_unordered(func=sale_post_extract, iterable=post_url_table))):
                    post_value_table.append(post_value_list)
                    extract_bar.update()
                    print("任务进度:" + str(i) + "/" + str(length))
        db_option.insert_table_sale(db_code, post_value_table, city, first_name)
        print("此一级区域信息入库:" + first_name)
        print(datetime.datetime.now().strftime('%H:%M:%S'))
        t2 = datetime.datetime.now()
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

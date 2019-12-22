from bs4 import BeautifulSoup
from requester import ultimate_request
import db_option
import info_parser
from multiprocessing import Pool
import datetime
from tqdm import tqdm
import time
import psycopg2


def sale_post_extract(url):
    time.sleep(5)
    # 获取本条post 如有异常允许抛出后pass
    try:
        post_r = ultimate_request(url, allow_exception=True)
        post_value_list = info_parser.sale_parser(post_r, url)
        print("本条post信息:" + str(post_value_list))
    except Exception as e:
        print("获取本条post时出错:" + str(e) + str(url))
        return [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, url[0:200], None]
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


def get_sale(city, db_code):
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    # 小区属性列表 ["城市", "一级区域", "小区名称", "小区均价", "价格走势", "区域商圈", "详细地址", "建筑类型", "物业费用", "产权类别", "容积率", "总户数", "绿化率", "建筑年代", "停车位", "开发商", "物业公司", "在租数", "在售数"]
    cur.execute(f"select distinct yijiquyu from {db_code}_sale where biaoti IS NULL")
    rows = cur.fetchall()
    conn.commit()
    conn.close()
    for row in rows:
        first_name = row[0]
        print("进入一级区域"+str(first_name))
        post_url_table = []
        conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
        cur = conn.cursor()
        # 小区属性列表 ["城市", "一级区域", "小区名称", "小区均价", "价格走势", "区域商圈", "详细地址", "建筑类型", "物业费用", "产权类别", "容积率", "总户数", "绿化率", "建筑年代", "停车位", "开发商", "物业公司", "在租数", "在售数"]
        cur.execute(f"select post_url from {db_code}_sale where yijiquyu = '{first_name}' AND biaoti IS NULL")
        rows = cur.fetchall()
        conn.commit()
        conn.close()
        for row in rows:
            if row:
                post_url_table.append(row[0])
        print(post_url_table)
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


if __name__ == '__main__':
    get_sale("宜昌", "yichang")
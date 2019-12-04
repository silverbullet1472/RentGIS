import psycopg2
import requests
import os
from bs4 import BeautifulSoup
import time
import re

community_key = ["小区名称", "帖数", "小区房价", "小区房价增长率", "区域商圈", "详细地址", "建筑类型", "物业费用", "产权类别", "容积率", "总户数", "绿化率", "建筑年代",
                  "停车位", "开发商", "物业公司", "在租房源", "在售房源"]
house_key = ["标题描述", "房租", "整租合租", "面积", "户型", "朝向", "装修情况", "楼层", "所在地址", "个人/经纪人", "房屋描述"]


def get_proxy():
    # 创建数据库连接
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    # 找到城市首页及城市子区域对应链接
    cur.execute("select * from proxy order by random() limit 1")
    rand_proxy = cur.fetchone()
    proxy = {rand_proxy[3]: rand_proxy[3] + '://' + rand_proxy[1] + ':' + rand_proxy[2]}
    # 提交事务，关闭数据库连接
    conn.commit()
    conn.close()
    return proxy


def request(url):
    headers = {
        'user-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36',
        'Referer': 'https://www.baidu.com'
    }
    url = str(url)
    if url.startswith('//'):
        url = url.replace('//', 'http://', 1)
    while True:
        try:
            r = requests.get(url, timeout=3, headers=headers, proxies=get_proxy())
            while (r.text.strip() == '') or ('Forbidden'in r.text):
                print('请求结果不含所需信息')
                time.sleep(0.3)
                r = requests.get(url, timeout=3, headers=headers, proxies=get_proxy())
            while '访问过于频繁' in r.text :
                print(url)
                print('程序暂停，请访问链接进行人机验证！')
                os.system("pause")
                r = requests.get(url, timeout=3, headers=headers, proxies=get_proxy())
            return r
        except TimeoutError as e:
            print(url)
            print(str(e))
            print('访问超时')
        except Exception as e:
            print(url)
            print(str(e))
            print('获取页面内容时出错了')


def get_community(city):
    # 进入城市首页链接
    index_r = request('http://www.ganji.com/index.htm')
    index_bs = BeautifulSoup(index_r.text, 'lxml')
    city_href = index_bs.find('a', text=city)['href']
    print(city_href)
    # 进入城市一级区域链接
    city_r = request(city_href + '/zufang/')
    city_bs = BeautifulSoup(city_r.text, 'lxml')
    first_list = [[x['href'] + 'pn1', x.get_text(strip=True)] for x in city_bs.select('.thr-list a')][1:]
    print(first_list)
    for first in first_list:
        first_href = first[0]
        # 进入城市二级区域
        first_r = request(first_href)
        first_bs = BeautifulSoup(first_r.text, "lxml")
        second_list = [[x['href'] + 'pn1', x.get_text(strip=True)] for x in first_bs.select('.fou-list a')]
        print(second_list)
        for second in second_list:
            next_href = second[0]
            page = 0
            # 进入一个二级区域，开始爬取直到页面最后
            while True:
                page += 1
                print(f'开始第{page}页爬取')
                post_list_r = request(next_href)
                post_list_bs = BeautifulSoup(post_list_r.text, 'lxml')
                # 获取到这一页所有发布信息的详情页链接
                post_list = [[x['href'], x.get_text(strip=True)] for x in
                             post_list_bs.select('.ershoufang-list .title a')]
                print(post_list)
                # 进入每一条信息详情页
                for post in post_list:
                    house_r = request(post[0])
                    house_bs = BeautifulSoup(house_r.text, 'lxml')
                    # 标题及房价
                    house_title = re.sub(r'\s+', ' ', house_bs.select_one('.card-top .card-title').text).strip()
                    house_price = re.sub(r'\s+', ' ', house_bs.select_one('span.price').text)
                    # 整租 面积 朝向 楼层 装修
                    house_value_list1 = [re.sub(r'\s+', ' ', x.get_text(strip=True)) for x in house_bs.select('li.item.f-fl .content')]
                    house_value_list = [house_title, house_price] + [house_value_list1[0]] + re.split(r'\s+', house_value_list1[1]) + house_value_list1[2:]
                    print(house_value_list)
                next = post_list_bs.find('a', text='下一页')
                if next is None:
                    break
                next_href = next['href']


if __name__ == "__main__":
    get_community("焦作")

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

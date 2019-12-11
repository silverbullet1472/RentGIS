# @Time    : 2019/12/9 11:04
# @Author  : Jiaqi Ding
# @Email   : jiaqiding.ricky@foxmail.com

import psycopg2
import requests
import os
from bs4 import BeautifulSoup
import time
import re

# community_key = ["小区名称", "小区均价", "小区房价增长率", "区域商圈", "详细地址", "建筑类型", "物业费用", "产权类别", "容积率", "总户数", "绿化率", "建筑年代", "停车位", "开发商", "物业公司", "在租房源", "在售房源", "帖数"]
# house_key = ["标题 总价 单价 户型 面积 朝向 楼层 建筑年代 产权 装修 小区名称  详细地址 个人经纪人 房屋描述"]


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
        'user-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36'
    }
    url = str(url)
    if url.startswith('//'):
        url = url.replace('//', 'http://', 1)
    while True:
        try:
            r = requests.get(url, timeout=3, headers=headers, proxies=get_proxy())
            while r.text.strip() == '':
                print('请求结果为空')
                time.sleep(0.3)
                r = requests.get(url, timeout=3, headers=headers, proxies=get_proxy())
            while 'Forbidden'in r.text:
                print('403 Forbidden')
                time.sleep(0.3)
                r = requests.get(url, timeout=3, headers=headers, proxies=get_proxy())
            while '访问过于频繁' in r.text:
                print(url)
                print('访问过于频繁，请访问链接进行人机验证！')
                os.system("pause")
                r = requests.get(url, timeout=3, headers=headers, proxies=get_proxy())
            return r
        except Exception as e:
            print(url)
            print(str(e))
            print('获取页面内容时出错了')


def get_sale(city, db_code):
    # 创建数据库连接
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"delete from {db_code}_sale_house")
    cur.execute(f"delete from {db_code}_sale_community")
    # 进入城市选择页面
    index_r = request('http://www.ganji.com/index.htm')
    index_bs = BeautifulSoup(index_r.text, 'lxml')
    city_href = index_bs.find('a', text=city)['href']
    print(city_href)
    # 进入城市一级区域链接
    city_r = request(city_href + '/ershoufang/')
    city_bs = BeautifulSoup(city_r.text, 'lxml')
    first_list = [[x['href'] + 'pn1', x.get_text(strip=True)] for x in city_bs.select('.thr-list a')][1:]
    print(first_list)
    for first in first_list:
        first_href = first[0]
        first_name = first[1]
        # 进入城市二级区域
        first_r = request(first_href)
        first_bs = BeautifulSoup(first_r.text, "lxml")
        second_list = [[x['href'] + 'pn1', x.get_text(strip=True)] for x in first_bs.select('.fou-list a')]
        print(second_list)
        for second in second_list:
            next_href = second[0]
            second_name = second[1]
            page = 0
            # 进入一个二级区域，开始爬取直到页面最后
            community_name_list = []
            while True:
                page += 1
                print(f'开始第{page}页爬取')
                post_list_r = request(next_href)
                post_list_bs = BeautifulSoup(post_list_r.text, 'lxml')
                # 获取到这一页所有发布信息的详情页链接
                post_list = [[x['href'], x.get_text(strip=True)] for x in
                             post_list_bs.select('.ershoufang-list .title a')]
                print(post_list)
                house_value_table = []
                community_value_table = []
                for post in post_list:
                    house_r = request(post[0])
                    house_bs = BeautifulSoup(house_r.text, 'lxml')
                    # 标题 总价 单价
                    house_title = re.sub(r'\s+', ' ', house_bs.select_one('.card-top .card-title').get_text(strip=True))
                    house_total = re.sub(r'\s+', ' ', house_bs.select_one('span.price').get_text(strip=True))
                    house_unit = re.sub(r'[|\s+]', '', house_bs.select_one('span.unit').get_text(strip=True))
                    # 户型 面积 朝向 楼层 建筑年代 产权 装修
                    house_value_list1 = [re.sub(r'\s+', ' ', x.get_text(strip=True)) for x in house_bs.select('li.item.f-fl .content')]
                    house_value_list = [city, first_name, second_name] + [house_title, house_total, house_unit] + house_value_list1
                    # 小区名称 链接
                    community_link_node = house_bs.select_one('.er-item .content a')
                    if community_link_node:
                        community_link = community_link_node['href']
                        # 小区名称
                        community_name = community_link_node.get_text(strip=True)
                        # 帖数 详细地址
                        house_value_list2 = house_bs.select('.er-item .content')
                        try:
                            post_num = re.findall(r'\d+', house_value_list2[0].get_text(strip=True))[0]
                        except Exception as e:
                            post_num = 'unknown'
                            print("正则提取时出错:" + str(e))
                        house_address = re.sub(r'\s+', '', house_value_list2[1].text)
                        if community_name not in community_name_list:
                            community_name_list.append(community_name)
                            # 进入小区详情页面
                            community_r = request(community_link)
                            community_bs = BeautifulSoup(community_r.text, 'lxml')
                            # 小区均价 价格走势
                            community_price = community_bs.select_one('span.price').contents[0]
                            if community_bs.select_one('div .contrast'):
                                try:
                                    community_contrast = re.findall(r'(\d+(\.\d+)?%)', community_bs.select_one('div .contrast').contents[0])[0][0]
                                except Exception as e:
                                    community_contrast = 'unknown'
                                    print("正则提取时出错:" + str(e))
                                if 'down' in community_bs.select_one('.contrast')['class']:
                                    community_contrast = '-' + community_contrast
                            else:
                                community_contrast = 'unknown'
                            # 在租 在售信息
                            community_value_list1 = [re.sub(r'\s+', ' ', attr.get_text(strip=True)) for attr in community_bs.select('li.item.f-fl .content')]
                            try:
                                community_rent = re.findall(r'\d+', community_bs.select('.xq-card-resource span a')[0].get_text(strip=True))[0]
                            except Exception as e:
                                community_rent = 'unknown'
                                print("正则提取时出错:" + str(e))
                            try:
                                community_sale = re.findall(r'\d+', community_bs.select('.xq-card-resource span a')[1].get_text(strip=True))[0]
                            except Exception as e:
                                community_sale = 'unknown'
                                print("正则提取时出错:" + str(e))
                            # 小区属性列表 ["小区名称", "小区均价", "价格走势", "区域商圈", "详细地址", "建筑类型", "物业费用", "产权类别", "容积率", "总户数", "绿化率", "建筑年代", "停车位", "开发商", "物业公司", "在租数", "在售数", "帖数"]
                            community_value_list = [city, first_name, second_name] + [community_name, community_price, community_contrast] + community_value_list1 + [community_rent] + [community_sale] + [post_num]
                            print(community_value_list)
                            community_value_table.append(community_value_list)
                    else:
                        house_value_list3 = house_bs.select('.er-item .content')
                        # 小区名称 详细地址
                        community_name = house_value_list3[0].get_text(strip=True)
                        house_address = re.sub(r'\s+', '', house_value_list3[1].text)
                    # 经纪人/个人 房屋描述
                    agent = "company" if house_bs.select_one('.user-info-top .license_box') else "individual"
                    house_comment = re.sub(r'\s+', ' ', house_bs.select_one('.describe .item').get_text(strip=True))
                    # 房屋属性列表 ["标题", "总价", "单价", "户型", "面积", "朝向", "楼层", "建筑年代", "产权", "装修", "小区名称", "详细地址", "个人/经纪人", "房屋描述"]
                    house_value_list = house_value_list + [community_name, house_address, agent, house_comment]
                    print(house_value_list)
                    house_value_table.append(house_value_list)
                print(house_value_table)
                print(community_value_table)
                cur.executemany(f"insert into {db_code}_sale_house values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", house_value_table)
                cur.executemany(f"insert into {db_code}_sale_community values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", community_value_table)
                next = post_list_bs.find('a', text='下一页')
                if next is None:
                    break
                next_href = next['href']
    # 关闭连接
    conn.close()


if __name__ == "__main__":
    get_sale("焦作", "jiaozuo")

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

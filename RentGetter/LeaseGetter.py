import psycopg2
import requests
import os
from bs4 import BeautifulSoup
import time
import re

community_key = ["小区名称", "小区均价", "小区房价增长率", "区域商圈", "详细地址", "建筑类型", "物业费用", "产权类别", "容积率", "总户数", "绿化率", "建筑年代", "停车位", "开发商", "物业公司", "在租房源", "在售房源"]
house_key = ["标题", "房租", "户型", "整租合租", "面积", "朝向", "楼层", "装修", "小区名称", "帖数", "详细地址", "个人/经纪人", "房屋描述"]


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
        except TimeoutError as e:
            print(url)
            print(str(e))
            print('访问超时')
        except Exception as e:
            print(url)
            print(str(e))
            print('获取页面内容时出错了')


def get_community(city):
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
                    # 户型 整租 面积 朝向 楼层 装修
                    house_value_list1 = [re.sub(r'\s+', ' ', x.get_text(strip=True)) for x in house_bs.select('li.item.f-fl .content')]
                    house_value_list = [house_title, house_price] + [house_value_list1[0]] + re.split(r'\s+', house_value_list1[1]) + house_value_list1[2:]
                    # 小区名称 链接
                    community_link_node = house_bs.select_one('.er-item .content a')
                    if community_link_node:
                        community_link = community_link_node['href']
                        # 进入小区详情页面
                        community_r = request(community_link)
                        community_bs = BeautifulSoup(community_r.text, 'lxml')
                        # 小区名称 小区均价 价格走势
                        community_title = community_bs.select_one('.card-top .card-title')['title']
                        community_price = community_bs.select_one('span.price').contents[0]
                        community_contrast = community_bs.select_one('div .contrast').contents[0]
                        # 在租 在售信息
                        community_value_list1 = [re.sub(r'\s+', ' ', attr.get_text(strip=True)) for attr in community_bs.select('li.item.f-fl .content')]
                        community_rent = re.findall(r'\d+', community_bs.select('.xq-card-resource span a')[0].get_text(strip=True))[0]
                        community_sale = re.findall(r'\d+', community_bs.select('.xq-card-resource span a')[1].get_text(strip=True))[0]
                        # 小区属性列表 ["小区名称", "小区均价", "价格走势", "区域商圈", "详细地址", "建筑类型", "物业费用", "产权类别", "容积率", "总户数", "绿化率", "建筑年代", "停车位", "开发商", "物业公司", "在租数", "在售数"]
                        community_value_list = [community_title, community_price, community_contrast] + community_value_list1 + [community_rent] + [community_sale]
                        print(community_value_list)
                        # 小区名称
                        community_name = community_link_node.get_text(strip=True)
                        # 帖数 详细地址
                        house_value_list2 = house_bs.select('.er-item .content')
                        post_num = re.findall(r'\d+', house_value_list2[0].get_text(strip=True))[0]
                        house_address = re.sub(r'\s+', '', house_value_list2[2].text)
                    else:
                        house_value_list3 = house_bs.select('.er-item .content')
                        # 帖数 小区名称 详细地址
                        post_num = 'unknown'
                        community_name = house_value_list3[0].get_text(strip=True)
                        house_address = re.sub(r'\s+', '', house_value_list3[2].text)
                    # 经纪人/个人 房屋描述
                    agent = "company" if house_bs.select_one('.user-info-top .license_box') else "individual"
                    house_comment = re.sub(r'\s+', ' ', house_bs.select_one('.describe .item').text)
                    # 房屋属性列表 ["标题", "房租", "户型", "整租合租", "面积", "朝向","楼层", "装修", "小区名称","帖数","详细地址", "个人/经纪人", "房屋描述"]
                    house_value_list.extend([community_name, post_num, house_address, agent,house_comment])
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

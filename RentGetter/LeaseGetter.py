import psycopg2
import requests
import os
from bs4 import BeautifulSoup


community_attr_key_list = ['小区名称', '小区房价', '小区房价增长率', '区域商圈', '详细地址：', '建筑类型', '物业费用',
                           '产权类别', '容积率', '总户数', '绿化率', '建筑年代', '停车位', '开发商', '物业公司',
                           '在租房源数', '在售房源数']

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
    while True:
        try:
            r = requests.get(url,timeout=3, headers=headers, proxies=get_proxy())
            while r.text.strip() == '':
                print('请求内容为空，重新请求')
                r = requests.get(url, timeout=3, headers=headers, proxies=get_proxy())
            while '进行验证码校验' in r.text:
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

    # 找到城市首页链接
    index_r = request('http://www.ganji.com/index.htm')
    index_bs = BeautifulSoup(index_r.text, 'lxml')
    city_href = index_bs.find('a', text=city)['href']
    print(city_href)
    # 找到城市子区域链接
    city_lease_r = request(city_href + '/zufang/')
    city_lease_bs = BeautifulSoup(city_lease_r.text, 'lxml')
    subarea_list = [[x['href'] + 'pn1', x.get_text(strip=True)] for x in city_lease_bs.select('.thr-list a')][1:]
    print(subarea_list)


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

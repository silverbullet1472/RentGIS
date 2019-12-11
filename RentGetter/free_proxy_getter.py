# @Time    : 2019/12/9 11:04
# @Author  : Jiaqi Ding
# @Email   : jiaqiding.ricky@foxmail.com

import requests
from bs4 import BeautifulSoup
import time
import psycopg2


def get_ip():
    # 通过connect方法创建数据库连接
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    # 创建cursor以访问数据库
    cur = conn.cursor()
    cur.execute("delete from proxy")
    # 提交事务
    conn.commit()
    # 关闭连接
    conn.close()
    for i in range(1, 6):
        # 通过connect方法创建数据库连接
        conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
        # 创建cursor以访问数据库
        cur = conn.cursor()
        url = 'https://www.xicidaili.com/nn/' + str(i)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Mobile Safari/537.36'
        }
        try:
            ip_res = requests.get(url, timeout=3, headers=headers)
        except Exception as e:
            print("get_ip:UrlRequest:" + str(e))
            print("get_ip:UrlRequest:ip_restart Get Ip!")
            time.sleep(0.5)
            get_ip()
        else:
            soup = BeautifulSoup(ip_res.text, 'lxml')
            ips = soup.select('#ip_list tr')
            for i in ips:
                # 解析代理信息
                try:
                    ipp = i.select('td')
                    ip = ipp[1].text
                    port = ipp[2].text
                    type = ipp[5].text
                    print("get_ip:succeeded:" + ip)
                except Exception as e:
                    print("get_ip:InfoExtraction:" + str(e))
                # 测试代理
                else:
                    try:
                        test_url = 'http://www.ganji.com/index.htm'
                        proxy = {type: type + '://' + ip + ':' + port}
                        test_res = requests.get(test_url, proxies=proxy, timeout=3)
                    except Exception as e:
                        print("check_ip:failed:" + str(e))
                    else:
                        if test_res.status_code == 200:
                            cur.execute("insert into proxy (proxy_ip,proxy_port,proxy_type) values(%s,%s,%s)",
                                        (ip, port, type))
                            print("check_ip:succeeded & inserted:" + ip)
                        else:
                            print("check_ip:failed:can not connect to Ganji")
            # 提交事务
            conn.commit()
            # 关闭连接
            conn.close()


if __name__ == "__main__":
    get_ip()

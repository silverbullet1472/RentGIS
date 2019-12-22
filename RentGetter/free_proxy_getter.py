# @Time    : 2019/12/9 11:04
# @Author  : Jiaqi Ding
# @Email   : jiaqiding.ricky@foxmail.com

import requests
from bs4 import BeautifulSoup
import time
import psycopg2
from requester import ultimate_request


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
        conn.autocommit = True
        # 创建cursor以访问数据库
        cur = conn.cursor()
        url = 'https://www.xicidaili.com/nn/' + str(i)
        try:
            ip_res = ultimate_request(url)
            print(ip_res)
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
                        test_url = 'http://bj.ganji.com/beitaipingzhuang/zufang/pn4/'
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
            # 关闭连接
            conn.close()


if __name__ == "__main__":
    # db_option.create_table_proxy()
    get_ip()

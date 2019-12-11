# @Time    : 2019/12/9 11:04
# @Author  : Jiaqi Ding
# @Email   : jiaqiding.ricky@foxmail.com

import requests
import psycopg2
import os
import time
import hashlib
import urllib3


def xun_request(url, allow_exception=False):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    orderno = "ZF20191295030VMIW4l"
    secret = "fc281e28941e4969b5d8f98e2cbd148a"
    ip = "forward.xdaili.cn"
    port = "80"
    ip_port = ip + ":" + port
    timestamp = str(int(time.time()))
    string = "orderno=" + orderno + "," + "secret=" + secret + "," + "timestamp=" + timestamp
    string = string.encode()
    md5_string = hashlib.md5(string).hexdigest()
    sign = md5_string.upper()
    auth = "sign=" + sign + "&" + "orderno=" + orderno + "&" + "timestamp=" + timestamp
    proxy = {"http": "http://" + ip_port, "https": "https://" + ip_port}
    headers = {"Proxy-Authorization": auth,
               'User-Agent': "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36"}
    url = str(url)
    if url.startswith('//'):
        url = url.replace('//', 'http://', 1)
    # 对于允许抛出异常请求 timeout值设小
    if allow_exception:
        timeout = 10
    else:
        timeout = 20
    extract_num = 0
    request_num = 0
    while True:
        try:
            r = requests.get(url, headers=headers, proxies=proxy, verify=False, allow_redirects=False, timeout=timeout)
        except Exception as e:
            request_num = request_num + 1
            time.sleep(0.1)
            print(str(e))
            print('requests出错')
            if request_num > 50:
                print('request异常过多')
                os.system("pause")
        else:
            if r.status_code != 200 or r.text.strip() == '' or '访问过于频繁' in r.text or "http://tieba.baidu.com/" in r.text:
                extract_num = extract_num + 1
                time.sleep(0.1)
                # 不处理重定向问题
                # if r.status_code == 302 or r.status_code == 301:
                #     if "antibot" not in r.headers['Location']:
                #         url = r.headers['Location']
                #         url = str(url)
                #         if url.startswith('//'):
                #             url = url.replace('//', 'http://', 1)
                #         print("重定向至" + url)
                #     else:
                #         print("我透，你想把我重定向到验证页面")
                if r.status_code == 404 and allow_exception:
                    raise Exception("404错误，页面不存在")
                if extract_num > 50:
                    print('extract异常过多')
                    os.system("pause")
            else:
                return r


def qingting_request(url, raise_exception=True):
    proxy_host = 'dyn.horocn.com'
    proxy_port = 50000
    proxy_username = 'MERR1652174473064309'
    proxy_pwd = "yqIf4H2Eyx7H"
    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        "host": proxy_host,
        "port": proxy_port,
        "user": proxy_username,
        "pass": proxy_pwd,
    }
    proxies = {
        'http': proxyMeta,
        'https': proxyMeta,
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36'
    }
    url = str(url)
    if url.startswith('//'):
        url = url.replace('//', 'http://', 1)
    extract_num = 0
    request_num = 0
    while True:
        try:
            r = requests.get(url, timeout=20, headers=headers, proxies=proxies)
        except Exception as e:
            request_num = request_num + 1
            time.sleep(0.1)
            print(url)
            print(str(e))
            print('requests出错')
            if request_num > 50:
                print('request异常过多')
                os.system("pause")
        else:
            if r.status_code != 200 or r.text.strip() == '' or '访问过于频繁' in r.text:
                extract_num = extract_num + 1
                time.sleep(0.1)
                if r.status_code == 404 and raise_exception:
                    raise Exception("404错误，页面不存在")
                if extract_num > 20:
                    print('extract异常过多')
                    os.system("pause")
            else:
                return r


def get_xici_proxy(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36'
    }
    url = str(url)
    if url.startswith('//'):
        url = url.replace('//', 'http://', 1)
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
    r = requests.get(url, timeout=20, headers=headers, proxies=proxy)
    return r


if __name__ == "__main__":
    headers1 = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36'
    }
    headers2 = {
        'User-Agent': "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36"
    }
    headers3 = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"
    }

    # r = xun_request("http://www.ganji.com/index.htm", raise_exception=False)
    # print(r.text)

    r = requests.get("http://www.ganji.com/index.htm", headers= headers2)
    print(r.text)
    print("http://tieba.baidu.com/" in r.text)
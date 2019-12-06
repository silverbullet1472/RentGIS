import requests
import time
import os


def request(url):
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
        'user-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36'
    }
    url = str(url)
    if url.startswith('//'):
        url = url.replace('//', 'http://', 1)
    while True:
        try:
            r = requests.get(url, timeout=20, headers=headers, proxies=proxies)
            while r.status_code != 200:
                print('状态码不为200')
                time.sleep(0.1)
                r = requests.get(url, timeout=20, headers=headers, proxies=proxies)
            while r.text.strip() == '':
                print('请求结果为空')
                time.sleep(0.1)
                r = requests.get(url, timeout=20, headers=headers, proxies=proxies)
            while '访问过于频繁' in r.text:
                print('访问过于频繁，请访问链接进行人机验证！')
                time.sleep(0.1)
                r = requests.get(url, timeout=20, headers=headers, proxies=proxies)
            return r
        except requests.exceptions.ProxyError as e:
            print("没连WIFI？来看看吧")
            os.system("pause")
        except requests.exceptions.ConnectTimeout as e:
            print("没登录WIFI？来看看吧")
            os.system("pause")
        except Exception as e:
            print(url)
            print(type(e))
            print(str(e))
            print('获取页面内容时出错了')


if __name__ == "__main__":
    while True:
        request("http://www.baidu.com")

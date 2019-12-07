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
    extract_num = 0
    request_num = 0
    while True:
        try:
            r = requests.get(url, timeout=20, headers=headers, proxies=proxies)
        except Exception as e:
            request_num = request_num + 1
            time.sleep(0.2)
            print(url)
            print(str(e))
            print('Requests出错')
            if request_num > 50:
                print('Request异常太多次！')
                os.system("pause")
        else:
            if r.status_code != 200 or r.text.strip() == '' or '访问过于频繁' in r.text:
                extract_num = extract_num + 1
                time.sleep(0.2)
                if extract_num > 20:
                    raise Exception("页面不正常！")
            else:
                return r




if __name__ == "__main__":
    while True:
        request("http://www.baidu.com")





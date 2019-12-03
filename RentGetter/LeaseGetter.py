import psycopg2

def get_community():
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,ja;q=0.6',
        'Cache-Control': 'max-age=0',
        'Host': 'bj.ganji.com',
        'Proxy-Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'
    }
    community_attr_key_list = ['小区名称', '房价', '区域商圈：', '详细地址：', '建筑类型：', '物业费用：',
                               '产权类别：', '容积率：', '总户数：', '绿化率：', '建筑年代：', '停车位：', '开发商：', '物业公司：',
                               '经度', '纬度']
    # 通过connect方法创建数据库连接
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    # 创建cursor以访问数据库
    cur = conn.cursor()
    cur.execute("select from proxy order by random() limit 1")
    proxy = cur.fetchone()
    # 提交事务
    conn.commit()
    # 关闭连接
    conn.close()

if __name__ == "__main__":
    print(1)
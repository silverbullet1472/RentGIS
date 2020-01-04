import psycopg2
import db_option
import requests
from tqdm import tqdm
from geo_coder import *
import json
import time
import os
from math import ceil

def community_add_location():
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    # 1 先去重
    cur.execute("""
                   create table yichang_community_processed
                   (
                       chengshi        varchar(50),
                       yijiquyu        varchar(50),
                       xiaoqumingcheng varchar(50),
                       xiaoqujunjia    varchar(50),
                       jiagezoushi     varchar(50),
                       quyushangquan   varchar(50),
                       xiangxidizhi    varchar(50),
                       jianzhuleixing  varchar(50),
                       wuyefeiyong     varchar(50),
                       chanquanleibie  varchar(50),
                       rongjilv        varchar(50),
                       zonghushu       varchar(50),
                       lvhualv         varchar(50),
                       jianzhuniandai  varchar(50),
                       tingchewei      varchar(50),
                       kaifashang      varchar(50),
                       wuyegongsi      varchar(50),
                       zaizushu        varchar(50),
                       zaishoushu      varchar(50),
                       community_url   varchar(200)
                   );
                   """)
    conn.commit()
    cur.execute("""
                   INSERT INTO yichang_community_processed SELECT DISTINCT ON (xiaoqumingcheng) * FROM yichang_community
                   """)
    conn.commit()

    # 2 地理编码得到百度坐标
    cur.execute("""
                     SELECT * FROM yichang_community_processed;
                     """)
    conn.commit()
    rows = cur.fetchall()
    # 遍历所有记录 调用地理编码
    value_table = []
    for row in tqdm(rows):
        entry = list(row)
        city = entry[0]
        community = entry[2]
        road = entry[6]
        address = road + " " + community
        location = get_location(address, city)
        entry.append(str(location[0])[:20])  # 因为数据库字段是字符 不转会有问题
        entry.append(str(location[1])[:20])
        value_table.append(entry)
    print(value_table)
    # 插入新表
    cur.execute("""
                   ALTER TABLE yichang_community_processed ADD COLUMN bd_lon varchar(20), ADD COLUMN bd_lat varchar(20);
                   """)
    conn.commit()
    cur.execute("""
                   DELETE FROM yichang_community_processed
                   """)
    conn.commit()
    cur.executemany("""
                   insert into yichang_community_processed values (%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s)
                   """
                    , value_table)
    conn.commit()

    # 3 坐标转换得到WGS84坐标
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    cur.execute("""
                         SELECT * FROM yichang_community_processed;
                         """)
    conn.commit()
    rows = cur.fetchall()
    # 遍历所有记录 调用地理编码
    value_table = []
    for row in tqdm(rows):
        row = list(row)
        lon = row[20]
        lat = row[21]
        location = bd09_to_wgs84(float(lon), float(lat))
        row.append(str(location[0])[0:20])  # 因为数据库字段是字符 转一下
        row.append(str(location[1])[0:20])
        # print(row)
        value_table.append(row)
    print(value_table)
    # 插入新表
    cur.execute("""
                       ALTER TABLE yichang_community_processed ADD COLUMN wgs_lon varchar(20), ADD COLUMN wgs_lat varchar(20);
                       """)
    conn.commit()
    cur.execute("""
                       DELETE FROM yichang_community_processed
                       """)
    conn.commit()
    cur.executemany("""
                       insert into yichang_community_processed values (%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s)
                       """
                    , value_table)
    conn.commit()
    conn.close()

def community_add_factor_cols():
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    cur.execute("""
                          ALTER TABLE yichang_community_processed 
                          ADD COLUMN min_time_gongjiao         varchar(20), 
                          ADD COLUMN min_time_zhongxue         varchar(20),
                          ADD COLUMN count_zhongxue            varchar(20),
                          ADD COLUMN min_time_xiaoxue          varchar(20),
                          ADD COLUMN count_xiaoxue             varchar(20),
                          ADD COLUMN min_time_gaoxiao          varchar(20),
                          ADD COLUMN count_gaoxiao             varchar(20),
                          ADD COLUMN min_time_yiyuan           varchar(20),
                          ADD COLUMN count_yiyuan              varchar(20),
                          ADD COLUMN min_time_jingdian         varchar(20),
                          ADD COLUMN count_jingdian            varchar(20),
                          ADD COLUMN min_time_kaifang          varchar(20),
                          ADD COLUMN count_kaifang             varchar(20),
                          ADD COLUMN min_time_xiuxian          varchar(20),
                          ADD COLUMN count_xiuxian             varchar(20);
                          """)
    conn.commit()
    conn.close()



def community_add_min_time(where_clause, col):
    # 读小区表
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    cur.execute("""
                      SELECT * FROM yichang_community_processed;
                      """)
    conn.commit()
    rows = cur.fetchall()
    community_old_table = []
    for row in tqdm(rows):
        row = list(row)
        community_old_table.append(row)
    print(community_old_table)

    # 读POI表
    cur.execute(f"""
                       SELECT * FROM poi_processed WHERE {where_clause};
                       """)
    conn.commit()
    rows = cur.fetchall()
    poi_table = []
    for row in tqdm(rows):
        row = list(row)
        poi_table.append(row)
    print(poi_table)

    # 建立新表 处理后重写
    time.sleep(1)
    community_new_table = []
    for community in tqdm(community_old_table, miniters=5):
        community_bd_lon = community[20]
        community_bd_lat = community[21]
        community_wgs_lon = community[22] #原表24列 输入坐标系参数为百度坐标系 因此取bd经纬度
        community_wgs_lat = community[23]
        # 利用distance list取得最小值索引
        destination_list = []
        distance_list = []
        # 计算POI到点距离 小于1200加入destination list
        for poi in poi_table:
            poi_wgs_lon = poi[-2]
            poi_wgs_lat = poi[-1]
            dis = distance(float(community_wgs_lon), float(community_wgs_lat), float(poi_wgs_lon), float(poi_wgs_lat)) # 大圆距离计算用WGS
            if dis < 1200:
                destination_list.append(poi)
                distance_list.append(dis)
        # 如果存在小于1200m的目标
        if destination_list:
            iters = ceil(len(destination_list) / 50)
            duration = 10000
            for i in range(0, iters):
                time.sleep(1.2)
                # 起点经纬度
                origin_loc = community_bd_lat + "," + community_bd_lon
                # 终点经纬度
                destination_loc_list = [destination[-3] + "," + destination[-4] for destination in destination_list[i * 50:i * 50 + 50]]
                destination_loc_list_str = "|".join(destination_loc_list)
                # 请求API得到最近目标的步行时间
                url = (
                    f'http://api.map.baidu.com/routematrix/v2/walking?output=json&'
                    f'origins={origin_loc}&'
                    f'destinations={destination_loc_list_str}&'
                    f'coord_type=bd09ll&'
                    f'ak=4gvdE4GPNH0kXgpz2j4uVtW9frjUNFe5')
                # 4gvdE4GPNH0kXgpz2j4uVtW9frjUNFe5
                # CzoxjqGE1GpguHyWj48zaDwmrh47lrkX
                res = requests.get(url)
                js = json.loads(res.text)
                if js['status'] != 0:
                    print(res.text)
                    os.system("pause")
                routes = js['result']
                for route in routes:
                    if duration > route['duration']['value']:
                        duration = route['duration']['value']
                print("查询子句:" + str(where_clause) + " 循环次数:" + str(i + 1) + " 检验点数:" + str(len(destination_loc_list)) + " 离此小区:" + community[2] + " 目前最近步行POI时长:" + str(duration))
            # # 求最小值得到索引
            # min_index = distance_list.index(min(distance_list))
            # # 起点经纬度
            # origin_loc = community_bd_lat + "," + community_bd_lon
            # # 终点经纬度
            # destination_loc = destination_list[min_index][-3] + "," + destination_list[min_index][-4] # 算路用百度坐标
            # # "|".join([loc[-1] + "," + loc[-2] for loc in destination_list])
            # # 请求API得到最近目标的步行时间
            # url = (
            #     f'http://api.map.baidu.com/routematrix/v2/walking?output=json&'
            #     f'origins={origin_loc}&'
            #     f'destinations={destination_loc}&'
            #     f'coord_type=bd09ll&'
            #     f'ak=4gvdE4GPNH0kXgpz2j4uVtW9frjUNFe5')
            # res = requests.get(url)
            # js = json.loads(res.text)
            # if js['status'] != 0:
            #     print(res.text)
            #     os.system("pause")
            # min_time = js['result'][0]['duration']['value']
            # print("离此小区:" + community[2] + " 最近的POI:" + destination_list[min_index][0] + destination_list[min_index][1])
        else:
            duration = 10000 # 在1200m范围内根本就没有
        community[col] = duration
        community_new_table.append(community)
    print(community_new_table)
    cur.execute("""
                       DELETE FROM yichang_community_processed
                       """)
    # 前24为原始列 后15为算路因子列
    cur.executemany("""
                       insert into yichang_community_processed values
                       (%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,
                        %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s
                        )
                       """
                    , community_new_table)
    conn.commit()
    conn.close()

def community_add_count(where_clause, col):
    # 读小区表
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    cur.execute("""
                      SELECT * FROM yichang_community_processed;
                      """)
    conn.commit()
    rows = cur.fetchall()
    community_old_table = []
    for row in tqdm(rows):
        row = list(row)
        community_old_table.append(row)
    print(community_old_table)

    # 读POI表
    cur.execute(f"""
                       SELECT * FROM poi_processed WHERE {where_clause};
                       """)
    conn.commit()
    rows = cur.fetchall()
    poi_table = []
    for row in tqdm(rows):
        row = list(row)
        poi_table.append(row)
    print(poi_table)

    # 建立新表 处理后重写
    time.sleep(1)
    community_new_table = []
    for community in tqdm(community_old_table, miniters=5):
        community_bd_lon = community[20]
        community_bd_lat = community[21]
        community_wgs_lon = community[22] #原表24列 输入坐标系参数为百度坐标系 因此取bd经纬度
        community_wgs_lat = community[23]
        # 利用distance list取得最小值索引
        destination_list = []
        distance_list = []
        # 计算POI到点距离 小于1200加入destination list
        for poi in poi_table:
            poi_wgs_lon = poi[-2]
            poi_wgs_lat = poi[-1]
            dis = distance(float(community_wgs_lon), float(community_wgs_lat), float(poi_wgs_lon), float(poi_wgs_lat)) # 大圆距离计算用WGS
            if dis < 1200:
                destination_list.append(poi)
                distance_list.append(dis)
        # 如果存在小于1200m的目标
        if destination_list:
            iters = ceil(len(destination_list)/50)
            count = 0
            for i in range(0, iters):
                time.sleep(1.2)
                # 起点经纬度
                origin_loc = community_bd_lat + "," + community_bd_lon
                # 终点经纬度
                destination_loc_list = [destination[-3] + "," + destination[-4] for destination in destination_list[i*50:i*50+50]]
                destination_loc_list_str = "|".join(destination_loc_list)
                # 请求API得到最近目标的步行时间
                url = (
                    f'http://api.map.baidu.com/routematrix/v2/walking?output=json&'
                    f'origins={origin_loc}&'
                    f'destinations={destination_loc_list_str}&'
                    f'coord_type=bd09ll&'
                    f'ak=CzoxjqGE1GpguHyWj48zaDwmrh47lrkX')
                res = requests.get(url)
                js = json.loads(res.text)
                if js['status'] != 0:
                    print(res.text)
                    os.system("pause")
                routes = js['result']
                for route in routes:
                    duration = route['duration']['value']
                    if duration <= 900:
                        count = count + 1
                print("查询子句:" + str(where_clause) + " 循环次数:" + str(i + 1) + " 检验点数:" + str(len(destination_loc_list)) + " 离此小区:" + community[2] + " 目前15分钟内的POI个数:" + str(count))
        else:
            count = 0 # 在1500m范围内根本就没有
        community[col] = count
        community_new_table.append(community)
    print(community_new_table)
    cur.execute("""
                       DELETE FROM yichang_community_processed
                       """)
    # 前24为原始列 后15为算路因子列
    cur.executemany("""
                       insert into yichang_community_processed values
                       (%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,
                        %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s
                        )
                       """
                    , community_new_table)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    # community_add_gongjiaoshijian()
    # community_add_location()
    # community_add_factor_cols()
    # community_add_min_time("leixing = '公交车站'", 24)
    # community_add_min_time("leixing = '中学'", 25)
    # community_add_count("leixing = '中学'", 26)
    # community_add_min_time("leixing = '小学'", 27)
    # community_add_count("leixing = '小学'", 28)
    # community_add_min_time("leixing = '高等院校'", 29)
    # community_add_count("leixing = '高等院校'", 30)
    # community_add_min_time("leixing = '综合医院'", 31)
    # community_add_count("leixing = '综合医院'", 32)
    # community_add_min_time("leixing = '文化古迹' OR leixing = '风景区'", 33)
    # community_add_count("leixing = '文化古迹' OR leixing = '风景区'", 34)
    community_add_min_time("leixing = '公园' OR leixing = '休闲广场' OR leixing = '植物园' OR leixing = '体育场馆' ", 35)
    community_add_count("leixing = '公园' OR leixing = '休闲广场' OR leixing = '植物园' OR leixing = '体育场馆'", 36)
    community_add_min_time("leixing = '百货商场' OR leixing = '购物中心' OR leixing = '集市'", 37)
    community_add_count("leixing = '百货商场' OR leixing = '购物中心' OR leixing = '集市'", 38)
    # 公园休闲广场/体育场馆/植物园
    # 百货商场 / 购物中心 / 集市


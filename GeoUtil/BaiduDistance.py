import os

# 更改当前目录为文件锁在目录
current_path = os.path.abspath(__file__)
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

import requests
import json
import time
import sys
import datetime
import pandas as pd
import csv
from pathlib import Path

# 读取配置文件
CONFIG_FILE = './setting.json'
f = open(CONFIG_FILE, "r", encoding='utf-8')
SETTING = json.load(f)
print(SETTING)

from math import radians, cos, sin, asin, sqrt


def haversine(lon1, lat1, lon2, lat2):  # 经度1，纬度1，经度2，纬度2 （十进制度数）
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # 将十进制度数转化为弧度
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine公式
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # 地球平均半径，单位为公里
    # 返回单位m
    return c * r * 1000


LAT_COEFFICIENT = 1
DISTANCE_PER_LON = 111000


def approximateDistance(lon1, lat1, lon2, lat2):
    global LAT_COEFFICIENT
    distance = sqrt(
        ((lon1 - lon2) * DISTANCE_PER_LON) ** 2
        + ((lat1 - lat2) * DISTANCE_PER_LON * LAT_COEFFICIENT) ** 2
    )
    return distance


def setLatCoef(lat):
    global LAT_COEFFICIENT
    LAT_COEFFICIENT = sin(radians(lat))


def isWithinBoundary(origin, destination, DEBUG=False):
    distance = haversine(*origin, *destination)
    # 使用近似距离
    # distance = approximateDistance(*origin, *destination)
    if DEBUG:
        print(f'计算距离：{distance}M')
    return distance <= SETTING['boundary']


akIndex = -1


def getAK(invalidate=False):
    global akIndex
    if akIndex == -1:
        akIndex = 0
        return SETTING['apiKey'][0]
    if invalidate:
        print(
            f'AK:{SETTING["apiKey"][akIndex]}失效。剩余{len(SETTING["apiKey"]) - 1}个AK'
        )
        SETTING['apiKey'].remove(SETTING['apiKey'][akIndex])
        akIndex = -1
    if len(SETTING['apiKey']) == 0:
        raise Exception("AK耗尽")

    akIndex += 1
    akIndex %= len(SETTING['apiKey'])
    key = SETTING['apiKey'][akIndex]

    # print('更换AK，当前AK是第 %d 个 \n%s' % (akIndex, key))
    return key


def fetchApi(ori_loc, dest_loc):
    url = (
        f'http://api.map.baidu.com/routematrix/v2/walking?output=json&'
        f'origins={",".join([format(loc, ".5f") for loc in reversed(ori_loc)])}&'
        f'destinations={",".join([format(loc, ".5f") for loc in reversed(dest_loc)])}'
        f'&ak={getAK()}')

    try:
        res = requests.get(url)
    except Exception as e:
        print('请求API接口时发生异常')
        print(e)
        return None

    try:
        res = json.loads(res.text)
    except Exception as e:
        print(res.text)
        print(url)
        print('json文本解析出现异常')
        print(e)
        raise Exception()

    if res['status'] != 0:
        print(res)
        # 如果配额超限，更换AK，重新来过
        if res['status'] == 4 or res['status'] == 302 \
                or ('配额' in res['message'] and '并发' not in res['message']):
            getAK(True)
            return None
        else:
            # 如果返回结果出现错误
            print('返回结果出现错误')
            raise Exception()

    return res


def batchFetchApi(ori_loc, dest_loc_list):
    print(f'发送{len(dest_loc_list)}个批量请求')
    dest_str_list = []
    for dist_loc in dest_loc_list:
        dest_str = ",".join([format(loc, ".5f") for loc in reversed(dist_loc)])
        dest_str_list.append(dest_str)
    dest_str = '|'.join(dest_str_list)

    # [format(loc,".5f") for loc in reversed(dist_loc) for dist_loc in dist_loc_list]
    url = (
        f'http://api.map.baidu.com/routematrix/v2/walking?output=json&'
        f'origins={",".join([format(loc, ".5f") for loc in reversed(ori_loc)])}&'
        f'destinations={dest_str}'
        f'&ak={getAK()}'
        f'&coord_type={"wgs84"}')

    try:
        res = requests.get(url)
    except Exception as e:
        print('请求API接口时发生异常')
        print(e)
        return None

    try:
        res = json.loads(res.text)
    except Exception as e:
        print(res.text)
        print(url)
        print('json文本解析出现异常')
        print(e)
        raise Exception()

    if res['status'] != 0:
        print(res)
        # 如果配额超限，更换AK，重新来过
        if res['status'] == 4 or res['status'] == 302 \
                or ('配额' in res['message'] and '并发' not in res['message']):
            getAK(True)
            return None
        elif '并发' in res['message']:
            return None
        else:
            # 如果返回结果出现错误
            print('返回结果出现错误')
            raise Exception()

    return res


def batchFetch(o, d):
    try:
        while True:
            res = batchFetchApi(o, d)
            if res is not None:
                break
    except Exception as e:
        print(e)
        raise Exception(e)
    return res


# 批量处理点，即使用百度API的批量请求
origin_last = []
destination_list = []


def batchProcess(origin, destination, cw, flush=False):
    global origin_last
    global destination_list
    res = None

    try:
        # 如果立即请求
        if flush == True and len(destination_list) != 0:
            res = batchFetch([origin_last[2], origin_last[3]], [[dest[2], dest[3]] for dest in destination_list])
        # 如果更换了origin，则需要进行请求
        elif len(origin_last) != 0 and origin_last[0] != origin[0]:
            res = batchFetch([origin_last[2], origin_last[3]], [[dest[2], dest[3]] for dest in destination_list])
        # 如果destination个数超过50，则需要进行请求
        elif len(destination_list) >= 50:
            res = batchFetch([origin_last[2], origin_last[3]], [[dest[2], dest[3]] for dest in destination_list])
    except Exception as e:
        print('致命异常，请记录异常信息并联系开发者')
        print(e)
        destination_list.clear()

    if res != None:
        for i in range(len(res['result'])):
            row = res['result'][i]
            cw.writerow([
                origin_last[0], origin_last[1], destination_list[i][0], destination_list[i][1],
                res['result'][i]['distance']['value'],
                res['result'][i]['duration']['value']
            ])
        destination_list.clear()
    if flush == True:
        origin_last.clear()
        destination_list.clear()
    else:
        origin_last = origin
        destination_list.append(destination)


def fetchDistance(origin, destination):
    ori_df = pd.read_excel(origin)
    dest_df = pd.read_excel(destination)

    print(ori_df)
    print(dest_df)

    amount = 0
    start = time.time()
    print(f"本次爬取开始于{datetime.datetime.now()}")

    # 定义输出文件，为文件创建目录
    output_file = Path(
        SETTING['output']) / f'{origin.stem}-{destination.stem}.csv'
    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    with open(output_file, 'w', newline='') as f:
        cw = csv.writer(f)
        cw.writerow([
            'OriginID', 'OriginName', 'DestinationID', 'DestinationName',
            'distance', 'duration'
        ])

        ori_count = len(ori_df.index)
        dest_count = len(dest_df.index)

        # 使用pandas索引并转换成列表的速度比较慢，成为了主要限制程序速度的因素
        # 所以先全部取出，转换成列表，再对列表进行索引
        # 速度慢的可能原因：1、pandas索引慢。2、numpy数组转list慢
        # pandas索引：使用.at来进行遍历https://github.com/pandas-dev/pandas/issues/6683
        # numpy数组：直接使用ndarray
        ori_info_list = ori_df[[
            SETTING['origin']['id'], SETTING['origin']['name']
        ]].values  # .tolist()
        ori_loc_list = ori_df[[
            SETTING['origin']['lon'], SETTING['origin']['lat']
        ]].values  # .tolist()
        dest_info_list = dest_df[[
            SETTING['destination']['id'],
            SETTING['destination']['name']
        ]].values  # .tolist()
        dest_loc_list = dest_df[[
            SETTING['destination']['lon'],
            SETTING['destination']['lat']
        ]].values  # .tolist()

        for i in range(ori_count):
            ori_info = ori_info_list[i]
            ori_loc = ori_loc_list[i]

            # 设置近似距离的纬度系数
            # setLatCoef(ori_loc[1])

            for j in range(dest_count):
                dest_info = dest_info_list[j]
                dest_loc = dest_loc_list[j]

                if (not isWithinBoundary(ori_loc, dest_loc)):
                    # 输出IO耗时
                    # print(
                    #     f"Origin:{i} / {ori_count}; destination:{j} / {dest_count} 距离超限"
                    # )
                    continue
                amount += 1
                print(
                    f"Origin:{i + 1} / {ori_count}; destination:{j + 1} / {dest_count} ({amount})"
                )

                batchProcess([*ori_info, *ori_loc], [*dest_info, *dest_loc], cw)

                # try:
                #     while True:
                #         res = fetchApi(ori_loc,dest_loc)
                #         if res is not None:
                #             break
                # except Exception as e:
                #     print(e)
                #     continue

                # cw.writerow([
                #     ori_info[0], ori_info[1], dest_info[0], dest_info[1],
                #     res['result'][0]['distance']['value'],
                #     res['result'][0]['duration']['value']
                # ])
        batchProcess(None, None, cw, True)
    end = time.time()
    print(f"本次爬取结束于{datetime.datetime.now()}，共耗时{end - start:.1f}s")


if __name__ == '__main__':
    origin = Path(SETTING['origin']['file'])
    dest_dir = Path(SETTING['destination']['dir'])
    dest_list = dest_dir.glob('*.xls*')
    for dest in dest_list:
        fetchDistance(origin, dest)

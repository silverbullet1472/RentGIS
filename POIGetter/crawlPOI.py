# -*- coding: UTF-8 -*-
import requests
import json
import time
import sys
from openpyxl import Workbook

import os
# 更改当前目录为文件锁在目录
current_path = os.path.abspath(__file__)
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# 读取配置文件
CONFIG_FILE = './setting.json'
f = open(CONFIG_FILE, "r", encoding='utf-8')
setting = json.load(f)
print(setting)

setting['akIndex'] = 0

# 关注区域的左下角和右上角百度地图坐标(经纬度）
Boundary = setting['boundary']
# 定义细分窗口的数量，横向X * 纵向Y
WindowSize = setting['windowSize']


def getAK():
    if len(setting['apiKey']) == setting['akIndex']:
        raise Exception("AK耗尽")
    key = setting['apiKey'][setting['akIndex']]
    setting['akIndex'] = setting['akIndex'] + 1
    print('更换AK，当前AK是第 %d 个 \n%s' % (setting['akIndex'], key))
    return key


# 获取初始百度Key
API_KEY = getAK()


def getRect(boundary,
            windowSize={
                'xNum': 1.0,
                'yNum': 1.0
            },
            windowIndex=0,
            stringfy=True):
    offset_x = (boundary['right']['x'] -
                boundary['left']['x']) / windowSize['xNum']
    offset_y = (boundary['right']['y'] -
                boundary['left']['y']) / windowSize['yNum']
    left_x = boundary['left']['x'] + offset_x * (windowIndex %
                                                 windowSize['xNum'])
    left_y = boundary['left']['y'] + offset_y * (windowIndex //
                                                 windowSize['yNum'])
    right_x = (left_x + offset_x)
    right_y = (left_y + offset_y)
    if stringfy:
        return str(left_y) + ',' + str(left_x) + ',' + str(
            right_y) + ',' + str(right_x)
    else:
        return {
            "left": {
                "x": left_x,
                "y": left_y
            },
            "right": {
                "x": right_x,
                "y": right_y
            }
        }


def fetchPOI(keyword, boundary, ws):
    time.sleep(0.003)
    global API_KEY
    pageNum = 0
    first_it = True
    while True:
        URL = "http://api.map.baidu.com/place/v2/search?query="+keyword+ \
            "&bounds=" + getRect(boundary) + \
            "&output=json" +  \
            "&ak=" + API_KEY + \
            "&scope=2" + \
            "&page_size=20" + \
            "&coord_type=wgs84ll"+ \
            "&page_num=" + str(pageNum)
        try:
            resp = requests.get(URL)
        except Exception as e:
            print('请求API接口时发生异常')
            print(e)
            continue

        try:
            res = json.loads(resp.text)
        except Exception as e:
            print(resp.text)
            print(URL)
            print('json文本解析出现异常')
            print(e)
            pageNum += 1
            continue
            
        if res['status'] != 0:
            print(res)
            # 如果配额超限，更换AK，重新来过
            if res['status'] == 4 or res['status'] == 302 \
            or ('配额' in res['message'] and '并发' not in res['message']):
                print(res)
                API_KEY = getAK()
                continue
            else:
                # 如果返回结果出现错误
                print('返回结果出现错误')
                continue

        total = res['total']

        if first_it:
            print('找到要素', total, '个')
            first_it = False

        # 如果此区域不存在点
        if total == 0:
            break
        # 如果翻页后，此页没有
        elif len(res['results']) == 0:
            break
        # 超过了400个，需要划分小格子。把当前区域划分成4个小格子
        elif total == 400:
            for i in range(4):
                fetchPOI(
                    keyword,
                    getRect(boundary, {
                        'xNum': 2.0,
                        'yNum': 2.0
                    }, i, False), ws)
            # 递归完成之后，跳出循环
            break
        else:
            count = len(res['results'])
            for r in res['results']:
                # 访问字段异常
                try:
                    values = [
                        r['name'],
                        float(r["location"]["lat"]),
                        float(r["location"]["lng"]), r["address"], r["area"]
                    ]
                    if r["detail"] == 1:
                        # 有时候没有type字段
                        if 'type' in r["detail_info"]:
                            values.append(r["detail_info"]["type"])
                        # 有时候没有tag字段
                        if 'tag' in r["detail_info"]:
                            values.append(r["detail_info"]["tag"])
                    ws.append(values)
                except Exception as e:
                    print('访问字段异常')
                    print(r)
                    print(e)
                
            print('完成要素：%d / %d' % (20 * pageNum + count, total))
            # 如果等于二十个，需要翻页。否则不用翻页
            if count == 20:
                pageNum += 1
            else:
                break


def requestBaiduApi(keyword, ws):
    # 声明全局变量，从而可以对其进行赋值（Python没有声明关键字的特点
    global API_KEY
    # 添加标题
    ws.append(['名称', '纬度', '经度', '地址', '区县', '一类', '二类'])
    # 循环视口
    windowNum = int(WindowSize['xNum'] * WindowSize['yNum'])
    for i in range(windowNum):
        rect = getRect(Boundary, WindowSize, i, stringfy=False)

        print('当前搜索窗口：%d / %d (%s)' % (i + 1, windowNum, keyword))
        fetchPOI(keyword, rect, ws)


def main():
    # 创建一个工作簿
    wb = Workbook()
    for keyword in setting['keyWord']:
        # 根据keyword创建对应的工作表
        ws = wb.create_sheet(title=keyword)
        requestBaiduApi(keyword, ws)
        # 保存工作簿
        wb.save(f'./{setting["city"]}.xlsx')
        time.sleep(1)


if __name__ == '__main__':
    main()
    # 白山 "left": {"x": 126.127889, "y": 41.36848}, "right": {"x": 128.325033, "y": 42.849945}
    # 玉树 "left": { "x": 95.704402, "y": 32.029359 }, "right": { "x": 97.742659, "y": 33.779319 }
    # 焦作 "left": { "x": 112.575229, "y": 34.814122 }, "right": { "x": 113.653416, "y": 35.499762 }
    # 大理 "left": { "x": 99.982194, "y": 25.426329 }, "right": { "x": 100.443298, "y": 26.079548 }
    # 衡阳 "left": { "x": 111.546799, "y": 26.104783 }, "right": { "x": 113.289203, "y": 27.472676 }
    # 北京 "left": { "x": 115.430651, "y": 39.448725 }, "right": { "x": 117.51999, "y": 41.066947 }
    # 宜昌 西陵区 "left": { "x": 111.221491, "y": 30.680533 }, "right": { "x": 111.376173, "y": 30.80638 }
    # 宜昌 伍家岗区 "left": { "x": 111.292478, "y": 30.601377 }, "right": { "x": 111.472727, "y": 30.704146 }
    # 宜昌 夷陵区 "left": { "x": 110.862134, "y": 30.544811 }, "right": { "x": 111.669738, "y": 31.466108 }
    # 宜昌 点军区 "left": { "x": 110.96008, "y": 30.563968 }, "right": { "x": 111.399811, "y": 30.784477 }
    # 宜昌 猇亭区 "left": { "x": 111.390532, "y": 30.460417 }, "right": { "x": 111.528547, "y": 30.642635 }
    # 宜昌 "left": { "x": 110.261482, "y": 29.942151 }, "right": { "x": 112.093107, "y": 31.571794 }


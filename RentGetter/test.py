from multiprocessing import Pool
import time
import psycopg2
import db_option
import os


def func(x):
    if x < 5:
        return x * x
    else:
        return


if __name__ == "__main__":
    mp1 = Pool(1)
    mp2 = Pool(4)
    mp3 = Pool(8)
    mp4 = Pool(3)
    ls = range(0, 8000000)

    print("单核")
    t1 = time.time()
    res = mp1.map(func, ls)
    t2 = time.time()
    print(t2 - t1)

    print("三核")
    t1 = time.time()
    res = mp3.map(func, ls)
    t2 = time.time()
    print(t2 - t1)

    print("四核")
    t1 = time.time()
    res = mp2.map(func, ls)
    t2 = time.time()
    print(t2 - t1)

    print("四核分工")
    t1 = time.time()
    res = mp2.map(func, ls, int(len(ls) / 4))
    t2 = time.time()
    print(t2 - t1)

    print("逻辑八核")
    t1 = time.time()
    res = mp3.map(func, ls)
    t2 = time.time()
    print(t2 - t1)



    # db_option.create_table_community("yushu")
    # table = [[ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1], [ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]]
    # conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    # cur = conn.cursor()
    # # 房屋属性列表 ["城市", "一级区域", "标题", "房租", "户型", "整租合租", "面积", "朝向","楼层", "装修", "小区名称", "帖数", "详细地址", "个人/经纪人", "房屋描述"]
    # cur.executemany(f"insert into yushu_community values ('a','a',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", table)
    # conn.commit()
    # conn.close()
    #
    # print(os.cpu_count())

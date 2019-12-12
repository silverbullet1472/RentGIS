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
    mp1 = Pool()
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
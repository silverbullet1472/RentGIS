# @Time    : 2019/12/11 16:44
# @Author  : Jiaqi Ding 
# @Email   : jiaqiding.ricky@foxmail.com

from requester import ultimate_request
import db_option
import info_parser
from multiprocessing import Pool
import datetime
from tqdm import tqdm
import time


def community_post_extract(post_url):
    time.sleep(10)
    # 获取本条post 如有异常允许抛出后pass
    if post_url:
        try:
            post_r = ultimate_request(post_url[0], allow_exception=True)
            post_value_list = info_parser.community_parser(post_r, post_url[0])
        except Exception as e:
            print("获取本条post时出错:" + str(e))
            return [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, post_url[0][0:200]]
        else:
            print("本条post信息:" + str(post_value_list))
            return post_value_list


def get_community(city, db_code, start=None, end=None):
    yijiquyu_list = db_option.get_yiji_list(db_code)
    print("进入一级区域:"+str(yijiquyu_list))
    for yijiquyu in yijiquyu_list[start:end]:
        community_url_set = db_option.fetch_community_urls(db_code, yijiquyu[0])
        print(community_url_set)
        print("此一级区域所有小区总量:" + str(len(community_url_set)))
        # 记录并发用时
        print(datetime.datetime.now().strftime('%H:%M:%S'))
        t1 = datetime.datetime.now()
        # 对此一级区域所有去重后的url进行收集
        print("开始收集此一级区域信息:" + yijiquyu[0])
        post_value_table = []
        # 新建进程池 并行收集
        with Pool(1) as extract_pool:
            length = len(community_url_set)
            with tqdm(total=length, miniters=1, mininterval=0) as extract_bar:
                for i, post_value_list in tqdm(enumerate(extract_pool.imap_unordered(func=community_post_extract, iterable=community_url_set))):
                    post_value_table.append(post_value_list)
                    extract_bar.update()
                    print("任务进度:" + str(i) + "/" + str(length))
        db_option.insert_table_community(db_code, post_value_table, city, yijiquyu[0])
        print("此一级区域信息入库:" + yijiquyu[0])
        print(datetime.datetime.now().strftime('%H:%M:%S'))
        t2 = datetime.datetime.now()
        print("extract并发用时:"+str((t2 - t1)))


if __name__ == "__main__":
    get_community("宜昌", "yichang")

#                    _ooOoo_
#                   o8888888o
#                   88" . "88
#                   (| -_- |)
#                    O\ = /O
#                ____/`---'\____
#              .   ' \\| |// `.
#               / \\||| : |||// \
#             / _||||| -:- |||||- \
#               | | \\\ - /// | |
#             | \_| ''\---/'' | |
#              \ .-\__ `-` ___/-. /
#           ___`. .' /--.--\ `. . __
#        ."" '< `.___\_<|>_/___.' >'"".
#       | | : `- \`.;`\ _ /`;.`/ - ` : | |
#         \ \ `-. \_ __\ /__ _/ .-` / /
# ======`-.____`-.___\_____/___.-`____.-'======
#                    `=---='
#          佛祖保佑             永无BUG

# @Time    : 2019/12/11 16:44
# @Author  : Jiaqi Ding 
# @Email   : jiaqiding.ricky@foxmail.com

from requester import xun_request
import db_option
import info_parser
from multiprocessing import Pool
import time


def community_post_extract(post_url):
    # 获取本条post 如有异常允许抛出后pass
    try:
        post_r = xun_request(post_url, allow_exception=True)
        post_value_list = info_parser.community_parser(post_r)
        print("本条post信息:" + str(post_value_list))
    except Exception as e:
        print("获取本条post时出错:" + str(e))
        return [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]
    else:
        return post_value_list


def get_community(city, db_code):
    # 创建该城市数据表
    db_option.create_table_community(db_code)
    yijiquyu_list = db_option.get_yiji_community_list(db_code)
    for yijiquyu in yijiquyu_list:
        community_url_set = db_option.fetch_community_urls(db_code, yijiquyu)
        # 记录并发用时
        t1 = time.time()
        # 对此一级区域所有去重后的url进行收集
        print("开始收集此一级区域信息:" + yijiquyu)
        # 新建进程池 并行收集
        post_extract_pool = Pool()
        post_value_table = post_extract_pool.map(func=community_post_extract, iterable=community_url_set)
        db_option.insert_table_community(db_code, post_value_table, city, yijiquyu)
        post_extract_pool.close()
        post_extract_pool.join()
        print("此一级区域信息入库:" + yijiquyu)
        t2 = time.time()
        print("extract并发用时:" + str((t2 - t1)))


if __name__ == "__main__":
    get_community("白山", "baishan")

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

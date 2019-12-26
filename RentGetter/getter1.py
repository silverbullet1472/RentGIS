from lease_getter import get_lease
from sale_getter import get_sale
from db_option import *
import time

if __name__ == "__main__":
    get_sale("北京", "beijingv2", 0, 1)

    # 宜昌 租房 3进程并发 用时 938条/136s
    # 租房 4进程并发 用时: 927条/108s

    # 进度update
    # V1.0: 未用到referer 数据表无post_url
    # v2.0: 用到referer 有post_url
    # 白山 租房 v2.0 全部完成 | 二手房 v2.0 全部完成
    # 北京 租房 v1.0 完成:朝阳/丰台/大兴/通州/海淀 | 二手房 完成:朝阳
    # 大理 租房 v2.0 全部完成 | 二手房 未完成
    # 衡阳 租房 v2.0 全部完成 | 二手房 未完成
    # 焦作 租房 v2.0 全部完成 | 二手房 未完成
    # 宜昌 租房 v2.0 全部完成 | 二手房 v2.0 全部完成
    # 玉树 租房 1条 | 二手房 0条


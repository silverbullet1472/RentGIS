from community_getter import *
from sale_getter import *
import db_option

if __name__ == "__main__":
    # 创建该城市数据表
    db_option.create_table_community("baishan")
    get_community("白山", "baishan")

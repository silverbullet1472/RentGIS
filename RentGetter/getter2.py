from community_getter import get_community
import db_option

if __name__ == "__main__":
    # 创建该城市数据表
    # db_option.create_table_community("yichang")
    get_community("宜昌", "yichang")

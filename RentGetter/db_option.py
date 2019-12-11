# @Time    : 2019/12/9 11:04
# @Author  : Jiaqi Ding
# @Email   : jiaqiding.ricky@foxmail.com

import psycopg2


def create_table_sale(db_code):
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    # 房屋属性列表 ["标题", "总价", "单价", "户型", "面积", "朝向", "楼层", "建筑年代", "产权", "装修", "小区名称", "帖数", "详细地址", "个人/经纪人", "房屋描述"]
    cur.execute(f"""
                create table {db_code}_sale
                (
                    chengshi        varchar(50),
                    yijiquyu        varchar(50),
                    biaoti          varchar(50),
                    zongjia         varchar(50),
                    danjia          varchar(50),
                    huxing          varchar(50),
                    mianji          varchar(50),
                    chaoxiang       varchar(50),
                    louceng         varchar(50),
                    jianzhuniandai  varchar(50),
                    chanquan        varchar(50),
                    zhuangxiu       varchar(50),
                    xiaoqumingcheng varchar(50),
                    tieshu          varchar(50),  
                    xiangxidizhi    varchar(50),
                    gerenjingjiren  varchar(50),
                    fangwumiaoshu   varchar(300)
                );
                """)
    conn.commit()
    conn.close()


def create_table_lease(db_code):
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    # 房屋属性列表 ["标题", "房租", "户型", "整租合租", "面积", "朝向","楼层", "装修", "小区名称", "帖数", "详细地址", "个人/经纪人", "房屋描述"]
    cur.execute(f"""
                create table {db_code}_lease(
                chengshi        varchar(50),
                yijiquyu        varchar(50),
                biaoti          varchar(50),
                fangzu          varchar(50),
                huxing          varchar(50),
                zhengzuhezu     varchar(50),
                mianji          varchar(50),
                chaoxiang       varchar(50),
                louceng         varchar(50),
                zhuangxiu       varchar(50),
                xiaoqumingcheng varchar(50),
                teishu          varchar(50),
                xiangxidizhi    varchar(50),
                gerenjingjiren  varchar(50),
                fangwumiaoshu   varchar(300)
                );
                """)
    conn.commit()
    conn.close()


def create_table_community(db_code):
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    # 小区属性列表 ["小区名称", "小区均价", "价格走势", "区域商圈", "详细地址", "建筑类型", "物业费用", "产权类别", "容积率", "总户数", "绿化率", "建筑年代", "停车位", "开发商", "物业公司", "在租数", "在售数"]
    cur.execute(f"""
                create table {db_code}_community
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
                    zaishoushu      varchar(50)
                );
                """)
    conn.commit()
    conn.close()


def insert_table_lease(db_code, value_table, city, yiji):
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    # 房屋属性列表 ["城市", "一级区域", "标题", "房租", "户型", "整租合租", "面积", "朝向","楼层", "装修", "小区名称", "帖数", "详细地址", "个人/经纪人", "房屋描述"]
    cur.executemany(f"insert into {db_code}_lease values ('{city}','{yiji}',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", value_table)
    conn.commit()
    conn.close()


def insert_table_sale(db_code, value_table, city, yiji):
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    # 房屋属性列表 ["城市", "一级区域", "标题", "总价", "单价", "户型", "面积", "朝向", "楼层", "建筑年代", "产权", "装修", "小区名称", "帖数", "详细地址", "个人/经纪人", "房屋描述"]
    cur.executemany(f"insert into {db_code}_sale values ('{city}','{yiji}',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", value_table)
    conn.commit()
    conn.close()


def insert_table_community(db_code, value_table, city, yiji):
    conn = psycopg2.connect(dbname="rent_db", user="postgres", password="postgresql", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    # 小区属性列表 ["城市", "一级区域", "小区名称", "小区均价", "价格走势", "区域商圈", "详细地址", "建筑类型", "物业费用", "产权类别", "容积率", "总户数", "绿化率", "建筑年代", "停车位", "开发商", "物业公司", "在租数", "在售数"]
    cur.executemany(f"insert into {db_code}_community values ('{city}','{yiji}',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", value_table)
    conn.commit()
    conn.close()
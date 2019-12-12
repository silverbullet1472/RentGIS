# @Time    : 2019/12/9 11:04
# @Author  : Jiaqi Ding
# @Email   : jiaqiding.ricky@foxmail.com

import lease_getter
import sale_getter

if __name__ == "__main__":
    city_list = [["白山", "baishan"]]
    for city in city_list:
        # lease_getter.get_lease(city[0], city[1])
        # print("租房爬取完成" + city[0])
        sale_getter.get_sale(city[0], city[1])
        print("二手房房爬取完成" + city[0])
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
#  , ["玉树", "yushu"], ["焦作", "jiaozuo"] ,  ["衡阳", "hengyang"], ["大理", "dali"], ["北京", "beijing"]
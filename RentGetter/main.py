import PaidLeaseGetter
import PaidSaleGetter

if __name__ == "__main__":
    city_list = [["北京", "beijing"], ["大理", "dali"], ["衡阳", "hengyang"], ["白山", "baishan"], ["玉树", "yushu"], ["焦作", "jiaozuo"]]
    for city in city_list:
        PaidLeaseGetter.get_lease(city[0], city[1])
        PaidSaleGetter.get_sale(city[0], city[1])
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
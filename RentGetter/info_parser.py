# @Time    : 2019/12/9 11:04
# @Author  : Jiaqi Ding
# @Email   : jiaqiding.ricky@foxmail.com

import re
from bs4 import BeautifulSoup


def community_parser(community_r, url):
    community_bs = BeautifulSoup(community_r.text, 'lxml')
    # 小区名称
    community_name = community_bs.select_one('.card-top .card-title').get_text(strip=True)
    # 小区均价
    community_price = community_bs.select_one('span.price').contents[0]
    # 价格走势
    community_contrast_node = community_bs.select_one('div .contrast')
    try:
        community_contrast = re.findall(r'(\d+(\.\d+)?%)', community_contrast_node.contents[0])[0][0]
    except Exception as e:
        community_contrast = 'unknown'
        print("价格走势正则提取时出错:" + str(e))
    if 'down' in community_contrast_node['class']:
        community_contrast = '-' + community_contrast
    # ["区域商圈", "详细地址", "建筑类型", "物业费用", "产权类别", "容积率", "总户数", "绿化率", "建筑年代", "停车位", "开发商", "物业公司"]
    community_value_list1 = [re.sub(r'\s+', ' ', attr.get_text(strip=True)) for attr in
                             community_bs.select('li.item.f-fl .content')]
    # 在租 在售信息
    rent_sale_node = community_bs.select('.xq-card-resource span a')
    try:
        community_rent = re.findall(r'\d+', rent_sale_node[0].get_text(strip=True))[0]
    except Exception as e:
        community_rent = 'unknown'
        print("在租正则提取时出错:" + str(e))
    try:
        community_sale = re.findall(r'\d+', rent_sale_node[1].get_text(strip=True))[0]
    except Exception as e:
        community_sale = 'unknown'
        print("在售正则提取时出错:" + str(e))
    # 小区属性列表 ["小区名称", "小区均价", "价格走势", "区域商圈", "详细地址", "建筑类型", "物业费用", "产权类别", "容积率", "总户数", "绿化率", "建筑年代", "停车位", "开发商", "物业公司", "在租数", "在售数"]
    community_value_list = [community_name, community_price, community_contrast] + community_value_list1 + [community_rent] + [community_sale]
    community_value_list = [x[:50] for x in community_value_list] + [url[0:200]]
    return community_value_list


def sale_parser(sale_r, url):
    sale_bs = BeautifulSoup(sale_r.text, 'lxml')
    # 标题 总价 单价
    sale_title = re.sub(r'\s+', ' ', sale_bs.select_one('.card-top .card-title').get_text(strip=True))
    sale_total = re.sub(r'\s+', ' ', sale_bs.select_one('span.price').get_text(strip=True))
    sale_unit = re.sub(r'[|\s+]', '', sale_bs.select_one('span.unit').get_text(strip=True))
    # 户型 面积 朝向 楼层 建筑年代 产权 装修
    sale_value_list1 = [re.sub(r'\s+', ' ', x.get_text(strip=True)) for x in
                        sale_bs.select('li.item.f-fl .content')]
    # 小区是否存在跳转链接
    community_link_node = sale_bs.select_one('.er-item .content a')
    if community_link_node:
        # 小区名称
        community_name = community_link_node.get_text(strip=True)
        # 帖数 详细地址
        sale_value_list2 = sale_bs.select('.er-item .content')
        try:
            post_num = re.findall(r'\d+', sale_value_list2[0].get_text(strip=True))[0]
        except Exception as e:
            post_num = 'unknown'
            print("正则提取时出错:" + str(e))
        sale_address = re.sub(r'\s+', '', sale_value_list2[1].text)
        # 小区url
        community_url = community_link_node['href']
    else:
        sale_value_list3 = sale_bs.select('.er-item .content')
        # 小区名称
        community_name = sale_value_list3[0].get_text(strip=True)
        # 帖数 详细地址
        post_num = 'unknown'
        sale_address = re.sub(r'\s+', '', sale_value_list3[1].text)
        # 小区url
        community_url = ""
    # 经纪人/个人 房屋描述
    agent = "company" if sale_bs.select_one('.user-info-top .license_box') else "individual"
    # 房屋属性列表 ["标题", "总价", "单价", "户型", "面积", "朝向", "楼层", "建筑年代", "产权", "装修", "小区名称", "帖数", "详细地址", "个人/经纪人", "房屋描述"]
    sale_comment = re.sub(r'\s+', ' ', sale_bs.select_one('.describe .item').get_text(strip=True))[0:300]
    sale_value_list = [sale_title, sale_total, sale_unit] + sale_value_list1 + [community_name, post_num, sale_address, agent]
    sale_value_list = [x[:50] for x in sale_value_list] + [sale_comment] + [url[0:200]] + [community_url[0:200]]
    return sale_value_list


def lease_parser(lease_r,url):
    lease_bs = BeautifulSoup(lease_r.text, 'lxml')
    # 标题 房租
    lease_title = re.sub(r'\s+', ' ', lease_bs.select_one('.card-top .card-title').get_text(strip=True))
    lease_price = re.sub(r'\s+', ' ', lease_bs.select_one('span.price').get_text(strip=True))
    # 户型 整租合租 面积 朝向 楼层 装修
    lease_value_list1 = [re.sub(r'\s+', ' ', x.get_text(strip=True)) for x in lease_bs.select('li.item.f-fl .content')]
    lease_value_list = [lease_title, lease_price] + [lease_value_list1[0]] + re.split(r'\s+', lease_value_list1[1]) + lease_value_list1[2:]
    # 小区名称
    community_link_node = lease_bs.select_one('.er-item .content a')
    if community_link_node:
        # 小区名称
        community_name = community_link_node.get_text(strip=True)
        # 帖数 详细地址
        lease_value_list2 = lease_bs.select('.er-item .content')
        try:
            post_num = re.findall(r'\d+', lease_value_list2[0].get_text(strip=True))[0]
        except Exception as e:
            post_num = 'unknown'
            print("正则提取时出错:" + str(e))
        lease_address = re.sub(r'\s+', '', lease_value_list2[2].text)
        # 小区url
        community_url = community_link_node['href']
    else:
        lease_value_list3 = lease_bs.select('.er-item .content')
        # 小区名称 帖数 详细地址
        community_name = lease_value_list3[0].get_text(strip=True)
        post_num = 'unknown'
        lease_address = re.sub(r'\s+', '', lease_value_list3[2].text)
        # 小区url
        community_url = ""
    # 经纪人/个人 房屋描述
    agent = "company" if lease_bs.select_one('.user-info-top .license_box') else "individual"
    lease_comment = re.sub(r'\s+', ' ', lease_bs.select_one('.describe .item').get_text(strip=True))[0:300]
    # 房屋属性列表 ["标题", "房租", "户型", "整租合租", "面积", "朝向","楼层", "装修", "小区名称", "帖数", "详细地址", "个人/经纪人", "房屋描述"]
    lease_value_list = lease_value_list + [community_name, post_num, lease_address, agent]
    lease_value_list = [x[:50] for x in lease_value_list] + [lease_comment] + [url[0:200]] + [community_url[0:200]]
    return lease_value_list

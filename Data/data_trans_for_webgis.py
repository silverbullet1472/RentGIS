# @Time    : 2020/1/7 21:07
# @Author  : Jiaqi Ding 
# @Email   : jiaqiding.ricky@foxmail.com
import csv
import re

def mianji_trans():
    table = []
    with open(r'yichang_sale_located.csv', 'r', encoding="utf-8", newline='') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            danjia = re.findall(r'\d+', row[2])[0]
            new_row = [row[0], row[1]] + [danjia] + row[3:]
            table.append(new_row)
    print(table)

    with open(r'yichang_sale_located.csv', 'w', encoding="utf-8", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(table)

def lonlat_trans(type):
    table = []
    with open(r'yichang_'+type+'_located.csv', 'r', encoding="utf-8", newline='') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            new_row = row + [row[-2]+","+row[-1]]
            table.append(new_row)
    print(table)

    with open(r'yichang_'+type+'_located.csv', 'w', encoding="utf-8", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header + ['amaplonlat'])
        writer.writerows(table)

if __name__ == "__main__":
    table = []
    with open(r'yichang_community_located.csv', 'r', encoding="utf-8", newline='') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            zoushi = row[4].rstrip('%')
            new_row = row[:4] + [zoushi] + row[5:]
            table.append(new_row)
    print(table)

    with open(r'yichang_community_located.csv', 'w', encoding="utf-8", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(table)
import pandas as pd
from pathlib import Path

from trans_util import bd09_to_wgs84


def main():
    source_dir = Path(r'D:\Document\HousePricing\Community\community')
    dist_dir = Path(r'D:\Document\HousePricing\Community\community_wgs84')
    source_list = [x for x in source_dir.glob('*.csv')]
    for source_path in source_list:

        f = open(source_path)
        df = pd.read_csv(f)
        f.close()

        try:
            origin_coors = df[['经度','纬度']].values
        except Exception as e:
            print(source_path)
            continue
        trans_coord = []
        for coor_origin in origin_coors:
            trans_coord.append(bd09_to_wgs84(*coor_origin))
        df['经度_wgs84'] = [x[0] for x in trans_coord]
        df['纬度_wgs84'] = [x[1] for x in trans_coord]

        # 之所以出错，是因为使用了to_excel，但文件的后缀名是csv
        # 要么改用to_csv方法，要么将后缀名改为xlsx
        df.to_csv(dist_dir/source_path.name,index=False)
        

if __name__ == "__main__":
    main()
import pandas as pd
from pathlib import Path

from trans_util import bd09_to_wgs84


def main():
    source_dir = Path(r'D:\Document\HousePricing\POI\poi_医院已分类')
    dist_dir = Path(r'D:\Document\HousePricing\CRSTrans\output')
    # 获取输入目录下，城市名/POI名.xls的所有Excel文件路径
    source_list = [x for x in source_dir.glob('*/*.xls*')]
    for source_path in source_list:
        df = pd.read_excel(source_path)
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

        city_dir = dist_dir/source_path.parent.name
        city_dir.mkdir(parents=True,exist_ok=True)
        df.to_excel(city_dir/source_path.name,index=False)
        

if __name__ == "__main__":
    main()
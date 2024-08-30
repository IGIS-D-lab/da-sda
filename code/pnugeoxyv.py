#-*- coding: utf-8 -*-
import warnings; warnings.filterwarnings('ignore')
import os, datetime, argparse, pytz, json, sys

import pandas as pd
import geopandas as gpd

from sqlalchemy import create_engine
from shapely import wkt
from shapely.geometry import Point
from dotenv import load_dotenv; load_dotenv("../env/.env")
from tqdm import tqdm

sys.path.append('../');sys.path.append('../../')
from util.epsg import *
from util.distance import *

class PnuGeoXyv:
    '''
    Get PnuGeoXyv Data For Experiment for given year
    '''
    def __init__(self, experiment_env, experiment_yyyymmdd):
        self._print_time(msg = "Class Init")
        self.experiment_env = experiment_env
        self.experiment_yyyymmdd = experiment_yyyymmdd

        if self.experiment_env == 'local':
            dbid = os.environ.get('ID_LOCALDB')
            dbpw = os.environ.get('PW_LOCALDB')
            dbhost = os.environ.get('HOST_LOCALDB')
            dbport = os.environ.get('PORT_LOCALDB')
            dbdbname = os.environ.get('DBNAME_LOCALDB')
            self.engine = create_engine(f"postgresql://{dbid}:{dbpw}@{dbhost}:{dbport}/{dbdbname}")

        elif self.experiment_env == 'dlabtest':
            dbid = os.environ.get('ID_DLABTEST')
            dbpw = os.environ.get('PW_DLABTEST')
            dbhost = os.environ.get('HOST_DLABTEST')
            dbport = os.environ.get('PORT_DLABTEST')
            dbdbname = os.environ.get('DBNAME_DLABTEST')
            self.engine = create_engine(f"postgresql://{dbid}:{dbpw}@{dbhost}:{dbport}/{dbdbname}")
        
        else:
            raise Exception('-e should be either \"local\" or  \"dlabtest\". Please Check')

        self.to_fpath = f'../asset/experiment/pnugeoxyv/'

        self._print_time(msg = "[PnuGeoXyv]: Extract pnugeo")
        pnugeo_path = f'../asset/experiment/pnugeo/pnugeo_{self.experiment_yyyymmdd}.csv'
        pnugeo = pd.read_csv(pnugeo_path, encoding = 'euc-kr', dtype = {'baseccd': str})
        geometry = [Point(xy) for xy in zip(pnugeo['x'], pnugeo['y'])]        
        pnugeo = gpd.GeoDataFrame(pnugeo, geometry=geometry, crs = 'EPSG:4326')
        #self.pnugeo = add_coord(pnugeo)
        self.pnugeo = add_coord(pnugeo)

        self._print_time(msg = "[PnuGeoXyv]: Extract target sdgeo(maybe Seoul)")
        sql_query = "SELECT *, ST_AsText(geometry) as geom_wkt FROM geodb.sdgeo where sdgbcd = 'rlg11';" 
        target_sdgeo = pd.read_sql_query(sql_query, self.engine)
        target_sdgeo = target_sdgeo.drop(columns='geometry')
        target_sdgeo = target_sdgeo.rename(columns= {'geom_wkt': 'geometry'})
        target_sdgeo = target_sdgeo.geometry.apply(wkt.loads)
        target_sdgeo = gpd.GeoDataFrame(target_sdgeo, geometry='geometry', crs='epsg:4326')
        self.target_sdgeo = target_sdgeo.to_crs('epsg:5179')
        self.feature_configs = []

    def _print_time(self, msg=None):
        timezone = pytz.timezone('Asia/Seoul')
        now = datetime.datetime.now(timezone)
        formatted_time = now.strftime('%Y %b %d %a %H:%M:%S')
        if msg is not None:
            fprint_msg = f"[{formatted_time}]: {msg}"
        else:
            fprint_msg = f"[{formatted_time}]"
        print(fprint_msg)
        return

    def pps_featuredb_busmetro(self, feature_nm:str, stdr_yyyy:str) -> pd.DataFrame():
        sql_query = f"SELECT * FROM featuredb.{feature_nm} WHERE yyyymm like '{stdr_yyyy}%%';"
        df = pd.read_sql_query(sql_query, self.engine)
        df['v'] = df['0809d'] + df['0910d'] + df['1718o'] + df['1819o'] # v: 08~10시 해당 정류소에 도착한 인구 + 17~19에 해당 정류소를 떠난 인구
        df.dropna(subset=['x', 'y'], inplace=True)
        df = df[(df['x'] != 0.0) | (df['y'] != 0.0)]
        df = add_coord(df)
        use_cols = ['yyyymm', 'stnm', 'stid', 'x', 'y', 'x_5179', 'y_5179', 'v']
        df = df[use_cols].drop_duplicates()
        df['yyyy'] = df['yyyymm'].str.slice(0, 4)  # 연도(yyyy)
        # 각 정류장(stid)의 연도별 평균 유동인구를 계산합니다.
        df = df.groupby(['yyyy', 'stnm', 'stid', 'x', 'y', 'x_5179', 'y_5179'])['v'].mean().reset_index()
        df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['x_5179'], df['y_5179']), crs = 'EPSG:5179')
        return df

    def pps_featuredb_corpfin(self, feature_nm:str, stdr_yyyy:str) -> pd.DataFrame():
        sql_query = f"SELECT * FROM featuredb.{feature_nm} WHERE settleyyyy = '{stdr_yyyy}';"
        df = pd.read_sql_query(sql_query, self.engine)
        df.dropna(subset=['x', 'y'], inplace=True)
        df = df[(df['x'] != 0.0) | (df['y'] != 0.0)]
        df = add_coord(df)
        use_cols = ['corpnm', 'marketnm','stockcd', 'value', 'settleyyyy', 'findocennm', 'finaccountennm', \
                    'finaccountkrnm','corpaddress', 'x', 'y', 'x_5179', 'y_5179']
        df = df[use_cols]
        # 매출액 데이터 추출시 finaccountennm 값: ifrs_Revenue(~2018), ifrs-full_Revenue(2019~)
        df = df[df['finaccountennm'].isin(['ifrs-full_Revenue', 'ifrs_Revenue'])]
        df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['x_5179'], df['y_5179']), crs = 'EPSG:5179')
        df = df.rename(columns={'value': 'v', 'settleyyyy': 'yyyy'})
        return df

    def get_therehold_distance(self, target_df:gpd.GeoDataFrame(), data_point_df:gpd.GeoDataFrame()) -> float:
        ''' 
        임계거리 산출하는 함수. 주어진 공간 모수에 특정 feature에 대한 minimun threshold(최소 임계거리)를 구해야 한다. 
        :params target_df: 공간 모수를 나타내는 GeoDataFrame
        :params data_point_df: ex. bus, metro, corpfin
        :return float: threshold distance in kilometers
        '''
        target_sqmarea = target_df.area
        data_point_df['is_within_target'] = data_point_df.apply(lambda row: target_df.contains(row.geometry).any(), axis=1)
        number_of_data_within_target = data_point_df.is_within_target.sum()
        threshold_meter = np.sqrt(target_sqmarea/number_of_data_within_target)
        threshold = float(threshold_meter/1000) # threshold in kilometers
        return threshold

    def get_ddvs(self, base_point_df:gpd.GeoDataFrame(), data_point_df: gpd.GeoDataFrame(), threshold_distance: float)-> list:
        ''' 
        ddv : distance decayed value
        :params base_point_df: 
        :return gpd:base_point_df with mass added 
        '''
        # call data
        base_df = base_point_df.copy()
        data_df = data_point_df.copy()
        # define minimum weight
        min_weight = 0.01
        # set parameter
        values = []
        for i in tqdm(range(len(base_df))):
            base_point_xy = Point(base_df.iloc[i].x_5179, base_df.iloc[i].y_5179)
            data_df['dist'] = list(data_df.distance(base_point_xy)/1000) # distance in kilometers
            data_df['value_dd'] = data_df.apply(lambda row: distance_decay(row['v'], row['dist'], min_weight, threshold_distance), axis=1)
            sum_of_value_dd = sum(list(data_df.value_dd))
            values.append(sum_of_value_dd)
        return values

    def exec(self, feature_nms:list, stdr_yyyy:str):
        self._print_time(msg = f"[PnuGeoXyv]: Preprocess feature tables for experiment")
        self._print_time(msg = f"[PnuGeoXyv]: Distance decay - Add up value from data point to base point")
        self.stdr_yyyy = stdr_yyyy 
        
        for feature_nm in feature_nms:
            self._print_time(msg = f"[PnuGeoXyv]: @ {feature_nm}_{stdr_yyyy}")
            if feature_nm in ['metro', 'bus']:
                feature_df = self.pps_featuredb_busmetro(feature_nm, stdr_yyyy)

            elif feature_nm == 'corpfin':
                feature_df = self.pps_featuredb_corpfin(feature_nm, stdr_yyyy)

            else:
                raise Exception(f'no function for preprocessing feature named {feature_nm}. please check')    
            
            feature_threshold_distance = self.get_therehold_distance(self.target_sdgeo, feature_df)
            
            ddvs = self.get_ddvs(self.pnugeo, feature_df, feature_threshold_distance)
            self.pnugeo[feature_nm] = ddvs
            self.feature_configs.append((feature_nm, feature_threshold_distance))

        pnugeoxyv = gpd.GeoDataFrame(self.pnugeo, geometry=gpd.points_from_xy(self.pnugeo['x_5179'], self.pnugeo['y_5179']), crs = 'EPSG:5179')
        self.data_length = len(pnugeoxyv)
        self._print_time(msg = f"[PnuGeoXyv]: Save to {self.to_fpath}")
        try:
            if not os.path.exists(self.to_fpath):
                os.makedirs(self.to_fpath)
        except:
            pass
        pnugeoxyv.to_csv(os.path.join(self.to_fpath, f'pnugeoxyv_{self.stdr_yyyy}_{self.experiment_yyyymmdd}.csv'), sep = ',', encoding='euc-kr', index=False)
        del pnugeoxyv

    def save_config(self):
        self.feature_config_dict = {}
        for feature_nm, threshold_distance in self.feature_configs:
            self.feature_config_dict[feature_nm] = threshold_distance

        config = {
            'env': self.experiment_env,
            'yyyymmdd': self.experiment_yyyymmdd,
            'data_length': self.data_length,
            'completed_time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'feature_info': self.feature_config_dict
            }
        
        with open(os.path.join(self.to_fpath, f'pnugeoxyv_{self.stdr_yyyy}_{self.experiment_yyyymmdd}.json'), 'w') as f:
            json.dump(config, f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--env",
                        help="Environment that code is executed; either \"local\" or  \"dlabtest\"",
                        default="local")

    parser.add_argument("-d", "--yyyymmdd", 
                        help="Standard date. Default is today",
                        default=datetime.datetime.now().strftime('%Y%m%d'), type=str)

    args = parser.parse_args()

    pnugeoxyv = PnuGeoXyv(experiment_env = args.env, experiment_yyyymmdd = args.yyyymmdd)
    stdr_yyyymms = ['2018', '2019', '2020', '2021', '2022', '2023']
    feature_nms = ['metro', 'bus', 'corpfin']
    
    for stdr_yyyy in stdr_yyyymms:
        pnugeoxyv.exec(feature_nms, stdr_yyyy)

        pnugeoxyv.save_config()
    
    exit()
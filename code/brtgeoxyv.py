#-*- coding: utf-8 -*-
import warnings; warnings.filterwarnings('ignore')
import os, datetime, argparse, pytz, json, sys

import pandas as pd
import geopandas as gpd

from sqlalchemy import create_engine
from shapely import wkt
from shapely.geometry import Point
from dotenv import load_dotenv; load_dotenv("../env/.env")

sys.path.append('../');sys.path.append('../../')
from util.epsg import *
from util.distance import *
from util.comm import *

class BrtGeoXyv:
    '''
    Get BrtGeoXyv Data For Experiment
    '''
    def __init__(self, experiment_env, experiment_yyyymmdd):
        self._print_time(msg = "Class Init")
        print(f"[BrtGeoXyv]: BrtGeoXyv Init")
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
        
        self.to_fpath = f'../asset/experiment/brtgeoxyv/'
        self.s3_bucket_name = 'dlab-ds-dataset'

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

    def convert_dot_zero_to_str(self, value):
        ''' 
        1223.0 -> '1223'
        '''
        try:
            # 소수점을 찾고, 그 앞에 있는 문자열만 추출합니다.
            int_value = int(value)
            return str(int_value)
        except ValueError:
            # 변환 중에 에러가 발생하면 None을 반환합니다.
            return None

    def pps_featuredb_salary(self, feature_nm:str, stdr_yyyy:str) -> pd.DataFrame():
        sql_query = f"SELECT * FROM featuredb.{feature_nm}yrbasec WHERE stdryyyy like '{stdr_yyyy}%%';"
        df = pd.read_sql_query(sql_query, self.engine)
        return df

    def exec(self, stdr_yyyy:str):
        self._print_time(msg = f"[BrtGeoXyv]: Extract brtgeo_{stdr_yyyy}")
        # brtgeo_path = f'../asset/experiment/brtgeo/brtgeo_{self.experiment_yyyymmdd}.csv'
        # brtgeo = pd.read_csv(brtgeo_path, encoding = 'euc-kr', dtype = {'baseccd': str})
        brtgeo = read_from_aws_as_df(
            bucket_name = self.s3_bucket_name,
            from_fname = f'brtgeo_{self.experiment_yyyymmdd}.csv',
            encoding = 'euc-kr'
        )



        reference_date = pd.to_datetime(f'{stdr_yyyy}1231', format='%Y%m%d')
        brtgeo['vintageyr'] = (reference_date - pd.to_datetime(brtgeo['useaprday'], format='%Y%m%d', dayfirst=True)).dt.days / 365 # vintage 생성(만, 년도 기준)dd
        brtgeo = brtgeo[brtgeo['vintageyr'] > 0]
        
        geometry = [Point(xy) for xy in zip(brtgeo['x'], brtgeo['y'])]
        brtgeo = gpd.GeoDataFrame(brtgeo, geometry=geometry, crs='EPSG:4326')
        self.brtgeo = add_coord(brtgeo)
        self.brtgeo = self.brtgeo[~self.brtgeo['baseccd'].isin(['inf', np.nan])]
        self.brtgeo['baseccd'] = self.brtgeo['baseccd'].apply(lambda x: str(int(x)).zfill(5))


        self._print_time(msg = f"[BrtGeoXyv]: Extract pnugeoxyv_{stdr_yyyy}")
        pnugeoxyv_path = f'../asset/experiment/pnugeoxyv/pnugeoxyv_{stdr_yyyy}_{self.experiment_yyyymmdd}.csv'
        pnugeoxyv = pd.read_csv(pnugeoxyv_path, encoding = 'euc-kr', dtype = {'baseccd': str})

        self._print_time(msg = f"[BrtGeoXyv]: Add Salary Data")
        feature_nm = 'salary'
        feature_df = self.pps_featuredb_salary(feature_nm, stdr_yyyy) # salary
        feature_df['baseccd'] = feature_df['baseccd'].apply(lambda x: str(x).zfill(5))
        brtgeov = pd.merge(self.brtgeo, feature_df[['baseccd', 'basecavgpensionamt']], on='baseccd', how='left')


        brtgeov['baseccd'] = brtgeov['baseccd'].apply(lambda x: str(x).zfill(5))


        self._print_time(msg = f"[BrtGeoXyv]: And Add metro, bus, corpfin in pnugeoxyv that matches pnu")
        feature_nms = ['jibunaddr', 'metro', 'bus', 'corpfin']
        use_cols = feature_nms + ['pnu']
        brtgeoxyv = pd.merge(brtgeov, pnugeoxyv[use_cols], on='pnu', how='left')
        brtgeoxyv = brtgeoxyv.rename(columns={'basecavgpensionamt': 'salary'})


        ilp_cols = [col for col in brtgeoxyv.columns if col.startswith('ilp')]
        for ilp_col in ilp_cols:
            brtgeoxyv[ilp_col].fillna(0, inplace=True)
            brtgeoxyv[ilp_col] = brtgeoxyv[ilp_col].apply(lambda x: int(x))

        str_cols = ['rlgcd', 'sggcd', 'hjdstatcd', 'bjdcd']
        for str_col in str_cols:
            brtgeoxyv[str_col] = brtgeoxyv[str_col].apply(self.convert_dot_zero_to_str)


        brtgeoxyv['baseccd'] = brtgeoxyv['baseccd'].apply(lambda x: str(x).zfill(5))

        self._print_time(msg = f"[BrtGeoXyv]: Dataset Preparation Complete")
        self.data_length = len(brtgeoxyv)
        self._print_time(msg = f"[BrtGeoXyv]: Save to {self.to_fpath}")
        try:
            if not os.path.exists(self.to_fpath):
                os.makedirs(self.to_fpath)
        except:
            pass
        brtgeoxyv.to_csv(os.path.join(self.to_fpath, f'brtgeoxyv_{stdr_yyyy}_{self.experiment_yyyymmdd}.csv'), sep = ',', encoding='euc-kr', index=False)

        self._print_time(msg = f"[BrtGeoXyv]: Save to AWS Bucket named {self.s3_bucket_name}")
        save_to_aws(
            bucket_name = self.s3_bucket_name,
            from_fpname = os.path.join(self.to_fpath, f'brtgeoxyv_{stdr_yyyy}_{self.experiment_yyyymmdd}.csv'),
            to_fpname = f'brtgeoxyv_{stdr_yyyy}_{self.experiment_yyyymmdd}.csv',
            keep_file = True
            )
        
        del brtgeoxyv

    def exec_ts(self, stdr_yyyys):
        brtgeoxyv_ts = pd.DataFrame()
        self._print_time(msg = f"[BrtGeoXyv]: Start Creating ts(timeseries) data")
        for stdr_yyyy in stdr_yyyys:
            self._print_time(msg = f"[BrtGeoXyv]: @{stdr_yyyy}->Cumulate rows until {stdr_yyyy}")
            brtgeoxyv = read_from_aws_as_df(
                bucket_name = self.s3_bucket_name,
                from_fname = f'brtgeoxyv_{stdr_yyyy}_{self.experiment_yyyymmdd}.csv',
                encoding = 'euc-kr'
            )

            ilp_cols = [col for col in brtgeoxyv.columns if col.startswith('ilp')]
            for ilp_col in ilp_cols:
                brtgeoxyv[ilp_col].fillna(0, inplace=True)
                brtgeoxyv[ilp_col] = brtgeoxyv[ilp_col].apply(lambda x: int(x))

            str_cols = ['rlgcd', 'sggcd', 'hjdstatcd', 'bjdcd']
            for str_col in str_cols:
                brtgeoxyv[str_col] = brtgeoxyv[str_col].apply(self.convert_dot_zero_to_str)
            
            brtgeoxyv['baseccd'] = brtgeoxyv['baseccd'].apply(lambda x: str(x).zfill(5))

            geometry = [Point(xy) for xy in zip(brtgeoxyv['x'], brtgeoxyv['y'])]
            brtgeoxyv = gpd.GeoDataFrame(brtgeoxyv, geometry=geometry, crs='EPSG:4326')

            # sum distance decayed value of 'metro' and 'bus' and name it as 'busmetro'
            brtgeoxyv['busmetro'] = brtgeoxyv['bus'] + brtgeoxyv['metro']
            brtgeoxyv = brtgeoxyv[~brtgeoxyv.jibunaddr.isnull()]
            drop_cols = [col for col in brtgeoxyv.columns if col.startswith('ilp') and not col.endswith(str(stdr_yyyy))]
            brtgeoxyv.drop(columns=drop_cols, inplace=True)
            brtgeoxyv.rename(columns={f'ilp{stdr_yyyy}': 'ilp'}, inplace=True)
            brtgeoxyv['stdr_yyyy'] = stdr_yyyy
            brtgeoxyv_ts = pd.concat([brtgeoxyv_ts, brtgeoxyv])
            del brtgeoxyv

        self._print_time(msg = f"[BrtGeoXyv]: Save to {self.to_fpath}")

        try:
            if not os.path.exists(self.to_fpath):
                os.makedirs(self.to_fpath)
        except:
            pass
        brtgeoxyv_ts.to_csv(os.path.join(self.to_fpath, f'brtgeoxyvts_{self.experiment_yyyymmdd}.csv'), sep = ',', encoding='euc-kr', index=False)
        self._print_time(msg = f"[BrtGeoXyv]: Save to AWS Bucket named {self.s3_bucket_name}")
        save_to_aws(
            bucket_name = self.s3_bucket_name,
            from_fpname = os.path.join(self.to_fpath, f'brtgeoxyvts_{self.experiment_yyyymmdd}.csv'),
            to_fpname = f'brtgeoxyvts_{self.experiment_yyyymmdd}.csv',
            keep_file = True
            )        
        del brtgeoxyv_ts

    def save_config(self):
        config = {
            'env': self.experiment_env,
            'yyyymmdd': self.experiment_yyyymmdd,
            'data_length': self.data_length,
            'completion_time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        
        with open(os.path.join(self.to_fpath, f'brtgeoxyv_{self.experiment_yyyymmdd}.json'), 'w') as f:
            json.dump(config, f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--env",
                        help="Environment that code is executed; either \"local\" or  \"dlabtest\"",
                        default="local")

    parser.add_argument("-d", "--yyyymmdd", 
                        help="Standard date(Utilize for calculating vinatage of building(column name: vintageyr)), Default is today",
                        default=datetime.datetime.now().strftime('%Y%m%d'), type=str)

    parser.add_argument("-ts", "--create_ts", 
                        action = "store_true",
                        help="Add -ts. if ts(timeseries) data should be created.\
                            timeseries data is cumulative data for every standard years\
                                (ex. 2018 ~ 2023, 6 years concatenated data)"
                                )
    args = parser.parse_args()
    brtgeoxyv = BrtGeoXyv(experiment_env = args.env, experiment_yyyymmdd = args.yyyymmdd)
    stdr_yyyys = ['2018', '2019', '2020', '2021', '2022', '2023']
    for stdr_yyyy in stdr_yyyys:
        brtgeoxyv.exec(stdr_yyyy)
        brtgeoxyv.save_config()  

    if args.create_ts: # execute if you want to preprocess and save timeseries cumulated data
        brtgeoxyv.exec_ts(stdr_yyyys)

    exit()
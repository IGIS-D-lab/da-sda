#-*- coding: utf-8 -*-
import warnings; warnings.filterwarnings('ignore')
import os, datetime, argparse, pytz, json, sys
import inspect

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

class BdScoring:
    ''' 
    For given dataset(ts: time series, data concated for multiple years), using nominal value of features,
    we 1) standardize data(min max scaler) 2) rank data(from 1 to 100) 3) score the data according to equation
    '''
    def __init__(self, experiment_env, experiment_yyyymmdd):
        self._print_time(msg = "Class Init")
        print(f"[BdScoring]: BdScoring Init")
        self.experiment_env = experiment_env
        self.experiment_yyyymmdd = experiment_yyyymmdd
        self.s3_bucket_name = 'dlab-ds-dataset'
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

        elif self.experiment_env == 'dlabetlrds':
            dbid = os.environ.get('ID_ETLRDS')
            dbpw = os.environ.get('PW_ETLRDS')
            dbhost = os.environ.get('HOST_ETLRDS')
            dbport = os.environ.get('PORT_ETLRDS')
            dbdbname = os.environ.get('DBNAME_ETLRDS')
            self.engine = create_engine(f"postgresql://{dbid}:{dbpw}@{dbhost}:{dbport}/{dbdbname}")
        else:
            raise Exception('-e should be either \"local\" or \"dlabtest\" or \"dlabetlrds\". Please Check')
        
        self.to_fpath = f'../asset/experiment/bdscoring/'
        self.s3_bucket_name = 'dlab-ds-dataset'
        self.tbnm = 'bds'

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

    def min_max_scaler(self, x, min_v, max_v):
        x = (x - min_v) / (max_v - min_v)
        x = x*100
        return min(99, max(1, x))

    def standardize_df(self, df:pd.DataFrame(), target_cols:list):
        ''' 
        standardize df, min max scaler with return value from 1 to 99 is applied
        :params df: dataframe of which target columns to be standardized
        :return None
        '''
        for target_col in target_cols:
            min_v = df[target_col].min()
            max_v = df[target_col].max()
            if target_col == 'vintageyr':
                df[f'{target_col}mms'] = df[target_col].apply(lambda x: 100-self.min_max_scaler(x, min_v, max_v))
            else:
                df[f'{target_col}mms'] = df[target_col].apply(lambda x:self.min_max_scaler(x, min_v, max_v))
        return ...

    def rank_df(self, df:pd.DataFrame(), target_cols:list):
        ''' 
        rank df. rank function with return value from 1 to 100
        (where value near 1 is below expectation, near 100 is above expectation)
        :params df: dataframe of which target columns to be ranked
        :return None
        '''
        for target_col in target_cols:
            if target_col == 'vintageyr':
                df[f'{target_col}rank'] = 100 - df[target_col].rank(pct=True, method='max') * 100
            else:
                df[f'{target_col}rank'] = df[target_col].rank(pct=True, method='max') * 100
        return ...

    def label_bdnm_df(self, df:pd.DataFrame()):
        ''' 
        label bd nm(cbd, gbd, ybd)
        '''
        df.loc[df['sggnm'].isin(['종로구', '중구']), 'district_nm'] = 'cbd'
        df.loc[df['sggnm'].isin(['강남구', '서초구']), 'district_nm'] = 'gbd'
        df.loc[df['jibunaddr'].str.startswith('서울특별시 영등포구 여의도동'), 'district_nm'] = 'ybd'
        return ...

    def bds(self, df:pd.DataFrame(), calc_cols_beta_dict):
        func_nm = inspect.currentframe().f_code.co_name # func_nm = min_max_scaler
        calc_mms_cols = [f'{calc_col}mms' for calc_col in calc_cols_beta_dict.keys()]
        missing_cols = [col for col in calc_mms_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing columns: {', '.join(missing_cols)}, Please execute df_standardize first.")
        else:
            df[func_nm] = df.apply(lambda row: np.prod([row[f'{calc_col}mms']**beta for calc_col, beta in calc_cols_beta_dict.items()]), axis=1).rank(pct=True, method='max') * 100
        return ...

    def exec(self, target_cols:list, calc_cols_beta_dict:dict, stdr_yyyys:list):
        self._print_time(msg = f"[BdScoring]: Extract brtgeoxyvts")
        brtgeoxyvts = read_from_aws_as_df(
            bucket_name = self.s3_bucket_name,
            from_fname = f'brtgeoxyvts_{self.experiment_yyyymmdd}.csv',
            encoding = 'euc-kr'
        )
        # print('1', len(brtgeoxyvts))
        # brtgeoxyvts = brtgeoxyvts[~brtgeoxyvts['ilp'].isin(['inf', np.nan])]
        # print('2', len(brtgeoxyvts))
        # print(brtgeoxyvts.columns)
        # brtgeoxyvts['ilp'] = brtgeoxyvts['ilp'].astype(int)
        # 1) standardize 2) rank 3) label bd nm
        brtgeoxyvts['baseccd'] = brtgeoxyvts['baseccd'].apply(lambda x: str(int(x)).zfill(5))
        print('AAA')
        print(brtgeoxyvts.iloc[0].baseccd)
        print(type(brtgeoxyvts.iloc[0].baseccd))

        self._print_time(msg = f"[BdScoring]: Minmax scale, Rank data")
        df_ts = pd.DataFrame()
        for stdr_yyyy in stdr_yyyys:
            df = brtgeoxyvts[brtgeoxyvts['stdr_yyyy'] == int(stdr_yyyy)]
            df = df.dropna(subset=['mainpurpscdnms'])
            df = df[df['mainpurpscdnms'].str.contains('업무시설')]
            print(f'Year: {stdr_yyyy} - Total PNU Count: {len(df)}')
            self.standardize_df(df, target_cols)
            self.rank_df(df, target_cols)
            self.label_bdnm_df(df)
            df_ts = pd.concat([df_ts, df])
            del df
        # 4) bd scoring
        self._print_time(msg = f"[BdScoring]: Bd Scoring")
        df_bds_ts = pd.DataFrame()
        for stdr_yyyy in stdr_yyyys:
            df = df_ts[df_ts['stdr_yyyy'] == int(stdr_yyyy)]
            self.bds(df, calc_cols_beta_dict)
            df_bds_ts = pd.concat([df_bds_ts, df])
            del df

        self._print_time(msg = f"[BdScoring]: Save to {self.to_fpath}")
        try:
            if not os.path.exists(self.to_fpath):
                os.makedirs(self.to_fpath)
        except:
            pass
        self.df_bds_ts = df_bds_ts.copy()
        df_bds_ts.to_csv(os.path.join(self.to_fpath, f'bds_{self.experiment_yyyymmdd}.csv'), sep = ',', encoding='euc-kr', index=False)
        print('len(df_bds_ts)', len(df_bds_ts))
        self._print_time(msg = f"[BdScoring]: Save to AWS Bucket named {self.s3_bucket_name}")
        save_to_aws(
            bucket_name = self.s3_bucket_name,
            from_fpname = os.path.join(self.to_fpath, f'bds_{self.experiment_yyyymmdd}.csv'),
            to_fpname = f'bds_{self.experiment_yyyymmdd}.csv',
            keep_file = True
            )
        del brtgeoxyvts
        del df_ts
        del df_bds_ts

    def load_to_db(self):
        # crs = 'EPSG:4326'  # Specify the coordinate reference system
        geometry = [Point(xy) for xy in zip(self.df_bds_ts['x'], self.df_bds_ts['y'])]        
        self.df_bds_ts = gpd.GeoDataFrame(self.df_bds_ts, geometry=geometry, crs = 'EPSG:4326')
        #############################################################
        
        self.df_bds_ts.to_postgis(self.tbnm, self.engine, schema = "pantera", if_exists='append', chunksize=1000)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--env",
                        help="Environment that code is executed; either \"local\" or  \"dlabtest\" or \"dlabetlrds\"",
                        default="local")
    parser.add_argument("-d", "--yyyymmdd", 
                        help="Standard date(Utilize for calculating vinatage of building(column name: vintageyr)), Default is today",
                        default=datetime.datetime.now().strftime('%Y%m%d'), type=str)

    parser.add_argument("-l", "--loadtodb", 
                        action = "store_true",
                        help="Add -l if data should be loaded to db")


    args = parser.parse_args()
    bds = BdScoring(experiment_env = args.env, experiment_yyyymmdd = args.yyyymmdd)

    stdr_yyyys = ['2018', '2019', '2020', '2021', '2022', '2023']
    target_cols = [
        'totarea',
        'ilp',
        'vintageyr',
        'salary',
        'metro',
        'bus',
        'corpfin',
        'busmetro'
        ]
    calc_cols_beta_dict = {
        'salary': 1,
        'busmetro': 1,
        'corpfin': 1,
        'ilp': 1,
        'totarea': 1,
        'vintageyr': 1
    }
    bds.exec(target_cols, calc_cols_beta_dict, stdr_yyyys)

    if args.loadtodb:
        bds.load_to_db()

    exit()
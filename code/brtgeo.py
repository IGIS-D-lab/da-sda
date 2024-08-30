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
from util.comm import *

def create_ppsbldnm(row):
    ''' 
    개요: 빌딩명 + 동명을 합한 빌딩동명을 반환하는 함수. 
    보충설명: 다른 동의 빌딩은 분명 다른 데이터다. 즉 서울 국제금융 센터 쓰리 아이에프씨동, 서울 국제금융 센터 투 아이에프씨동 은 다른 건물이다.
    보충설명: 빌딩명 + 동명을 합해서 그 고유값들을 관리하며 다른 빌딩동을 특정하기 위하여 사용한다.
    : param row: df의 행(가로 방향)
    : return blddongnm: 빌딩명 + ' ' + 동명 
    '''
    bldnm = str(row['bldnm']).strip()  
    dongnm = str(row['dongnm']).strip()
    bldsigungunm = str(row['bldsigungunm']).strip()

    bldnm_bool = True if len(bldnm) > 0 else False
    dongnm_bool = True if len(dongnm) > 0 else False
    bldsigungunm_bool = True if len(bldsigungunm) > 0 else False

    #1
    if bldnm_bool and dongnm_bool and bldsigungunm_bool:
        return str(bldnm + ' ' + dongnm).strip().replace("\"", "").replace("\'", "")
    #2
    elif bldnm_bool and dongnm_bool and not bldsigungunm_bool:
        return str(bldnm + ' ' + dongnm).strip().replace("\"", "").replace("\'", "")
    
    #3
    elif not bldnm_bool and dongnm_bool and bldsigungunm_bool:
        return str(bldsigungunm + ' ' + dongnm).strip().replace("\"", "").replace("\'", "")

    #4
    elif bldnm_bool and not dongnm_bool and bldsigungunm_bool:
        return bldnm
    
    #5
    elif bldnm_bool and not dongnm_bool and not bldsigungunm_bool:
        return bldnm

    # 6
    elif not bldnm_bool and dongnm_bool and not bldsigungunm_bool:
        return dongnm

    # 7
    elif not bldnm_bool and not dongnm_bool and bldsigungunm_bool:
        return bldsigungunm
    
    else:
        return None

def get_valid_date(date_str, experiment_yyyymmdd):
    try:
        # 날짜로 변환 시도
        date = datetime.datetime.strptime(date_str, '%Y%m%d')
        # 날짜가 유효한지 확인
        if date.strftime('%Y%m%d') == date_str:
            if date_str < '19000101':
                date_str = '19000101'
            elif date_str >= experiment_yyyymmdd:
                date_str = experiment_yyyymmdd
            else:
                pass
            return date_str
        else:
            return None
    except ValueError:
        return None

# 'useaprday' 값의 맨 끝자리가 '00'인 경우 '01'로 변환하는 람다 함수 정의
def get_useaprday_yyyymmdd(useaprday, experiment_yyyymmdd):
    ''' 
    개요: brtitle의 useaprday는 string formate으로서 'yyyymmdd' 형태를 띈다 ex. '20240101, 20230915' 하지만, 해당 포맷(ex. '2015')에 맞지 않아 전처리가 필요한 경우 당 함수를 사용한다
    '''
    if len(useaprday) == 8:
        if useaprday.endswith('00'):
            useaprday =  useaprday[:-2] + '01'
        elif useaprday.endswith('0000'):
            useaprday = useaprday[:-4] + '0101'
        else:
            pass
        return get_valid_date(useaprday, experiment_yyyymmdd)
    
    elif len(useaprday) == 6:
        useaprday = useaprday + '01'
        return get_valid_date(useaprday, experiment_yyyymmdd)
    
    elif len(useaprday) == 4:
        useaprday = useaprday + '0101'
        return get_valid_date(useaprday, experiment_yyyymmdd)
    
    else:
        return None

def join_non_empty_strings(values:list()) -> str:
    valid_strs = [value for value in values if pd.notna(value) and value != '' and value is not None]
    valid_strs = list(set(valid_strs)) # 중복값제거
    return ', '.join(valid_strs)

class BrtGeo:
    '''
    Get BrtGeo Data For Experiment
    '''
    def __init__(self, experiment_env, experiment_yyyymmdd):
        self._print_time(msg = "Class Init")
        print(f"[BrtGeo]: BrtGeo Init")
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
        
        self.to_fpath = f'../asset/experiment/brtgeo/'
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
    
    def exec(self):
        self._print_time(msg = "[BrtGeo]: Get brtitle, sd, sdgeo")
        # get brt(mview:bddb.sdabrt_seoul) + convert to gdf(using geopandas)
        sql_query = "SELECT * FROM bddb.sdabrt_seoul;" 
        sdabrt = pd.read_sql_query(sql_query, self.engine)
        sdabrt = sdabrt.drop_duplicates()
        bld = sdabrt[['pnu', 'platplc', 'x', 'y']]
        geometry = [Point(xy) for xy in zip(bld['x'], bld['y'])]
        bld = gpd.GeoDataFrame(bld, geometry=geometry, crs='EPSG:4326')

        # get sdsdgeo(mview:geodb.sdsdgeo_seoul) + convert to gdf(using geopandas)
        sql_query = "SELECT *, ST_AsText(geometry) as geom_wkt FROM geodb.sdsdgeo_seoul;"
        sdsdgeo = pd.read_sql_query(sql_query, self.engine)
        sdsdgeo['geometry'] = sdsdgeo['geom_wkt'].apply(wkt.loads)
        sdsdgeo.drop(columns=['geom_wkt'], inplace=True)
        sdgeo = gpd.GeoDataFrame(sdsdgeo, geometry='geometry')
        sdgeo.crs = 'EPSG:4326'
        basecgeo = sdgeo[sdgeo['sdgb'] == 'basec']

        # map basec for given x, y in bld
        intersect = gpd.overlay(bld, basecgeo, how='intersection')
        intersect['area'] = intersect.geometry.area
        max_intersect_area_idx = intersect.groupby('pnu')['area'].idxmax()
        intersect = intersect.loc[max_intersect_area_idx]
        intersect = intersect[['pnu', 'platplc', 'x', 'y', 'baseccd', 'sdsqmarea',
            'rlgnm', 'rlgcd', 'sggcd', 'sggnm', 'hjdstatcd', 'hjdnm', 'bjdcd',
            'bjdnm', 'geometry']]
        intersect.rename(columns={'sdsqmarea': 'basecsqmarea', 'geometry': 'bld_geometry'}, inplace=True)
        intersect = gpd.GeoDataFrame(intersect, geometry='bld_geometry', crs='EPSG:4326')
        intersect = intersect.merge(basecgeo[['baseccd', 'geometry']], on='baseccd', how='left')
        intersect.rename(columns={'geometry': 'basec_geometry'}, inplace=True)
        bldgeo = intersect.drop_duplicates() 

        self._print_time(msg = "[BrtGeo]: Preprocess")
        sdabrt['ppsbldnm'] = sdabrt.apply(lambda row: create_ppsbldnm(row), axis=1) # 빌딩명+동명 조합어 생성

        use_cols = ['pnu', 'ppsbldnm', 'mainpurpscd', 'mainpurpscdnm', 'etcpurps',\
                    'platplc', 'totarea', 'jijigucdnm', 'useaprday', 'x', 'y']
        ilp_cols = [col for col in sdabrt.columns if col.startswith('ilp')]
        use_cols += ilp_cols
        sdabrt = sdabrt[use_cols]
        sdabrt = sdabrt.drop_duplicates()
        sdabrt = sdabrt.drop_duplicates(subset=sdabrt.columns.difference(['jijigucdnm']), keep='first')
        sdabrt['useaprday'] = sdabrt['useaprday'].apply(lambda x: get_useaprday_yyyymmdd(x, self.experiment_yyyymmdd))
        not_null_cols = ['totarea', 'useaprday', 'x', 'y']

        for ilp_col in ilp_cols:
            sdabrt[ilp_col].fillna(0, inplace=True)
            sdabrt[ilp_col] = sdabrt[ilp_col].apply(lambda x: int(x))

        for col in not_null_cols:
            sdabrt = sdabrt[~((sdabrt[col].isna()) | (sdabrt[col] == 0.0) | (sdabrt[col].isnull()))]
        # reference_date = pd.to_datetime(self.experiment_yyyymmdd, format='%Y%m%d', dayfirst=True)
        # sdabrt['vintageyr'] = (reference_date - pd.to_datetime(sdabrt['useaprday'], format='%Y%m%d', dayfirst=True)).dt.days / 365 # vintage 생성(만, 년도 기준)dd

        '''
        sdabrt -> sdabrt_pnu
        sdabrt: 건축물대장 표제부 1건 -> 1행으로 이루어짐. 즉 같은 pnu를 가진 여러행이 존재함. 보통 1개 필지에 여러개동의 건물이 있는 경우에 해당함
        sdabrt_pnu: pnu 1건 -> 1행으로 이루어지도록 brt로부터 가공된 데이터

        'pnu', 'blddongnm', 'mainpurpscd', 'platplc', 'totarea', 'jijigucdnm',
            f'ilp{yyyy}', 'useaprday', 'x', 'y', 'vintageyr'

        1) 같은 pnu를 가진 여러행 묶음을 찾는다. 이 여러행을 1개의 행으로 바꾸는 것이 목표이다
        2) 묶음 중 아래와 같이 1개 행으로 바꾼다
        2-1) totarea 내림차순 정리 후 첫번째 행 값만 남기는 열: pnu, blddongnm, mainpurpscd, platplc, ilp2022, useaprday, x, y, vintageyr
        2-2) totarea열: 묶음의 totarea의 합값으로 바꾼다, elct2022열: 묶음중 큰 값으로 바꾼다.
        2-3) 새로 생성하는 열
        2-3-1) blddongnms: blddongnm을 ','로 연결하여 만든 string 반영
        2-3-2) cnt: 묶음 데이터의 갯수. 즉 같은 pnu를 가진 행이 4개 행이었으면 4

        즉, 
        'pnu', 'blddongnm', 'mainpurpscd', 'platplc', 'totarea', 'jijigucdnm',
            f'ilp{yyyy}', 'useaprday', 'x', 'y', 'vintageyr', 'blddongnms', 'cnt' 열을 가지는 brt를 만든다.
        '''
        self._print_time(msg = "[BrtGeo]: Make Brt data grouped by PNU")
        sdabrt_pnu = sdabrt.copy()
        grouped = sdabrt_pnu.groupby('pnu')
        sdabrt_pnu['bldcnt'] = grouped['pnu'].transform('count')
        aggregate_dict = {
            'ppsbldnm': lambda x: join_non_empty_strings(x),
            'bldcnt': 'first',
            'mainpurpscd': 'first',
            'mainpurpscdnm': lambda x: join_non_empty_strings(x),
            'etcpurps': lambda x: join_non_empty_strings(x),
            'platplc': 'first',
            'useaprday': 'first',
            'x': 'first',
            'y': 'first',
            'totarea': 'sum',
            'jijigucdnm': lambda x: join_non_empty_strings(x),
        }
        for ilp_col in ilp_cols:
            aggregate_dict[ilp_col] = 'first'
        sdabrt_pnu = grouped.agg(aggregate_dict).reset_index()

        sdabrt_pnu.rename(columns={'ppsbldnm': 'ppsbldnms'}, inplace=True)
        sdabrt_pnu.rename(columns={'jijigucdnm': 'jijigucdnms'}, inplace=True)
        sdabrt_pnu.rename(columns={'mainpurpscdnm': 'mainpurpscdnms'}, inplace=True)
        sdabrt_pnu.rename(columns={'etcpurps': 'etcpurpss'}, inplace=True)

        use_cols = ['pnu', 'bldcnt', 'ppsbldnms', 'mainpurpscd', 'mainpurpscdnms', 'etcpurpss', 'platplc',\
                    'totarea', 'jijigucdnms', 'useaprday', 'x', 'y'] #, 'vintageyr']
        ilp_cols = [col for col in sdabrt.columns if col.startswith('ilp')]
        use_cols += ilp_cols
        sdabrt_pnu = sdabrt_pnu[use_cols]

        self._print_time(msg = "[BrtGeo]: Merge-> Brt + Geo")
        sdabrtgeo = pd.merge(
            sdabrt_pnu[['pnu', 'bldcnt', 'ppsbldnms', 'mainpurpscd',  'mainpurpscdnms', 'etcpurpss', 'totarea', 'jijigucdnms', 'useaprday'] + ilp_cols],
            bldgeo[['pnu', 'platplc', 'baseccd', 'basecsqmarea', 'rlgnm', 'rlgcd',
                'sggcd', 'sggnm', 'hjdstatcd', 'hjdnm', 'bjdcd', 'bjdnm',
                'x', 'y']],
            on = 'pnu',
            how = 'left'
        )
        
        sdabrtgeo = sdabrtgeo[sdabrtgeo['pnu'] != '1132010800106290006'] # 1132010800106290006	1	한밭법조타워	14000	업무시설	업무시설	5205721.00	-> 오류(연면적 520만제곱) 데이터 행 삭제
        sdabrtgeo = sdabrtgeo[sdabrtgeo['pnu'] != '1153010800100090035'] # 1153010800100090035	1	예성타워	14000	업무시설	업무시설(오피스텔)	1471845.0	-> 오류(연면적 147만제곱) 데이터 행 삭제


        self.data_length = len(sdabrtgeo)
        self._print_time(msg = f"[BrtGeo]: Save to {self.to_fpath}")
        try:
            if not os.path.exists(self.to_fpath):
                os.makedirs(self.to_fpath)
        except:
            pass
        sdabrtgeo.to_csv(os.path.join(self.to_fpath, f'brtgeo_{self.experiment_yyyymmdd}.csv'), sep = ',', encoding='euc-kr', index=False)
        self._print_time(msg = f"[BrtGeo]: Save to AWS Bucket named {self.s3_bucket_name}")
        save_to_aws(
            bucket_name = self.s3_bucket_name,
            from_fpname = os.path.join(self.to_fpath, f'brtgeo_{self.experiment_yyyymmdd}.csv'),
            to_fpname = f'brtgeo_{self.experiment_yyyymmdd}.csv',
            keep_file = True
            )

        self._print_time(msg = f"[BrtGeo]: Complete")
        del sdabrtgeo 

    def save_config(self):
        config = {
            'env': self.experiment_env,
            'yyyymmdd': self.experiment_yyyymmdd,
            'data_length': self.data_length,
            'completion_time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        
        with open(os.path.join(self.to_fpath, f'brtgeo_{self.experiment_yyyymmdd}.json'), 'w') as f:
            json.dump(config, f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--env",
                        help="Environment that code is executed; either \"local\" or  \"dlabtest\"",
                        default="local")

    parser.add_argument("-d", "--yyyymmdd", 
                        help="Standard date, Default is today",
                        default=datetime.datetime.now().strftime('%Y%m%d'), type=str)

    args = parser.parse_args()
    brtgeo = BrtGeo(experiment_env = args.env, experiment_yyyymmdd = args.yyyymmdd)
    brtgeo.exec()
    brtgeo.save_config()
    exit()
#-*- coding: utf-8 -*-
import warnings; warnings.filterwarnings('ignore')
import os, datetime, argparse, pytz, json

import pandas as pd
import geopandas as gpd

from sqlalchemy import create_engine
from shapely import wkt
from shapely.geometry import Point
from dotenv import load_dotenv; load_dotenv("../env/.env")

class PnuGeo:
    '''
    Get PnuGeo Data For Experiment
    '''
    def __init__(self, experiment_env, experiment_yyyymmdd):
        self._print_time(msg = "Class Init")
        print(f"[PnuGeo]: PnuGeo Init")
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

        self.to_fpath = f'../asset/experiment/pnugeo/'

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
        self._print_time(msg = "[PnuGeo]: Get jibunaddress, sd, sdgeo")
        sql_query = "SELECT * FROM bddb.jibunaddress WHERE rlgcd = '11';"
        j = pd.read_sql_query(sql_query, self.engine)
        j = j.drop_duplicates()
        j = j[j['bchk']!= '5']
        use_cols = ['pnu', 'jibunnm', 'x', 'y', 'jibunaddr']
        j = j[use_cols]
        geometry = [Point(xy) for xy in zip(j['x'], j['y'])]
        j = gpd.GeoDataFrame(j, geometry=geometry, crs='EPSG:4326')

        # get sdsdgeo(mview:geodb.sdsdgeo_seoul) + convert to gdf(using geopandas)
        sql_query = "SELECT *, ST_AsText(geometry) as geom_wkt FROM geodb.sdsdgeo_seoul;"
        sdsdgeo = pd.read_sql_query(sql_query, self.engine)
        sdsdgeo['geometry'] = sdsdgeo['geom_wkt'].apply(wkt.loads)
        sdsdgeo.drop(columns=['geom_wkt'], inplace=True)
        sdgeo = gpd.GeoDataFrame(sdsdgeo, geometry='geometry')
        sdgeo.crs = 'EPSG:4326'
        basecgeo = sdgeo[sdgeo['sdgb'] == 'basec']

        self._print_time(msg = "[PnuGeo]: Merge-> Pnu + Geo")
        # map basec for given x, y in bld
        intersect = gpd.overlay(j, basecgeo, how='intersection')
        intersect = intersect[['pnu', 'jibunaddr', 'x', 'y', 'baseccd', 'sdsqmarea',
            'rlgnm', 'rlgcd', 'sggcd', 'sggnm', 'hjdstatcd', 'hjdnm', 'bjdcd',
            'bjdnm', 'geometry']]        
        intersect.rename(columns={'sdsqmarea': 'basecsqmarea', 'geometry': 'pnu_geometry'}, inplace=True)
        intersect = gpd.GeoDataFrame(intersect, geometry='pnu_geometry', crs='EPSG:4326')
        intersect = intersect.merge(basecgeo[['baseccd', 'geometry']], on='baseccd', how='left')
        intersect.rename(columns={'geometry': 'basec_geometry'}, inplace=True)
        pnugeo = intersect.drop_duplicates() 
        pnugeo.drop(columns=['pnu_geometry', 'basec_geometry'], inplace=True)
        self.data_length = len(pnugeo)
        self._print_time(msg = f"[PnuGeo]: Save to {self.to_fpath}")
        try:
            if not os.path.exists(self.to_fpath):
                os.makedirs(self.to_fpath)
        except:
            pass
        pnugeo.to_csv(os.path.join(self.to_fpath, f'pnugeo_{self.experiment_yyyymmdd}.csv'), sep = ',', encoding='euc-kr', index=False)
        self._print_time(msg = f"[PnuGeo]: Complete")
        del pnugeo 

    def save_config(self):
        config = {
            'env': self.experiment_env,
            'yyyymmdd': self.experiment_yyyymmdd,
            'data_length': self.data_length,
            'completed_time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        
        with open(os.path.join(self.to_fpath, f'pnugeo_{self.experiment_yyyymmdd}.json'), 'w') as f:
            json.dump(config, f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--env",
                        help="Environment that code is executed; either \"local\" or  \"dlabtest\"",
                        default="local")

    parser.add_argument("-d", "--yyyymmdd", 
                        help="Standard date(Utilize for calculating vinatage of building(column name: vintageyr)), Default is today",
                        default=datetime.datetime.now().strftime('%Y%m%d'), type=str)

    args = parser.parse_args()
    pnugeo = PnuGeo(experiment_env = args.env, experiment_yyyymmdd = args.yyyymmdd)
    pnugeo.exec()
    pnugeo.save_config()
    exit()
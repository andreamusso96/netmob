from datetime import date
from joblib import Parallel, delayed
from typing import List
import itertools
from tqdm import tqdm

import pandas as pd
import xarray as xr
import numpy as np
import geopandas as gpd

from .enums import City, Service, TrafficType, TimeOptions
from config import DATA_DIR


def load_traffic_data_city(traffic_type: TrafficType, city: City, service: List[Service], day: List[date]) -> xr.DataArray:
    tuples = list(itertools.product(service, day))
    map_tuple_index = {t: i for i, t in enumerate(tuples)}
    # data_vals = Parallel(n_jobs=-1)(delayed(load_traffic_data_base)(traffic_type=traffic_type, city=city, service=s, day=d) for s, d in tuples)
    data_vals = [load_traffic_data_base(traffic_type=traffic_type, city=city, service=s, day=d) for s, d in tqdm(tuples)]
    data_vals = np.stack([np.stack([data_vals[map_tuple_index[(s, d)]] for s in service], axis=-1) for d in day], axis=-1)
    coords = {'tile': get_location_list(city=city),
              'time': TimeOptions.get_times(),
              'service': [s.value for s in service],
              'day': day}
    dims = ['tile', 'time', 'service', 'day']
    xar = xr.DataArray(data_vals, coords=coords, dims=dims)
    return xar


def get_location_list(city: City):
    traffic_data = load_traffic_data_file(traffic_type=TrafficType.UL, city=city, service=Service.WIKIPEDIA, day=date(2019, 4, 1))
    return traffic_data.index


def load_traffic_data_base(traffic_type: TrafficType, city: City, service: Service, day: date) -> pd.DataFrame:
    if traffic_type == TrafficType.UL_AND_DL:
        ul_data = load_traffic_data_file(traffic_type=TrafficType.UL, city=city, service=service, day=day)
        dl_data = load_traffic_data_file(traffic_type=TrafficType.DL, city=city, service=service, day=day)
        traffic = ul_data + dl_data
        return traffic
    else:
        return load_traffic_data_file(traffic_type=traffic_type, city=city, service=service, day=day)


def load_traffic_data_file(traffic_type: TrafficType, city: City, service: Service, day: date) -> pd.DataFrame:
    file_path = get_mobile_traffic_data_file_path(traffic_type=traffic_type, city=city, service=service, day=day)
    cols = ['tile'] + list(TimeOptions.get_times())
    traffic_data = pd.read_csv(file_path, sep=' ', names=cols)
    traffic_data.set_index('tile', inplace=True)

    nans = traffic_data.isna().sum().sum()

    if nans > 0:
        print(f'WARNING: file of traffic_type={traffic_type.value}, city={city.value}, service={service.value}, day={day} contains NaN values n={nans}, share={nans / (traffic_data.shape[0] * traffic_data.shape[1])}. Replacing them with 0.')
        traffic_data.fillna(0, inplace=True)

    return traffic_data


def load_tile_geo_data_city(city: City):
    file_path = get_geo_data_file_path(city=city)
    data = gpd.read_file(filename=file_path, engine="pyogrio")
    data['tile_id'] = data['tile_id'].astype(int)
    data.rename(columns={'tile_id': 'tile'}, inplace=True)
    data.set_index(keys='tile', inplace=True)
    return data


def get_mobile_traffic_data_file_path(traffic_type: TrafficType, city: City, service: Service, day: date):
    day_str = day.strftime('%Y%m%d')
    path = f'{DATA_DIR}/traffic/{city.value}/{service.value}/{day_str}/'
    file_name = f'{city.value}_{service.value}_{day_str}_{traffic_type.value}.txt'
    file_path = path + file_name
    return file_path


def get_geo_data_file_path(city: City) -> str:
    return f'{DATA_DIR}/tile_geo/{city.value}.geojson'

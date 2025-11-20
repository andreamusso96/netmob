from datetime import date
from typing import List, Tuple
import sys

import pandas as pd
import xarray as xr
import numpy as np
from datetime import time, datetime, timedelta
import logging
from datetime import time

from traffic_data.enums import City, Service, TrafficType, TimeOptions
from traffic_data.load import load_traffic_data_city
from traffic_data.utils import Calendar, Anomalies


logger = logging.getLogger('aggregate_night_traffic')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)


def get_traffic_city_service_date(city: City, service: Service, traffic_type: TrafficType) -> pd.DataFrame:
    logger.info(f"Loading traffic data for {city} {service}")
    traffic_data_service = load_traffic_data_city(traffic_type=traffic_type, city=city, service=[service], day=TimeOptions.get_days())
    traffic_data_service = day_time_to_datetime_index(xar=traffic_data_service)
    traffic_data_service = traffic_data_service.squeeze('service').to_pandas().T.reset_index()
    traffic_data_service['date'] = (traffic_data_service['datetime'] - pd.Timedelta(hours=6)).dt.date
    traffic_data_service = traffic_data_service.groupby(group='date').sum().T
    traffic_data_service = traffic_data_service.rename(columns={t: date(t.year, t.month, t.day).strftime('%Y-%m-%d') for t in traffic_data_service.columns})
    return traffic_data_service


def day_time_to_datetime_index(xar: xr.DataArray) -> xr.DataArray:
    new_index = np.add.outer(xar.indexes['day'], xar.indexes['time']).flatten()
    datetime_xar = xar.stack(datetime=('day', 'time'), create_index=False)
    datetime_xar = datetime_xar.reindex({'datetime': new_index})
    return datetime_xar

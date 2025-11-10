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


def get_traffic_city_service_hour(city: City, service: Service, traffic_type: TrafficType) -> pd.DataFrame:
    logger.info(f"Loading traffic data for {city} {service}")
    traffic_data_service = load_traffic_data_city(traffic_type=traffic_type, city=city, service=[service], day=TimeOptions.get_days())
    traffic_data_service = traffic_data_service.sum(dim='day')
    traffic_data_service = traffic_data_service.squeeze('service').to_pandas()
    traffic_data_service = traffic_data_service.rename(columns={t: time(t.components.hours, t.components.minutes, t.components.seconds).strftime('%H:%M:%S') for t in traffic_data_service.columns})
    return traffic_data_service
from datetime import date
from typing import List, Tuple
import sys

import pandas as pd
import xarray as xr
import numpy as np
from datetime import time, datetime, timedelta
import logging

from traffic_data.enums import City, Service, TrafficType, TimeOptions
from traffic_data.load import load_traffic_data_city
from traffic_data.utils import Calendar, Anomalies


logger = logging.getLogger('aggregate_night_traffic')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)


def get_night_traffic_city_service(city: City, service: Service, traffic_type: TrafficType, start_night: time, end_night: time, remove_nights_before_holiday_and_anomalies: bool = True) -> pd.DataFrame:
    logger.info(f"Loading traffic data for {city} {service}")

    traffic_data_service = load_traffic_data_city(traffic_type=traffic_type, city=city, service=[service], day=TimeOptions.get_days())
    traffic_data_service = day_time_to_datetime_index(xar=traffic_data_service)

    if remove_nights_before_holiday_and_anomalies:
        traffic_data_service = remove_nights_before_holidays(traffic_data=traffic_data_service)
        traffic_data_service = remove_nights_before_anomalies(traffic_data=traffic_data_service, city=city)

    traffic_data_service = remove_times_outside_range(traffic_data=traffic_data_service, start=start_night, end=end_night)
    traffic_data_service = traffic_data_service.groupby(group=f'datetime.time').sum()
    sorted_time_index = _sort_time_index(time_index=traffic_data_service.time.values, reference_time=start_night)
    traffic_data_service = traffic_data_service.reindex({'time': sorted_time_index})
    traffic_data_service = traffic_data_service.squeeze('service').to_pandas()
    traffic_data_service = traffic_data_service.rename(columns={t: t.strftime('%H:%M:%S') for t in traffic_data_service.columns})
    return traffic_data_service


def day_time_to_datetime_index(xar: xr.DataArray) -> xr.DataArray:
    new_index = np.add.outer(xar.indexes['day'], xar.indexes['time']).flatten()
    datetime_xar = xar.stack(datetime=('day', 'time'), create_index=False)
    datetime_xar = datetime_xar.reindex({'datetime': new_index})
    return datetime_xar


def _sort_time_index(time_index: List[time], reference_time: time):
    auxiliary_day = datetime(2020, 2, 1)
    auxiliary_dates = []

    for t in time_index:
        if t < reference_time:
            auxiliary_dates.append(datetime.combine(date=auxiliary_day + timedelta(days=1), time=t))
        else:
            auxiliary_dates.append(datetime.combine(date=auxiliary_day, time=t))

    auxiliary_dates.sort()
    sorted_times = [d.time() for d in auxiliary_dates]
    return sorted_times


def remove_time_period_on_dates(traffic_data: xr.DataArray, dates: List[date], time_start_period: time, length_period: timedelta):
    datetime_ = pd.DatetimeIndex(traffic_data.datetime.values).to_pydatetime()  # noqa
    datetime_intervals_to_remove = [(datetime.combine(day, time_start_period), datetime.combine(day, time_start_period) + length_period) for day in dates]
    datetime_to_remove = np.concatenate([np.where((datetime_ >= start) & (datetime_ < end))[0] for start, end in datetime_intervals_to_remove])
    datetime_to_keep = np.setdiff1d(np.arange(len(datetime_)), datetime_to_remove)
    return traffic_data.isel(datetime=datetime_to_keep)


def remove_nights_before_holidays(traffic_data: xr.DataArray) -> xr.DataArray:
    days_holiday = Calendar.holidays()
    days_before_holiday = [holiday - timedelta(days=1) for holiday in days_holiday]
    days_to_remove = list(set(days_before_holiday).union(set(Calendar.fridays_and_saturdays())))
    traffic_data = remove_time_period_on_dates(traffic_data=traffic_data, time_start_period=time(15), length_period=timedelta(days=1), dates=days_to_remove)
    # Since the first day is a saturday, we cut of its night. If we do not remove it, we have half a day detached from the rest of our series.
    traffic_data = remove_time_period_on_dates(traffic_data=traffic_data, time_start_period=time(0), length_period=timedelta(days=1), dates=[pd.Timestamp(traffic_data.datetime[0].values).to_pydatetime().date()])
    return traffic_data


def remove_nights_before_anomalies(traffic_data: xr.DataArray, city: City) -> xr.DataArray:
    days_anomaly = Anomalies.get_anomaly_dates_by_city(city=city)
    days_before_anomaly = [day - timedelta(days=1) for day in days_anomaly]
    days_to_remove = list(set(days_anomaly).union(set(days_before_anomaly)))
    return remove_time_period_on_dates(traffic_data=traffic_data, time_start_period=time(15), length_period=timedelta(days=1), dates=days_to_remove)


def remove_times_outside_range(traffic_data: xr.DataArray, start: time, end: time) -> xr.DataArray:
    auxiliary_date = date(2020, 1, 1)
    auxiliary_datetime_start, auxiliary_datetime_end = datetime.combine(auxiliary_date, start), datetime.combine(auxiliary_date, end)
    length_period_keep = auxiliary_datetime_end - auxiliary_datetime_start if auxiliary_datetime_end > auxiliary_datetime_start else (auxiliary_datetime_end + timedelta(days=1)) - auxiliary_datetime_start
    length_period_remove = timedelta(days=1) - length_period_keep
    dates = list(np.unique([d.date() for d in pd.DatetimeIndex(traffic_data.datetime.values).to_pydatetime()]))  # noqa
    return remove_time_period_on_dates(traffic_data=traffic_data, time_start_period=end, length_period=length_period_remove, dates=dates)
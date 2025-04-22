from traffic_data.load import load_traffic_data_city, load_tile_geo_data_city 
from traffic_data.enums import City, Service, TrafficType
from datetime import date, time, datetime
from aggregate_night_traffic import get_night_traffic_city_service
from rasterize_traffic import rasterize_traffic_city_service_by_tile_time
from zonal_statistics import compute_zonal_statistics_traffic_raster_city_service
import plotly.express as px
import geopandas as gpd
from memory_profiler import profile
import time as time_profiling
import sys
from config import INSEE_TILE_GEO_DATA_FILE
import logging


def test_load_traffic():
    td = load_traffic_data_city(traffic_type=TrafficType.UL_AND_DL, city=City.BORDEAUX, service=[Service.WIKIPEDIA], day=[date(2019, 4, 3)])
    geo = load_tile_geo_data_city(city=City.BORDEAUX)
    print('TRAFFIC DATA')
    print(td.dims)
    print(td.shape)
    print(td.coords)
    print(td.head())
    print('GEO DATA')
    print(geo)
    print(geo.head())
    print(geo.dtypes)
    print(geo.shape)
    print(geo.columns)


def test_aggregate_night_traffic():
    city = City.DIJON
    traffic_type = TrafficType.UL_AND_DL
    start_night = time(22)
    end_night = time(5)
    service = Service.WIKIPEDIA
    remove_nights_before_holiday_and_anomalies = True

    night_traffic = get_night_traffic_city_service(city=city, traffic_type=traffic_type, start_night=start_night, end_night=end_night, service=service, remove_nights_before_holiday_and_anomalies=remove_nights_before_holiday_and_anomalies)
    print('NIGHT TRAFFIC DATA')
    print(night_traffic.head())

    return night_traffic


def test_rasterize_traffic():

    city = City.DIJON
    traffic_type = TrafficType.UL_AND_DL
    start_night = time(22)
    end_night = time(5)
    service = Service.WIKIPEDIA
    remove_nights_before_holiday_and_anomalies = True

    night_traffic = get_night_traffic_city_service(city=city, traffic_type=traffic_type, start_night=start_night, end_night=end_night, service=service, remove_nights_before_holiday_and_anomalies=remove_nights_before_holiday_and_anomalies)

    raster = rasterize_traffic_city_service_by_tile_time(traffic_data=night_traffic, city=city)
    print('RASTER DATA')
    print(raster.dims)
    print(raster.shape)
    print(raster.coords)
    print(raster.head())
    print('RASTER DATA')

    raster_2230 = raster.sel(time="22:30:00")
    raster_2230_pd = raster_2230.squeeze().to_pandas()
    fig = px.imshow(raster_2230_pd, color_continuous_scale='Viridis', title="Traffic Service and Geo Test", labels={'value': 'Value'}, width=800, height=600,  origin='lower')
    fig.show()


def test_zonal_statistics():
    city = City.DIJON
    traffic_type = TrafficType.UL_AND_DL
    start_night = time(22)
    end_night = time(5)
    service = Service.WIKIPEDIA
    remove_nights_before_holiday_and_anomalies = True

    night_traffic = get_night_traffic_city_service(city=city, traffic_type=traffic_type, start_night=start_night, end_night=end_night, service=service, remove_nights_before_holiday_and_anomalies=remove_nights_before_holiday_and_anomalies)

    raster = rasterize_traffic_city_service_by_tile_time(traffic_data=night_traffic, city=city)
    print('RASTER DATA')
    print(raster.dims)
    print(raster.shape)
    print(raster.coords)
    print(raster.head())
    print('RASTER DATA')

    raster_2230 = raster.sel(time="22:30:00")
    raster_2230_pd = raster_2230.squeeze().to_pandas()
    fig = px.imshow(raster_2230_pd, color_continuous_scale='Viridis', title="Traffic Service and Geo Test", labels={'value': 'Value'}, width=800, height=600,  origin='lower')
    fig.show()

    vector_file_path = '/cluster/work/coss/anmusso/netmob/data/shape/insee_tile_geo.parquet'
    vectors = gpd.read_parquet(vector_file_path)

    zonal_stats = compute_zonal_statistics_traffic_raster_city_service(city=city, service=service, traffic_raster=raster, vectors=vectors, vector_id_col='Idcar_200m', coverage_threshold=0.8)
    print('ZONAL STATISTICS')
    print(zonal_stats.head())


@profile
def speed_and_memory_test():
    city = City.PARIS
    traffic_type = TrafficType.UL_AND_DL
    start_night = time(22)
    end_night = time(6)
    service = Service.FACEBOOK
    remove_nights_before_holiday_and_anomalies = True
    vector_file_path = '/cluster/work/coss/anmusso/netmob/data/shape/insee_tile_geo.parquet'
    vector_id_col = 'Idcar_200m'
    coverage_threshold = 0.8
    zonal_stats_ouput_file_path = '/cluster/home/anmusso/Projects/NetMobV2/netmob/zonal_stats_speed_test.parquet'

    print('Starting speed and memory test test')
    time_start = time_profiling.time()
    night_traffic = get_night_traffic_city_service(city=city, traffic_type=traffic_type, start_night=start_night, end_night=end_night, service=service, remove_nights_before_holiday_and_anomalies=remove_nights_before_holiday_and_anomalies)
    print('Night traffic data loaded')
    raster = rasterize_traffic_city_service_by_tile_time(traffic_data=night_traffic, city=city)
    print('Raster data created')
    vectors = gpd.read_parquet(vector_file_path)
    zonal_stats = compute_zonal_statistics_traffic_raster_city_service(city=city, service=service, traffic_raster=raster, vectors=vectors, vector_id_col=vector_id_col, coverage_threshold=coverage_threshold)
    zonal_stats.to_parquet(zonal_stats_ouput_file_path, index=False)
    print('Zonal statistics saved to', zonal_stats_ouput_file_path)
    time_end = time_profiling.time()
    print('Time taken:', time_end - time_start, 'seconds')
    print('Speed and memory test finished')


def run_job():
    city = City(sys.argv[1])
    service = Service(sys.argv[2])

    start_night = time(22)
    end_night = time(5)
    traffic_type = TrafficType.UL_AND_DL
    remove_nights_before_holiday_and_anomalies = True
    vector_file_path = INSEE_TILE_GEO_DATA_FILE
    vector_id_col = 'Idcar_200m'
    coverage_threshold = 0.8
    zonal_stats_output_file_path = f'/cluster/work/coss/anmusso/netmob/data/zonal_stats/insee_tile_v2/zonal_stats_{city.value}_{service.value}.parquet'


    logger = logging.getLogger(f'Logger_{city.value}_{service.value}')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.info('Starting zonal statistics job for city %s and service %s', city.value, service.value)
    logger.info('Loading traffic data')
    night_traffic = get_night_traffic_city_service(city=city, traffic_type=traffic_type, start_night=start_night, end_night=end_night, service=service, remove_nights_before_holiday_and_anomalies=remove_nights_before_holiday_and_anomalies)
    logger.info('Night traffic data loaded')
    logger.info('Rasterizing traffic data')
    raster = rasterize_traffic_city_service_by_tile_time(traffic_data=night_traffic, city=city)
    logger.info('Raster data created')
    logger.info('Zonal statistics computation started')
    vectors = gpd.read_parquet(vector_file_path)
    zonal_stats = compute_zonal_statistics_traffic_raster_city_service(city=city, service=service, traffic_raster=raster, vectors=vectors, vector_id_col=vector_id_col, coverage_threshold=coverage_threshold)
    logger.info('Zonal statistics computation finished')
    logger.info('Saving zonal statistics to %s', zonal_stats_output_file_path)
    zonal_stats.to_parquet(zonal_stats_output_file_path, index=False)
    logger.info('Zonal statistics saved to %s', zonal_stats_output_file_path)
    logger.info('Zonal statistics job finished')

if __name__ == '__main__':
    run_job()

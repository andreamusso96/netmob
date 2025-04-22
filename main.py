from traffic_data.load import load_traffic_data_city, load_tile_geo_data_city 
from traffic_data.enums import City, Service, TrafficType
from datetime import date, time
from aggregate_night_traffic import get_night_traffic_city_service
from rasterize_traffic import rasterize_traffic_city_service_by_tile_time
from zonal_statistics import compute_zonal_statistics_traffic_raster_city_service
import plotly.express as px

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
    city = City.BORDEAUX
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

    city = City.BORDEAUX
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
    city = City.BORDEAUX
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

    zonal_stats = compute_zonal_statistics_traffic_raster_city_service(city=city, service=service, traffic_raster=raster, vectors=night_traffic, vector_id_col='tile', coverage_threshold=0.8)
    print('ZONAL STATISTICS')
    print(zonal_stats.head())


if __name__ == '__main__':
    test_zonal_statistics()

from traffic_data.load import load_traffic_data_city, load_tile_geo_data_city 
from traffic_data.enums import City, Service, TrafficType
from datetime import date

if __name__ == '__main__':
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


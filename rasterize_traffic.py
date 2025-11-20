from typing import List
import sys

import xarray as xr
import geopandas as gpd
import numpy as np
import pandas as pd
import geopandas as gpd
import numpy as np
import xarray as xr
from geocube.api.core import make_geocube
from geocube.rasterize import rasterize_image
import pandas as pd


from traffic_data.enums import City
from traffic_data.load import load_tile_geo_data_city


def rasterize_points(gdf: gpd.GeoDataFrame, measurements: List[str], tile_size: float, no_data: float, epsg: int) -> xr.DataArray:
    raster = make_geocube(
        vector_data=gdf,
        measurements=measurements,
        resolution=(-tile_size, tile_size),
        output_crs=f"epsg:{epsg}",
        rasterize_function=rasterize_image,
        fill=no_data
    ).to_array()
    return raster


def rasterize_traffic_city_service_by_tile(traffic_data: pd.DataFrame, city: City, z_dim: str) -> xr.DataArray:
    # Traffic data must have as index the tiles of a city and as columns the z_dim
    traffic_tile_geo = load_tile_geo_data_city(city).to_crs(epsg=2154)
    traffic_tile_geo['geometry'] = traffic_tile_geo['geometry'].centroid
    traffic_service_and_geo = gpd.GeoDataFrame(pd.merge(traffic_data, traffic_tile_geo, left_index=True, right_index=True))
    raster = rasterize_points(gdf=traffic_service_and_geo, measurements=list(traffic_data.columns), tile_size=100, no_data=np.nan, epsg=2154)
    raster = raster.rename({'variable': z_dim})
    # Be careful here that rioxarray (which is the type of the raster) has a bug when you make a copy of the dataarray (i.e., the transform is lost)
    # So never make a copy of this raster
    return raster
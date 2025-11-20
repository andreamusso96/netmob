from typing import List, Any

import geopandas as gpd
import rasterio
import numpy as np
import xarray as xr
import rasterstats
import shapely
import pandas as pd
from datetime import time, date
from tqdm import tqdm

from traffic_data.enums import City, Service


def compute_zonal_statistics_traffic_raster_city_service(city: City, service: Service, traffic_raster: xr.DataArray, vectors: gpd.GeoDataFrame, vector_id_col: str, coverage_threshold: float, z_dim: str) -> pd.DataFrame:
    assert traffic_raster.ndim == 3, "The input raster must be 3D, i.e. it must correspond to the traffic for a specific city, service pairs. The dimensions should be y,x and z."
    vectors = extract_vector_within_raster_coverage(raster=traffic_raster.isel({z_dim: 0}), vector=vectors, coverage_threshold=coverage_threshold)
    zonal_stats = [_get_zonal_statistics(c=city, s=service, z=z, z_dim=z_dim, raster=traffic_raster.sel({z_dim: z}), vectors=vectors, vector_id_col=vector_id_col) for z in tqdm(traffic_raster.coords[z_dim].values)]
    zonal_stats = pd.concat(zonal_stats)
    return zonal_stats

def _get_zonal_statistics(c: City, s: Service, z: str, z_dim: str, raster: xr.DataArray, vectors: gpd.GeoDataFrame, vector_id_col: str) -> pd.DataFrame:
    zonal_stats = extract_zonal_stats(raster=raster, no_data=np.nan, vector=vectors, stats=['mean'], all_touched=True)
    zonal_stats['service'] = s.value
    zonal_stats[z_dim] = z
    zonal_stats['city'] = c.value
    zonal_stats.rename(columns={'mean_raster_value': 'traffic'}, inplace=True)
    zonal_stats = zonal_stats[[vector_id_col, 'city', 'service', z_dim, 'traffic']].copy()
    return zonal_stats


def extract_zonal_stats(raster: xr.DataArray, no_data: float, vector: gpd.GeoDataFrame, stats: List[str] = ['mean'], all_touched: bool = True) -> pd.DataFrame:
    _check_xarray_is_2d_raster(raster=raster)
    vector['tmp_index'] = np.arange(len(vector))
    raster_statistics = rasterstats.zonal_stats(vectors=vector.geometry, raster=raster.values, affine=raster.rio.transform(), nodata=no_data, stats=stats, all_touched=all_touched)
    raster_statistics = pd.DataFrame([[r[k] for k in stats] for r in raster_statistics], index=np.arange(len(vector)), columns=[f'{s}_raster_value' for s in stats])
    vector_with_zonal_stats = gpd.GeoDataFrame(pd.merge(vector, raster_statistics, left_on='tmp_index', right_index=True, how='left'))
    vector_with_zonal_stats.drop(columns='tmp_index', inplace=True)
    # Returns null for vectors that do not intersect the raster
    return vector_with_zonal_stats


def extract_vector_within_raster_coverage(raster: xr.DataArray, vector: gpd.GeoDataFrame, coverage_threshold: float = 0.8) -> gpd.GeoDataFrame:
    _check_xarray_is_2d_raster(raster=raster)
    binary_raster_np = np.where(np.isnan(raster.values), 0, 1).astype(rasterio.uint8)
    level_curve_vector = gpd.GeoDataFrame([{'geometry': shapely.geometry.shape(s), 'value': v} for s, v in rasterio.features.shapes(binary_raster_np, mask=None, transform=raster.rio.transform())], crs=raster.rio.crs)
    level_curve_vector['geometry'] = level_curve_vector['geometry'].buffer(1)
    raster_coverage = level_curve_vector[level_curve_vector.value == 1].unary_union
    vector = vector.loc[vector.intersection(raster_coverage).area / vector.area > coverage_threshold].copy()
    return vector


def _check_xarray_is_2d_raster(raster: xr.DataArray):
    assert raster.ndim == 2, "The input raster must be 2D"
    assert raster.rio is not None, "The input raster must have a CRS"
    assert raster.rio.transform() is not None, "The input raster must have a transform"
    assert raster.rio.crs is not None, "The input raster must have a CRS"

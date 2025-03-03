"""
MODIS-LAI
"""

# the modified MOD15A2GFS version sourced from nasa data is provided here by BNU data link: http://globalchange.bnu.edu.cn/research/laiv061#usage
# data managment issue: https://github.com/leap-stc/data-management/issues/188
from dataclasses import dataclass

import apache_beam as beam

# from leap_data_management_utils.data_management_transforms import (
#     get_catalog_store_urls,
# )
import numpy as np
import pandas as pd
import s3fs
import xarray as xr
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.storage import FSSpecTarget
from pangeo_forge_recipes.transforms import (
    ConsolidateDimensionCoordinates,
    ConsolidateMetadata,
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
)

# transfered from very slow http server to leap osn with rclone - transfer.sh
input_urls = [
    f'https://nyu1.osn.mghpcc.org/leap-pangeo-manual/MODIS_LAI/lai_8-day_0.05_{year}.nc'
    for year in np.arange(2002, 2020 + 1)
]


pattern = pattern_from_file_sequence(input_urls, concat_dim='time')


@dataclass
class FixTime(beam.PTransform):
    """change day of year to datetime"""

    def _fixtime(self, ds: xr.Dataset) -> xr.Dataset:
        year = ds.time.encoding['source'].split('lai_8-day_0.05_')[1].split('.nc>')[0]
        dates = pd.to_datetime([f'{year}-{doy}' for doy in ds.time.values], format='%Y-%j')
        return ds.assign_coords(time=dates)

    def expand(self, pcoll):
        return pcoll | '_fixtime' >> beam.MapTuple(lambda k, v: (k, self._fixtime(v)))


fs = s3fs.S3FileSystem(
    key='', secret='', client_kwargs={'endpoint_url': 'https://nyu1.osn.mghpcc.org'}
)
target_root = FSSpecTarget(fs, 'leap-pangeo-pipeline/MODIS_LAI')


with beam.Pipeline() as p:
    (
        p
        | beam.Create(pattern.items())
        | OpenURLWithFSSpec()
        | OpenWithXarray()
        | FixTime()
        | StoreToZarr(
            target_root=target_root,
            target_chunks={'time': 100, 'lat': 360, 'lon': 720},
            store_name='MODIS_LAI.zarr',
            combine_dims=pattern.combine_dim_keys,
        )
        | ConsolidateDimensionCoordinates()
        | ConsolidateMetadata()
    )

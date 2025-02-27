"""
MODIS-LAI
"""

# the modified MOD15A2GFS version sourced from nasa data is provided here by BNU data link: http://globalchange.bnu.edu.cn/research/laiv061#usage
# data managment issue: https://github.com/leap-stc/data-management/issues/188
from dataclasses import dataclass

import apache_beam as beam
import fsspec

# from leap_data_management_utils.data_management_transforms import (
#     get_catalog_store_urls,
# )
import numpy as np
import pandas as pd
import xarray as xr
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.storage import FSSpecTarget
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
)

input_urls = [
    f'http://globalchange.bnu.edu.cn/laiv061/global_lai_0.05/lai_8-day_0.05_{year}.nc'
    for year in np.arange(2002, 2020 + 1)
]
pattern = pattern_from_file_sequence(input_urls, concat_dim='time')
# NOTE! this is pruned for testing
pattern = pattern.prune(2)


@dataclass
class FixTime(beam.PTransform):
    """change day of year to datetime"""

    def _fixtime(self, ds: xr.Dataset) -> xr.Dataset:
        year = ds.time.encoding['source'].split('lai_8-day_0.05_')[1].split('.nc>')[0]
        dates = pd.to_datetime([f'{year}-{doy}' for doy in ds.time.values], format='%Y-%j')
        return ds.assign_coords(time=dates)

    def expand(self, pcoll):
        return pcoll | '_fixtime' >> beam.MapTuple(lambda k, v: (k, self._fixtime(v)))


fs = fsspec.get_filesystem_class('file')()
target_root = FSSpecTarget(fs, 'modis_LAI')
with beam.Pipeline() as p:
    (
        p
        | beam.Create(pattern.items())
        | OpenURLWithFSSpec()
        | OpenWithXarray()
        | FixTime()
        | StoreToZarr(
            target_root='.',
            # TODO: Determine chunking
            store_name='MODIS_LAI.zarr',
            combine_dims=pattern.combine_dim_keys,
        )
    )

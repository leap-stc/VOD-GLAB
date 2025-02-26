"""
MODIS-LAI
"""
# nasa data link: https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/6/MOD15A2GFS/
# data managment issue: https://github.com/leap-stc/data-management/issues/188
import apache_beam as beam
from leap_data_management_utils.data_management_transforms import (
    get_catalog_store_urls,
)
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    ConsolidateDimensionCoordinates,
    ConsolidateMetadata,
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
)

###########################
## Start Modifying here ###
###########################
input_urls = [
    f'http://globalchange.bnu.edu.cn/laiv061/global_lai_0.05/lai_8-day_0.05_{year}.nc'
    for year in np.arange(2002, 2020 + 1)
]
@dataclass
class RenameC(beam.PTransform):
    """
    this function rename columns
    """
    def _rename_date(self, ds: xr.Dataset) -> xr.Dataset:
        return ds.rename({'acq_date': 'time'})
    
    def expand(self, pcoll):
        return pcoll | beam.MapTuple(lambda filename, ds: (filename, self._rename_date(ds)))

pattern = pattern_from_file_sequence(input_urls, concat_dim='time')

# Construct the pipeline.
modis-lai = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()            # Download files with fsspec and EarthAccess authentication.
    | OpenWithXarray()               # Open each file as an xarray.Dataset.
    | RenameC()                      # Rename columns
    | StoreToZarr(
          store_name="modis-lai.zarr",
          combine_dims=pattern.combine_dim_keys,
      )
    | ConsolidateDimensionCoordinates()  # Merge per-file coordinate info.
    | ConsolidateMetadata()              # Merge global metadata.
)

# For local testing, run the pipeline with a Beam runner.
if __name__ == "__main__":
    with beam.Pipeline() as p:
        p | pipeline

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

# parse the catalog store locations (this is where the data is copied to after successful write (and maybe testing)
catalog_store_urls = get_catalog_store_urls('feedstock/catalog.yaml')

###########################
## Start Modifying here ###
###########################
import re
import earthaccess
import tempfile
from datetime import datetime, timedelta
from urllib.parse import urlparse
# Log in to EarthAccess (credentials must be configured)
earthaccess.login()
results = earthaccess.search_data(
    short_name="MOD15A2H",
    temporal=("2001-01-01", "2001-01-01"),
    bounding_box=(-180, -90, 180, 90)
)#for testing end date is set to "2001-01-01", it should change to "2021-12-31" to match casm end date

##  Search for the MOD15A2H product (LAI) over January 2001-2021 globally.
# Extract file URLs from the UMM metadata. We look for the first URL of type "GET DATA".
input_urls = []
for result in results:
    related_urls = result.get("umm", {}).get("RelatedUrls", [])
    for url_info in related_urls:
        if url_info.get("Type", "").upper() == "GET DATA":
            input_urls.append(url_info.get("URL"))
            break


@# --- Custom Transform: AddLatLonCoordinates ---
# This transform uses the filename (key) to compute lat/lon coordinates.
@dataclass
class AddLatLonCoordinates(beam.PTransform):
    """
    For each (filename, dataset) pair, parse the filename to extract the tile indices,
    compute the MODIS Sinusoidal tile boundaries, generate a grid of pixel centers,
    reproject to geographic (lat/lon), and assign these as new coordinates.
    """
    # Define MODIS grid constants (approximate values)
    tile_width: int = 1111950        # Tile width in meters
    x_min_global: int = -20015109      # Global origin x (meters)
    y_max_global: int = 10007555       # Global origin y (meters)
    
    def _parse_filename(self, filename: str):
        """
        Parse filenames of the form:
          MOD15A2H.AYYYYDDD.hXXvYY.CCC.XXXXXXXXXXXXX.hdf
        Returns a dict with acquisition_date, tile, h, v, etc.
        """
        pattern = r"^(MOD15A2H)\.(A\d{7})\.(h\d{2}v\d{2})\.(\d{3})\.(\d{13})\.hdf$"
        m = re.match(pattern, filename)
        if not m:
            raise ValueError(f"Filename does not match expected pattern: {filename}")
        product, acquisition, tile, collection, prod_time = m.groups()
        year = int(acquisition[1:5])
        doy = int(acquisition[5:])
        acq_date = datetime(year, 1, 1) + timedelta(days=doy - 1)
        h = int(tile[1:3])
        v = int(tile[4:6])
        return {"acq_date": acq_date, "tile": tile, "h": h, "v": v}
    
    def _add_coords(self, ds: xr.Dataset, filename: str) -> xr.Dataset:
        # Use pyproj to define the transformation from MODIS Sinusoidal to WGS84.
        from pyproj import Transformer, Proj
        sinus_proj = Proj("+proj=sinu +R=6371007.181 +nadgrids=@null +wktext")
        wgs84_proj = Proj(proj='latlong', datum='WGS84')
        transformer = Transformer.from_proj(sinus_proj, wgs84_proj)
        
        # Parse filename for tile information.
        info = self._parse_filename(filename)
        h = info["h"]
        v = info["v"]
        
        # Assume the main data variable is 'Lai_500m' and its dimensions are ('y', 'x').
        if "Lai_500m" not in ds:
            raise ValueError("Dataset does not contain variable 'Lai_500m'")
        nrows, ncols = ds["Lai_500m"].shape
        
        # Compute tile boundaries in the Sinusoidal projection.
        x_left = self.x_min_global + h * self.tile_width
        x_right = x_left + self.tile_width
        y_top = self.y_max_global - v * self.tile_width
        y_bottom = y_top - self.tile_width
        
        # Generate pixel-center coordinates.
        x_coords = np.linspace(x_left, x_right, num=ncols)
        y_coords = np.linspace(y_top, y_bottom, num=nrows)
        xv, yv = np.meshgrid(x_coords, y_coords)
        
        # Transform to geographic coordinates.
        lon, lat = transformer.transform(xv, yv)
        
        # Add lat/lon as new coordinate variables. Here we assume the dataset's spatial dims are named 'y' and 'x'.
        ds = ds.assign_coords(lat=(("y", "x"), lat), lon=(("y", "x"), lon))
        
        # Optionally, add the parsed acquisition date as an attribute.
        ds = ds.assign_attrs(acquisition_date=str(info["acq_date"]))
        return ds

    def expand(self, pcoll):
        # Expect each element to be a (filename, dataset) tuple.
        return pcoll | beam.MapTuple(lambda filename, ds: (filename, self._add_coords(ds, filename)))

# --- Build the Beam Pipeline ---
@dataclass
class RenameDate(beam.PTransform):
    """
    Example transform to rename the date coordinate.
    If your datasets use a coordinate like 'acq_date', rename it to 'time'.
    """
    def _rename_date(self, ds: xr.Dataset) -> xr.Dataset:
        return ds.rename({'acq_date': 'time'})
    
    def expand(self, pcoll):
        return pcoll | beam.MapTuple(lambda filename, ds: (filename, self._rename_date(ds)))

pattern = pattern_from_file_sequence(input_urls, concat_dim='time')

# Construct the pipeline.
pipeline = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()            # Download files with fsspec and EarthAccess authentication.
    | OpenWithXarray()               # Open each file as an xarray.Dataset.
    | AddLatLonCoordinates()         # Compute and add lat/lon coordinates.
    | RenameDate()                   # Rename 'acq_date' to 'time' for concatenation.
    | StoreToZarr(
          store_name="mod15a2h_lai.zarr",
          combine_dims=pattern.combine_dim_keys,
      )
    | ConsolidateDimensionCoordinates()  # Merge per-file coordinate info.
    | ConsolidateMetadata()              # Merge global metadata.
)

# For local testing, run the pipeline with a Beam runner.
if __name__ == "__main__":
    with beam.Pipeline() as p:
        p | pipeline

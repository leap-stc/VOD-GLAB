# switched to dask 'recipe'
# input files were transfered in `transfer.sh` to LEAP OSN due to very slow server

import numpy as np
import xarray as xr
from obstore.fsspec import AsyncFsspecStore
from obstore.store import S3Store
import pandas as pd
from dask.distributed import Client
import s3fs

client = Client()
client

input_urls = [
    f"https://nyu1.osn.mghpcc.org/leap-pangeo-manual/MODIS_LAI/lai_8-day_0.05_{year}.nc"
    for year in np.arange(2002, 2020 + 1)
]
input_url_suffixes = [url.rsplit("/", 1)[-1] for url in input_urls]


read_store = S3Store(
    "leap-pangeo-manual/MODIS_LAI",
    aws_endpoint="https://nyu1.osn.mghpcc.org",
    access_key_id="",
    secret_access_key="",
)
read_fss = AsyncFsspecStore(read_store)


def preprocess(ds: xr.Dataset) -> xr.Dataset:
    year = ds.time.encoding["source"].split("lai_8-day_0.05_")[1].split(".nc>")[0]
    dates = pd.to_datetime([f"{year}-{doy}" for doy in ds.time.values], format="%Y-%j")
    return ds.assign_coords(time=dates)


cds = xr.open_mfdataset(
    [read_fss.open(suffix) for suffix in input_url_suffixes],
    preprocess=preprocess,
    parallel=True,
    coords="minimal",
    data_vars="minimal",
    compat="override",
)

fs = s3fs.S3FileSystem(
    key="", secret="", client_kwargs={"endpoint_url": "https://nyu1.osn.mghpcc.org"}
)

mapper = fs.get_mapper("leap-pangeo-pipeline/MODIS_LAI/MODIS_LAI.zarr")

cds.chunk({"time": 100, "lat": 360, "lon": 720}).to_zarr(
    mapper, mode="w", consolidated=True
)

# check RT


store = "https://nyu1.osn.mghpcc.org/leap-pangeo-pipeline/MODIS_LAI/MODIS_LAI.zarr"
ds = xr.open_dataset(store, engine="zarr", chunks={})
ds.isel(time=0).lai.plot()

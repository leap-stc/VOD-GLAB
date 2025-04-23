import xarray as xr
import numpy as np
import fsspec
from dask.distributed import Client
from tqdm import tqdm 
import time 

# run on a 128 cpu LEAP instance
client = Client(n_workers=16)


netcdf_urls = [
    f"https://zenodo.org/records/10306095/files/GLAB_VOD_{year}.nc?download=1"
    for year in np.arange(2002, 2021)
]

def open_with_delay(url: str, delay_seconds: int = 5)-> xr.Dataset:
    # Zenodo is limiting requests, so we add a time.sleep to make it happy. 
    # We're also using fsspec's simple cache to download the files so when to_zarr is called,
    # we won't hit the rate limit from Zenodo.
    file = fsspec.open_local(f"simplecache::{url}", simplecache={'cache_storage': '/tmp/fsspec_cache'})[0]
    ds = xr.open_dataset(file, chunks={}, engine='netcdf4')
    time.sleep(delay_seconds) 
    return ds
    
ds_list = [open_with_delay(url) for url in tqdm(netcdf_urls)]

cds = xr.concat(ds_list, dim='time')

# downcast float 64 to 32
cds['VOD'] = cds['VOD'].astype('float32')
cds['VOD_std'] = cds['VOD_std'].astype('float32')
# medium chunking - a bit of time a bit of space
cds = cds.chunk({'time':200, 'lat':260,'lon':450})
# write to gcs
cds.to_zarr('gs://leap-persistent/data-library/GLAB-VOD/GLAB-VOD.zarr',mode='w', consolidated=True)
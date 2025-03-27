#---------------------------------------------------------
# loading packages
#---------------------------------------------------------

import numpy as np
import xarray as xr
from dask.distributed import Client, LocalCluster
from itertools import islice
import s3fs
#---------------------------------------------------------
# 1. initialization and defining variables
#---------------------------------------------------------

cluster = LocalCluster(n_workers=2, threads_per_worker=2)
client = Client(cluster)


# NetCDF URLs on zenedo
netcdf_urls = [
    f"https://zenodo.org/records/10306095/files/GLAB_VOD_{year}.nc?download=1"
    for year in np.arange(2002, 2021)
]

def batch(iterable, n=1):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, n))
        if not chunk:
            break
        yield chunk

batches = list(batch(netcdf_urls, n=5))# to avoid TOO MANY REQUESTS error from Zenodo
#---------------------------------------------------------
# 2. loading the data
#---------------------------------------------------------
datasets = []
import time
for batch in batches:
    print(batch)
    ds = xr.open_mfdataset(
        batch,
        engine="h5netcdf",
        combine="by_coords",
        coords="minimal",
        data_vars="minimal",
        compat="override",
        parallel=True
    )
    datasets.append(ds)
    
    time.sleep(25)# to avoid TOO MANY REQUESTS error from Zenodo
cds = xr.combine_by_coords(datasets)

#---------------------------------------------------------
# 3. writting the zarr file
#-------------------------------------------------------


fs = s3fs.S3FileSystem(
    key="", secret="", client_kwargs={"endpoint_url": "https://nyu1.osn.mghpcc.org"}
)

mapper = fs.get_mapper("leap-pangeo-pipeline/VOD-GLAB/VOD-GLAB.zarr")

cds.chunk({"time": 100, "lat": 360, "lon": 720}).to_zarr(
    mapper, mode="w", consolidated=True
)

#---------------------------------------------------------
# 4. reading and plotting
#-------------------------------------------------------

store = "https://nyu1.osn.mghpcc.org/leap-pangeo-pipeline/VOD-GLAB/VOD-GLAB.zarr"
ds = xr.open_dataset(store, engine="zarr", chunks={})
ds.isel(time=0).VOD.plot()

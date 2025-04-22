import gcsfs
import xarray as xr
import requests
import numpy as np
import time
import fsspec
from dask.distributed import Client, LocalCluster

def main():
    # 0) Kick off a local Dask cluster
    cluster = LocalCluster(n_workers=2, threads_per_worker=2)
    client = Client(cluster)

    # 1) GCS scratch area for downloaded .nc files
    fs = gcsfs.GCSFileSystem()
    base_path = "leap-scratch/mitraa90/GLAB-VOD/"
    fs.mkdirs(base_path, exist_ok=True)

    # 2) Stream each year’s NetCDF from Zenodo into GCS
    years = np.arange(2002, 2021)
    gcs_paths = []
    for year in years:
        url = f"https://zenodo.org/records/10306095/files/GLAB_VOD_{year}.nc?download=1"
        dest = base_path + f"GLAB_VOD_{year}.nc"
        gcs_paths.append(f"gcs://{dest}")

        if not fs.exists(dest):
            print(f"⬇️  Streaming {year} → {dest}")
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with fs.open(dest, "wb") as out:
                    for chunk in r.iter_content(8192):
                        out.write(chunk)
            print(f"✅  {year} uploaded")
        else:
            print(f"✅  {year} already exists, skipping")

    # 3) Split into batches of 5 files
    batch_size = 5
    batches = [gcs_paths[i : i + batch_size]
               for i in range(0, len(gcs_paths), batch_size)]

    # 4) Zarr store on GCS
    #zarr_store = "gcs://leap-scratch/mitraa90/GLAB-VOD.zarr"
    #zarr_store ="gcs://leap-persistent/mitraa90/GLAB-VOD/GLAB-VOD.zarr"
    zarr_store="gs://leap-persistent/data-library/GLAB-VOD.zarr"
    fs.mkdirs(zarr_store, exist_ok=True)
    print(f"📁 Confirmed GCS folder: {base_path}")
    
    # 5) Process each batch: open, rechunk, write/append
    i=0
    batch=gcs_paths
    #for i, batch in enumerate(batches):
    print(f"\n📂 Processing batch {i+1}/{len(batches)}:")
    ds = xr.open_mfdataset(
        batch,
        engine="h5netcdf",
        combine="by_coords",
        coords="minimal",
        data_vars="minimal",
        compat="override",
        parallel=True,
        chunks={"time": 377, "lat": 253, "lon": 320},
    )
    ds = ds.chunk({"time": 189, "lat": 253, "lon": 320})
    print(f"📦 Rechunked dataset: time={ds.dims['time']} steps")

    mode = "w" if i == 0 else "a"

    print(f"💾 Writing to Zarr (mode={mode})…")
    if i == 0:
        writing_mode="w"
        ds.to_zarr(
                zarr_store, mode=writing_mode, consolidated=True
            )
    else:
        writing_mode="a"
        ds.to_zarr(
                zarr_store, mode=writing_mode, consolidated=True,append_dim="time" 
            )
    print(f"✅ Batch {i+1} done")

        #if needed time.sleep(20)

    # 6) Tear down Dask
    client.close()
    cluster.close()
    print("\n🎉 All batches written to", zarr_store)

if __name__ == "__main__":
    main()

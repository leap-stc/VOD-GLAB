import xarray as xr
import matplotlib.pyplot as plt
import os
import math
import zarr
def main():
    # ------------------------------------------------------------------
    # 1) open the data
    # ------------------------------------------------------------------
    file_dir = "gs://leap-persistent/data-library/GLAB-VOD.zarr"

    # 2) Open with open_dataset + engine='zarr'

    

    # 1) open normally (no Dask)
    ds = xr.open_dataset(
        file_dir,
        engine="zarr",
        backend_kwargs={"consolidated": False}
    )
    
    
                
   
    # ------------------------------------------------------------------
    # 2) Inspect chunk sizes
    # ------------------------------------------------------------------
     # 2) pull out the on‑disk chunk tuple for your key var
    v_chunks = ds["VOD"].encoding["chunks"]  # e.g. (365, 180, 360)
    
    # 3) re‑wrap the whole dataset with those same dims
    #    (assumes every var shares the same dims/order)
    ds = ds.chunk(dict(zip(ds["VOD"].dims, v_chunks)))
    
    print(ds.chunks)
    chunk_map = ds.chunks
    time_chunks = chunk_map["time"]
    lat_chunks  = chunk_map["lat"]
    lon_chunks  = chunk_map["lon"]
    
    # 4) size in bytes per value
    element_bytes = ds["VOD"].dtype.itemsize  # e.g. 8 for float64
    
    # 5) loop over every combination of chunk‐shapes
    for t in time_chunks:
        for la in lat_chunks:
            for lo in lon_chunks:
                n_elems = t * la * lo
                size_mb = n_elems * element_bytes / (1024 ** 2)
                print(f"chunk shape ({t:>3}, {la:>3}, {lo:>4}) → {size_mb:6.2f} MB")

    # ------------------------------------------------------------------
    # 3) Compute time‑mean and plot
    # ------------------------------------------------------------------
    vod_mean = ds["VOD"].mean(dim="time")

    plt.figure(figsize=(8, 4))
    vod_mean.plot(cmap="viridis")
    plt.title("Mean GLAB‑VOD (2002–2020)")

    # define cwd so your outfn is correct
    cwd = os.getcwd()
    outfn = os.path.join(cwd, "vod_mean.png")
    plt.savefig(outfn, dpi=150, bbox_inches="tight")
    print(f"✅ Saved mean‑VOD plot to: {outfn}")




if __name__ == "__main__":
    main()




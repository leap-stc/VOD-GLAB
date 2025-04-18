import gcsfs
import xarray as xr
import matplotlib.pyplot as plt
import os
def main():
    # 1) point at your Zarr store by URL
    cwd = os.getcwd()
    print(f"cwd is {cwd}")
    store_url = "gcs://leap-scratch/mitraa90/GLAB-VOD.zarr"

    # 2) open it
    ds = xr.open_zarr(
        store_url,
        consolidated=True,
        storage_options={"anon": False},
    )

    # 3) inspect chunking
    #    This prints out the tuple of chunk sizes along each dimension
    print("Chunk sizes for VOD:")
    print(ds["VOD"].data.chunks)  

    # 4) compute the time‐mean VOD
    vod_mean = ds["VOD"].mean(dim="time")

    # 5) plot & save to file
    plt.figure(figsize=(8,4))
    vod_mean.plot(cmap="viridis")
    plt.title("Mean GLAB-VOD (2002–2020)")
    # get the cwd and build your filename
    outfn = os.path.join(cwd, "vod_mean.png")
    plt.savefig(outfn, dpi=150, bbox_inches="tight")
    print(f"✅ Saved mean‑VOD plot to: {outfn}")

    # if you're in an interactive environment, you can also show it:
    try:
        plt.show()
    except:
        pass

if __name__ == "__main__":
    main()
   



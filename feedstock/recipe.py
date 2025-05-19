import xarray as xr
import fsspec
from distributed import Client


"""
Recipe adapted from: https://github.com/pangeo-forge/gpcp-feedstock/blob/main/feedstock/recipe.py
Ran on 05-29-2025 on a 128GB VM
"""


def main():
    client = Client(n_workers=32)
    print(client.dashboard_link)

    # The GPCP files use an annoying naming convention which embeds the creation date in the file name.
    # e.g., https 1996/gpcp_v01r03_daily_d19961001_c20170530.nc
    # This makes it very hard to create a deterministic function to generate the file names,
    # so instead we crawl the NCEI server.

    url_base = "https://www.ncei.noaa.gov/data/global-precipitation-climatology-project-gpcp-daily/access/"
    years = range(1996, 2026)
    file_list = []
    fs = fsspec.filesystem("https")
    for year in years:
        file_list += sorted(
            filter(
                lambda x: x.endswith(".nc"), fs.ls(url_base + str(year), detail=False)
            )
        )

    ds = xr.open_mfdataset(
        file_list,
        parallel=True,
        engine="h5netcdf",
        coords="minimal",
        data_vars="minimal",
        compat="override",
    )

    # ~ 100MB chunks
    ds.chunk({"time": 400}).to_zarr(
        "gs://leap-persistent/data-library/GPCP-daily/GPCP-daily.zarr",
        mode="w",
        zarr_format=3,
        consolidated=False,
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

"""
NCLD 2016 US Forest Service tree canopy cover estimates

    * Source: https://www.mrlc.gov/data/nlcd-2016-usfs-tree-canopy-cover-conus
    * Description: Tree canopy coverage in 30m^2 area cells, collected by USFS and adjusted by MRLCC. See the link. 
                   This is produced as a raster and clipped to a box around the counties of interest. Last updated August 2019.
    * Variables of interest:
        - Value: percentage of 30m^2 unit tree coverage, from 0 to 100. Values of the box around the county boundary are set to 0 in the present dataset, 
                an artifact of exporting from the ArcGIS original dataset. This inflates the zero values in an annoying way, and the code corrects for this 
                zero-inflation in a rough fashion based on county area.
        - Count: number of 30m^2 cells with given Value of coverage
"""

import re
import rasterio
import pandas as pd
import numpy as np
import rasterio.plot as rioplt
import matplotlib.pyplot as plt
from dbfread import DBF
from functools import wraps

### FILEPATHS AND PROPCO

files = {'durham': 'raw/ncld_canopy/durham_canopy.dbf', 'orange': 'raw/ncld_canopy/orange_canopy.dbf'}
files_tif = {k: v.replace(".dbf", ".tif") for k, v in files.items()}

# TODO remove this aspect of the api once zero-deflation no longer needed
county_area =  {'durham': 722, 'orange': 1039}


# prop_co = {'durham': 0.65 * 0.18222222222222165, 'orange': 0.65 * 0.20740740740740712}


### PLOTTERS
# students won't use the data. just for hw prompt display

def plot_canopy(file, outfile, dpi = 300):
    with rasterio.open(file) as r:
        plt.figure(figsize=(12, 12))
        plt.axis("off")
        plt.imshow(r.read(1), cmap = "Greens")
        plt.savefig(outfile, dpi = dpi, pad_inches = 0, transparent = True, bbox_inches = "tight")
        

### GETTERS

# TODO update to act on raw dataset (largeish) or on tif files, not on arcgis exported attribute tables

def dbf_to_df(file):
    """Read ArcGIS attribute tables as dbf files and convert to data frames"""
    d = DBF(file)
    d = pd.DataFrame(d.records)
    return d


### DATA PROCESS


# decorators and helpers
# TODO revisit whether this needs to be a decorator or is better integrated into one of the cleaning functions. at present unsure which function should have this purpose so making a decorator
# this effectively a placeholder for future sanitation needs

def _deflate_zeros(data, county_area):
    """deflates zeros in dataset as prepared by standardize. see module description.
    each cell is 900m^2. calculate total area of squares in data in km^2, which should be larger than county total.
    then subtract excess from zeros. county_area is the county land area in km^2"""
    
    area_diff = int(data.loc[:, "count"].sum() * 900 / 1e6) - county_area
    
    assert area_diff > 0, print(f"area_diff = {area_diff}")
    
    data.loc[0, "count"] -= area_diff
    
    assert data.loc[0, "count"] > 0, print(f"deflated zero count <= 0")
    
    return data

# TODO remove if no longer used
def _extract_county(file):
    """Extracts the county name from the file name, for files using the PATH/county_canpy.extension format."""
    
    return re.search(r"([^/.]+).[a-z]+$", file).group(1).replace("_canopy", "")

def standardize(f):
    
    @wraps(f)
    def wrapper(**kwargs):
        if type(kwargs['data']) != pd.core.frame.DataFrame:
            raise TypeError("'data' must be a data frame")
        
        kwargs['data'].columns = kwargs['data'].columns.str.lower()
        
        # type conversion for np.repeat in unpack: counts must be int64
        kwargs['data'] = kwargs['data'].loc[:, ['value', 'count']].astype('int64')
        
        return f(**kwargs)
    
    return wrapper

@standardize
def unpack(data = None, county_area = 0):
    """data is the output from dbf_to_df after standardization. Replicates rows count number of times and shuffles,
    so that the end result is one row per cell in raster data with single column of value (percent tree cover). drops count.
    This is to allow students to sample from the data uniformly, rather than having to use a weighted sampling scheme they have not learned."""
    
    data = _deflate_zeros(data, county_area)
    
    # pretend like you're using R
    ids = np.repeat(data.index.to_numpy(), data.loc[:, "count"].to_numpy())
    
    data = data.loc[ids, ["value"]].reset_index(drop = True)
    
    return data



### OUT

def clean_and_dump(files, county_area):
    """Gets data from a list of dbf files, processes with unpack, adds a county identifier column and row-binds before writing csv and xlsx.
    files is a dict where keys are county names and values are .dbf file paths
    county_area is a dict with county names and land areas needed for zero-deflation. see module description."""
    
    d = [unpack(data = dbf_to_df(f), county_area = county_area[k],
                ).assign(county = k) for k, f in files.items()]
    
    # rowbind and shuffle
    d = pd.concat(d)
    d = d.sample(d.shape[0]).reset_index(drop = True)
    
    # write csv
    d.to_csv('../stor155_sp21/final_project/canopy/canopy.csv', index = False)
    
    # write excel. can't handle more than 1,048,576 rows
    # https://support.microsoft.com/en-us/office/excel-specifications-and-limits-1672b34d-7043-467e-8e27-269d656771c3
    d.loc[:1e6].to_excel('../stor155_sp21/final_project/canopy/canopy.xlsx', index = False)


#####
#RUN
#####

if __name__ == "__main__":
    
    clean_and_dump(files, county_area)
    
    for k, f in files_tif.items():
        outfile = f"../stor155_sp21/final_project/canopy/{k}.jpeg"
        plot_canopy(f, outfile)
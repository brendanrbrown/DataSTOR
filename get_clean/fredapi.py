#!/usr/bin/env python3

"""
get data from the FRED API

https://fred.stlouisfed.org/docs/api/fred/
"""

import requests
import pandas as pd
from functools import wraps

# TODO:
# place funs overlappling with other modules in common util for later packaging
# generally standardize
# change the file out situation re stor155_sp21: should these be saved in this data project? at least make an arg


############
# MY API KEY
###########
# can set your FRED api key here if calling from another module or notebook
# else use as cli argument
# key = ''

#######
# INTERNALS AND DECORATORS
#######

def _series_ids():
    """extract FRED series ids, country and other relevant data
    for use in get_series"""


def clean_series(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        d = f(*args, **kwargs)
        d = d.drop(columns = ['realtime_start', 'realtime_end'])

        # the federal reserve in its wisdom is using a . for missing values
        d.value = d.value.str.replace(r'^\.$', 'NaN').astype('float')
        d.date = pd.to_datetime(d.date)

        # give the 'value' column the series name
        # so students can ref docs on FRED, then change the name
        var = kwargs.get('ids')
        d = d.rename(columns = {'value': var})

        return d
    return wrapper


# check for errors, return df
def safe_get(target_key):
    def getter(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            r = f(*args, **kwargs)
            assert r.status_code == 200, f"Unsuccessful request, status code {r.status_code}"
            return pd.DataFrame(r.json()[target_key])
        return wrapper
    return getter


#######
# GETTERS
#######

# GET METADATA

@safe_get('tags')
def get_tags(key, **kwargs):
    """
    https://fred.stlouisfed.org/docs/api/fred/tags.html
    """

    url = f'https://api.stlouisfed.org/fred/tags?api_key={key}&file_type=json'

    r = requests.get(url, **kwargs)

    return r



@safe_get('seriess')
def get_series_meta(key, tag_names,
exclude_tag_names = None, limit = 1000, **kwargs):
    """https://fred.stlouisfed.org/docs/api/fred/tags_series.html"""
    url = 'https://api.stlouisfed.org/fred/tags/series?'

    if type(tag_names) is list:
        tag_names = ';'.join(tag_names)

    url = url + f'tag_names={tag_names}&'

    if exclude_tag_names is not None:
        if type(exclude_tag_names) is list:
            exclude_tag_names = ';'.join(exclude_tag_names)

        url = url + f'exclude_tag_names={exclude_tag_names}&'

    url = url + f'api_key={key}&file_type=json'

    r = requests.get(url, **kwargs)

    return r



# GET DATA SERIES

@clean_series
@safe_get('observations')
def get_series(key, ids = None, **kwargs):
    """https://fred.stlouisfed.org/docs/api/fred/series_observations.html"""

    url = f'https://api.stlouisfed.org/fred/series/observations?series_id={ids}&api_key={key}&file_type=json'

    r = requests.get(url, **kwargs)

    return r




###########
# WRITE OUT PROJECT DATA
#########
# TODO:
# break out the country selection part, handle for all in tag_names argument
# too much heterogeneity to functionalize country name handling?
# add annual data series get
# create a process_dsamples decorator as in covid_nyt, or make that one more all-purpose


def get_and_dump(key, n, path):
    """write out n data files for STOR155 project, sampled from a collection
    of employment and economic time series by gender for different european countries.

    Note that matching across series relies on the fact that country names are at the end of the series titles.

    the collection sampled from could be at quarterly or annual time intervals, consistent within each file.

    file format is FRED_i.{csv,xlsx} for i = 0 ... n-1 where each i corresponds to an single student.

    source: Federal Reserve FRED database
    """
    
    # get quarterly emp-pop ratio
    empl = get_series_meta(key, tag_names = ['employment-population ratio', 'quarterly', 'nsa'])

    matches = ~empl.title.str.contains('DISCONTINUED') & empl.title.str.match('Employment to Population Rate: All Ages: (Females|Males)')
    mycountry = empl.loc[matches].title.str.extract(r':\s+([A-z\s]+)$', expand = False).str.split(pat = r'\sfor\s(the\s)?', expand = True)
    mycountry = mycountry.iloc[:, [0, 2]].rename(columns = {0: 'gender', 2: 'country'})

    # df with series info, including ID for API
    empl = mycountry.merge(empl.loc[:, ['id']], left_index = True, right_index = True).reset_index(drop = True)
    empl.country = empl.country.str.lower()
    
    
    # get gdp data series info for those same countries
    gdp = get_series_meta(key, tag_names = ['gdp', 'quarterly', 'nsa'])

    gdp.title = gdp.title.str.lower()
    matches = ~gdp.title.str.contains('euro/ecu series') & gdp.title.str.match('gross domestic product')
    
    gdp = gdp.loc[matches].assign(title = lambda x: x.title.str.extract(r'for\s*(the)?\s*([A-z\s]+)$', expand = False).loc[:, 1])
    gdp = gdp.loc[gdp.title.isin(empl.country)].loc[:, ['title', 'id']].rename(columns = {'title': 'country', 'id': 'gdp'})
    
    # join empl and gdp
    # drop level none to avoid warning
    series = empl.pivot(index = 'country', columns = 'gender').droplevel(level = None, axis = 1).merge(gdp, how = 'inner', right_on = 'country', left_index = True)
      
    # get all, assign to student numbers later since likely n > out.shape[0]
    def series_by_country(i, series):
        # merging on date should be safe --- fred seems to format consistently 
        d = [get_series(key, ids = v) for v in series.loc[i, ['gdp', 'Females', 'Males']]]
        d = d[0].merge(d[1].merge(d[2], how = 'inner', on = 'date'), how = 'inner', on = 'date')
    
        return d
    
    out = [series_by_country(i, series) for i in series.index]
    
    s = pd.Series(range(len(out))).sample(n, replace = True)
    
    for i, _ in enumerate(s):
        try:
            out[i].to_csv(path + '/FRED_{i}.csv', index = False)
            out[i].to_excel(path + '/FRED_{i}.xlsx', index = False)
        except:
            continue
            

            
#######
# RUN
#######

if __name__ == "__main__":
    
    import argparse

    parser = argparse.ArgumentParser('Get and write out FRED gdp and employment by gender for select countries, STOR155 projects, spring `21')
    parser.add_argument('key', type = str, help = 'your FRED api key')
    parser.add_argument('n', type = int, help = 'number of datasets, equal to number of students')
    parser.add_argument('path_out', type = 'str', help = 'filepath to store output, as path_out/FRED_{i}.{csv,xlsx} for i = 0...n-1')

    args = parser.parse_args()

    get_and_dump(args.key, args.n, args.path_out)
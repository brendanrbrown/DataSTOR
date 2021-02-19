#!/usr/bin/env python3

"""
getting and cleaning code for nyt covid data

https://github.com/nytimes/covid-19-data

MASKS SURVEY (here in case it disappears)
This data comes from a large number of interviews conducted online by the global data and survey firm Dynata at the request of The New York Times. The firm asked a question about mask use to obtain 250,000 survey responses between July 2 and July 14, enough data to provide estimates more detailed than the state level. (Several states have imposed new mask requirements since the completion of these interviews.)

Specifically, each participant was asked: How often do you wear a mask in public when you expect to be within six feet of another person?

This survey was conducted a single time, and at this point we have no plans to update the data or conduct the survey again.

COUNTYFP: The county FIPS code.
NEVER: The estimated share of people in this county who would say never in response to the question
“How often do you wear a mask in public when you expect to be within six feet of another person?”
RARELY: The estimated share of people in this county who would say rarely
SOMETIMES: The estimated share of people in this county who would say sometimes
FREQUENTLY: The estimated share of people in this county who would say frequently
ALWAYS: The estimated share of people in this county who would say always
"""

import pandas as pd
from functools import wraps


#######
# GETTERS
#######

# INITIAL
# masks = pd.read_csv('https://github.com/nytimes/covid-19-data/raw/master/mask-use/mask-use-by-county.csv')
# county = pd.read_csv('https://github.com/nytimes/covid-19-data/blob/master/us-counties.csv?raw=true')
# univ = pd.read_csv('https://raw.githubusercontent.com/nytimes/covid-19-data/master/colleges/colleges.csv')

# masks.to_csv('raw/maskuse_nyt.csv', index = False)
# county.to_csv('raw/covid_nyt.csv', index = False)
# univ.to_csv('raw/univ_covid_nyt.csv', index = False)

# LOAD
# masks = pd.read_csv('raw/maskuse_nyt.csv')
# county = pd.read_csv('raw/covid_nyt.csv')
#univ = pd.read_csv('raw/univ_covid_nyt.csv')

#######
# FUNCTIONS
#######

# INTERNALS

# TODO:
# make this more general and port to other getters
def _name_fixer(s):
    # s is the column name as string
    s = s.lower()
    s = s.replace('countyfp', 'fips')
    return s


# DECORATORS
def standardize(f):

    @wraps(f)
    def wrapper(**kwargs):
        # standard names
        data = kwargs.get('data')
        data = data.rename(columns = _name_fixer)

        # Int64 rather than int because pandas implements NaN to pd.NA conversion
        # NOTE: this is experimental https://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html
        data.loc[:, data.dtypes == 'object'] = data.loc[:, data.dtypes == 'object'].apply(lambda x: x.str.lower()).values
        data.loc[:, 'fips'] = data.loc[:, 'fips'].astype("Int64")

        kwargs['data'] = data
        return f(**kwargs)

    return wrapper


# post-processing for data frame sampling
# TODO:
# make more general, e.g. pass arguments with column names to check
# allow file out path to be set in cli?
# just write to sqlite db in future

def process_dsamples(pathout = '', filepre = ''):
    def processer(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            out = f(*args, **kwargs)

            # any undesireables?
            checkna = any([dd.loc[:, 'masks_never':'masks_always'].apply(lambda x: x.notna().sum()).eq(0).any() for dd in out])

            assert not checkna, "NA values discovered in essential data columns, csv not written"

            # datasets must have at least 15 observations
            out = [dd for dd in out if dd.shape[0] >= 15]

            # write out to csv, with id
            filecsv = pathout + '/' + filepre + '_{}.csv'
            filexl = pathout + '/' + filepre + '_{}.xlsx'

            for i, v in enumerate(out):
                try:
                    v.to_csv(filecsv.format(i), index = False)
                    v.to_excel(filexl.format(i), index = False)
                except:
                    continue

        return wrapper
    return processer

# DATA PROCESS
@standardize
def covid_by_co(data = None):
    """clean up covid cases/deaths the sum by county"""

    data.loc[:, 'date'] = pd.to_datetime(data.loc[:, 'date'])
    data.loc[:, 'county'] = data.loc[:, 'county'].mask(data.county.eq('unknown'), pd.NA)

    data = data.groupby(['county', 'state']).agg(cases = ('cases', 'sum'),
    deaths = ('deaths', 'sum'), last_record_on = ('date', 'max'),
    fips = ('fips', lambda x: x.iloc[0])).reset_index()

    return data

# might create categorical variables here later
@standardize
def masks_by_co(data = None):
    data.columns = data.columns.str.replace('([a-z]+)', 'masks_\g<1>')
    data.columns = data.columns.putmask(data.columns == 'masks_fips', 'fips')
    return data


def project_data(county = None, masks = None):
    """clean and join the county covid and masks NYT datasets for STOR155 project"""
    county = covid_by_co(data = county)
    masks = masks_by_co(data = masks)
    d = county.merge(masks, how = 'left', on = 'fips')

    return d


# TODO:
# make this consistent with the fedapi script
# make special class and check class type before processing
# checks for all masks data missing for a given state (none missing in original survey)

# THIS IS WHERE YOU SET THE FILE OUT PATH
@process_dsamples(pathout = '../stor155_sp21/project/CV', filepre = 'CV')
def sample_and_dump(data, n, **kwargs):
    """
    randomly sample states with replacement (default) from d created by project data, and for each state:
    subset d to state and write out csv with name specifying sampling id
    one sampling id per student, to assign one dataset to each student (w/ possible duplicates)
    additional kwargs are passed to sample method

    Does not consider states where there is no mask data
    """

    if 'replace' not in kwargs.keys():
        kwargs['replace'] = True

    # remove states where there is no mask use data
    s = pd.Series(data.state.unique()).sort_values().reset_index(drop = True)
    nonmiss = data.groupby('state').apply(lambda x: x.notna().sum()).loc[:, 'masks_never':'masks_always'].sum(axis = 1).ge(1)
    nonmiss = nonmiss.sort_index().reset_index(drop = True)
    s = s.loc[nonmiss]

    s = s.sample(n, **kwargs)

    data = [data.loc[data.state == v] for k, v in s.items()]

    return data





######
# ISSUES
######
# [x] 3 co with fips missing. NYC and missing fips: https://github.com/nytimes/covid-19-data#geographic-exceptions
# retained as missing, left join with county on left to preserve. none missing in masks data





######
# RUN
######

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser('Sample states and write out county-by-state level for STOR155 projects, spring `21. Data read from dataSTOR/raw')
    parser.add_argument('n', type = int, help = 'number of datasets, equal to number of students')

    args = parser.parse_args()

    masks = pd.read_csv('raw/maskuse_nyt.csv')
    county = pd.read_csv('raw/covid_nyt.csv')

    d = project_data(county = county, masks = masks)
    sample_and_dump(d, args.n)

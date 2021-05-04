#!/usr/bin/env python3

"""
UNC Chapel Hill employee salary data. Updated quarterly and provides a snapshot of HR data, which is *not* the same as actual annual earnings.

Details: https://uncdm.northcarolina.edu/salaries/index.php
"""

### MISC

# no api, so you need to use the manual tool above
# this just manually marks the date of export

# yyyy-mm-dd
export_date = "2021-04-29"

raw_file = "raw/uncch_salaries.xlsx"


import pandas as pd

### GETTERS

def get_salaries(file):
    # xlrd no longer supports xlsx?
    return pd.read_excel(file, engine = 'openpyxl')


### DATA PROCESS

# no need to decorate here

def standardize(d):
    """Standardizes formatting of raw excel salaries file. Column names made proper, all strings to lower, 
    dates to datetime and a year column added for student convenience."""
    
    d.columns = d.columns.str.lower().str.replace("\s+", "_").str.replace('employee_', '')
    d.loc[:, d.dtypes.eq('object')] = d.loc[:, d.dtypes.eq('object')].apply(lambda x: x.str.lower())
    d = d.assign(initial_hire_date = pd.to_datetime(d.initial_hire_date), 
                 hire_year = lambda x: x.initial_hire_date.dt.year)
    
    return d


### OUT

def clean_and_dump(file):
    d = standardize(get_salaries(file))
    
    d.to_csv('../stor155_sp21/final_project/salaries/salaries.csv', index = False)
    d.to_excel('../stor155_sp21/final_project/salaries/salaries.xlsx', index = False)
    

####
# RUN
####

if __name__ == "__main__":
    clean_and_dump(raw_file)
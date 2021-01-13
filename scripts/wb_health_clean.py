# CLEAN AND GET LIFE EXPECTANCY DATA FROM WB DATASET FOR SELECT COUNTRIES AND YEARS
# wd from atom project is ~/Documents/stor155_data
# this dataset was produced by crazy people it seems

import pandas as pd
# import os
# print(os.getcwd())

d = pd.read_csv('./raw/wb_health.csv').drop(columns = ['Series Code', 'Country Code'])
d.query("`Series Name`=='Life expectancy at birth, female (years)' \
& `Country Name` in ['Thailand', 'United States', 'Mali', 'China', 'Ireland', 'Zimbabwe', 'Azerbaijan']",
inplace = True)

d = d.drop(columns = 'Series Name')
d = d.melt(id_vars = 'Country Name', var_name = 'year', value_name = 'lifexpec_yrs_fem').rename(columns={'Country Name' : 'country'})

# crazy year names
# missing values handled poorly in original
d = d.assign(year = d.year.str.extract(r'(^\d+)', expand = False).astype('int64'),
            lifexpec_yrs_fem = d.lifexpec_yrs_fem.str.replace(r'^\.+$', "NaN").astype('float64'))

# one col per country, one row per year
# 2019/20 are missing
d = d.pivot(index = 'year', values = 'lifexpec_yrs_fem', columns = 'country').dropna()
d.columns.name = ''

d.to_csv('../stor155_sp21/data/wb_lifexpec.csv')

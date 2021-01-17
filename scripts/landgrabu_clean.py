import pandas as pd

d = pd.read_csv('./raw/landgrabu.csv')

d.columns = d.columns.str.lower()
d.columns = d.columns.str.replace(r'[\s,]+', '_')

# stripping human-readable dollars
d = d.apply(lambda x: x.astype('str').str.replace('[$,]', ''))

# na handling
d = d.apply(lambda x: x.str.replace('UNKNOWN|nan', 'NaN'))

# type conversion
# note strictness of assignment using .loc
# https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html
# using u as index anyway
u = d['university']
d = d.loc[:, "total_morrill_acres_found":].apply(lambda x: x.astype('float64'))
d.index = u


d.to_csv('../stor155_sp21/data/landgrabu.csv')
d.to_excel('../stor155_sp21/data/landgrabu.xlsx')

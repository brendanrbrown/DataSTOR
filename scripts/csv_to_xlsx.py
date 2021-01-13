# pandas convert csv to to_excel

import pandas as pd
import argparse
import re

p = argparse.ArgumentParser('csv to excel via pandas')
p.add_argument('file', help='full csv file name to save to xlsx format in same path')

args = p.parse_args()

pat = re.compile(r'\.csv$')
f_out = pat.sub('.xlsx', args.file)
d = pd.read_csv(args.file)
d.to_excel(f_out)

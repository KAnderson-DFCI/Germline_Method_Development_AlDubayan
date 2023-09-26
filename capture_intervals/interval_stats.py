#!python3

import pandas as pd
import numpy as np

import sys

ifile = sys.argv[1]
ofile = sys.argv[2]

intervals = pd.read_csv(ifile, sep='\t',
                        names=['Seq', 'Start', 'End'],
                        index_col=0,
                        dtype={'Start':np.int32, 'End':np.int32})


totals = (intervals.End-intervals.Start).groupby('Seq').sum()
totals.to_csv(ofile, header=False, sep='\t')


# Necessary packages
import sys

sys.path.append('../')
from prov_acquisition.prov_libraries import provenance as pr
from prov_acquisition.prov_libraries import provenance_lib as pr_lib
import pandas as pd
import numpy as np
import time
import argparse
import os
from prov_acquisition.prov_libraries import ProvenanceTracker
import numpy as np

def main(opt):
    output_path = 'prov_results'

    # Specify where to save the processed files as savepath
    savepath = os.path.join(output_path, 'Join')

    df = pd.DataFrame({'key1': ['K0', 'K0', 'K1', 'K2', 'K0'],
                         'key2': ['K0', 'K1', 'K0', 'K1', 'K0'],
                         'A': ['A0', 'A1', 'A2', 'A3', 'A4'],
                         'B': ['B0', 'B1', 'B2', 'B3', 'B4']
                         })
    print('[' + time.strftime("%d/%m-%H:%M:%S") + '] Initialization')
    # Create a new provenance document
    if not opt:
        p = pr.Provenance(df, savepath)
    else:
        savepath = os.path.join(savepath, 'FP')
        p = pr_lib.Provenance(df, savepath)
    tracker=ProvenanceTracker.ProvenanceTracker(df, p)
#    tracker.df=tracker.df.dropna()

    right = pd.DataFrame({'key1': ['K0', 'K1', 'K1', 'K2', ],
                          'key2': ['K0', 'K0', 'K0', 'K0'],
                          'A': ['C0', 'C1', 'C2', 'C3'],
                          'D': ['D0', 'D1', 'D2', 'D3'],
                          'C':['B0', 'B1', 'B2', 'B3']})
    tracker.add_second_df(right)
    tracker.set_join_op(axis=None,on=['key1', 'key2'])
    tracker.df = pd.merge(tracker.df, tracker.second_df, on=['key1', 'key2'], how='right')
    print(tracker.df)
#print(result)
#print(result.columns)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-op', dest='opt', action='store_true', help='Use the optimized capture')
    args = parser.parse_args()
    main(args.opt)



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
    savepath = os.path.join(output_path, 'Testing')

    df = pd.DataFrame({'B': ['B2', 'B3', 'B6', 'B7'],
                  'D': ['D2', 'D3', 'D6', 'D7'],
                  'F': ['F2', 'F3', 'F6', 'F7']},
                 index=[2, 3, 6, 7])
    print('[' + time.strftime("%d/%m-%H:%M:%S") + '] Initialization')
    # Create a new provenance document
    if not opt:
        p = pr.Provenance(df, savepath)
    else:
        savepath = os.path.join(savepath, 'FP')
        p = pr_lib.Provenance(df, savepath)
    tracker=ProvenanceTracker.ProvenanceTracker(df, p)
#    tracker.df=tracker.df.dropna()

    df4 = pd.DataFrame({'A': ['A0', 'A1', 'A2', 'A3'],
                        'B': ['B0', 'B1', 'B2', 'B3'],
                        'C': ['C0', np.nan, 'C2', 'C3'],
                        'D': ['D0', 'D1', 'D2', 'D3']},
                       index=[0, 1, 2, 3])
    tracker.add_second_df(df4)
    tracker.set_join_op(axis=0,on=None)
    tracker.df = pd.concat([df, df4], axis=0, sort=False)
    print(tracker.df)
#print(result)
#print(result.columns)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-op', dest='opt', action='store_true', help='Use the optimized capture')
    args = parser.parse_args()
    main(args.opt)
import pandas as pd

def read_TRANS(filename):

    dtype_spec = {'TID': str}

    DF_TRANS = pd.read_csv(filename,sep=';', dtype=dtype_spec)

    return DF_TRANS

#transactions_entry = read_TRANS('data\TRANS.csv')

#print(transactions_entry)
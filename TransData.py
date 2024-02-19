import pandas as pd

def read_TRANS(filename):

    DF_TRANS = pd.read_csv(filename,sep=';')

    return DF_TRANS

#transactions_entry = read_TRANS('data\TRANS.csv')

#print(transactions_entry)
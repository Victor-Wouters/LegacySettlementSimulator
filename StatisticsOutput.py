import pandas as pd


def calculate_statistics(transactions_entry, settled_transactions, statistics):

    total_input_value = transactions_entry['Value'].sum()
    total_settled_value = settled_transactions['Value'].sum()
    settlement_efficiency = total_settled_value/total_input_value
    print('values:')
    print(total_input_value)
    print(total_settled_value)
    print("Settlement efficiency:")
    print(settlement_efficiency)
    

    new_row = pd.DataFrame({'Settlement efficiency': [settlement_efficiency]})
    statistics = pd.concat([statistics, new_row], ignore_index=True, axis=0)

    #statistics.to_csv('statistics.csv', index=False, sep = ';')

    return statistics


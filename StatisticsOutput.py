import pandas as pd


def calculate_statistics(transactions_entry, settled_transactions):
    statistics = pd.DataFrame()

    total_input_value = transactions_entry['Value'].sum()
    total_settled_value = settled_transactions['Value'].sum()
    settlement_efficiency = total_settled_value/total_input_value
    print('values:')
    print(total_input_value)
    print(total_settled_value)
    print("Settlement efficiency:")
    print(settlement_efficiency)
    statistics['Settlement efficiency'] = [settlement_efficiency]

    statistics.to_csv('statistics.csv', index=False, sep = ';')

    return


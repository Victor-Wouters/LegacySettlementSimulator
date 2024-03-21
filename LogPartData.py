import pandas as pd

def get_partacc_data(participants, transactions_entry): 

    balances_status = pd.DataFrame(columns=['Participant','Account ID','Account Balance'])

    for i, value in participants.items():
        for j in transactions_entry['FromAccountId'].unique():
                new_row = pd.DataFrame([[value.get_part_id(), value.get_account(j).get_account_id(), value.get_account(j).get_balance()]],columns=['Participant', 'Account ID', 'Account Balance'])
                balances_status = pd.concat([balances_status, new_row], ignore_index=True)

        '''
            print('Participant: ')
            print(value.get_part_id())
            print('Account:')
            print(value.get_account(j).get_account_id())
            print('Account Balance: ' )
            print(value.get_account(j).get_balance())'''
    
    return balances_status

def balances_history_calculations(balances_history, participants):
     
     balances_history = balances_history.applymap(lambda x: int(x))
     balances_history.to_csv('balanceHistory\\BalanceHistory.csv', index=False, sep = ';')

     first_two_columns = balances_history.iloc[:, :2]
     remaining_columns = balances_history.iloc[:, 2:].applymap(lambda x: x if x < 0 else 0)
     modified_balances_history = pd.concat([first_two_columns, remaining_columns], axis=1)
     total_credit = remaining_columns.sum()
     total_credit_dataframe = total_credit.to_frame().transpose()
     total_credit_dataframe = total_credit_dataframe.abs()
     total_credit_dataframe.to_csv('balanceHistory\\Total_credit.csv', index=False, sep = ';')

     dfs = {part_id: group for part_id, group in balances_history.groupby('PartID')}
     for part_id, dataframe in dfs.items():
        credit_limit_row = [None, None] + [-(participants[str(part_id)].get_account('0').get_credit_limit())] * (len(dataframe.columns) - 2)
        dataframe.loc['credit limit'] = credit_limit_row
        dataframe.to_csv(f'balanceHistory\\BalanceHistoryPart{part_id}.csv', index=False, sep = ';')
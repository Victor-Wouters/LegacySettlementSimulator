import pandas as pd
import matplotlib.pyplot as plt

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
     balances_history.to_csv('balanceHistoryCSV\\BalanceHistory.csv', index=False, sep = ';')

     #first_two_columns = balances_history.iloc[:, :2]
     remaining_columns = balances_history.iloc[:, 2:].applymap(lambda x: x if x < 0 else 0)
     #modified_balances_history = pd.concat([first_two_columns, remaining_columns], axis=1)
     total_credit = remaining_columns.sum()
     total_credit_dataframe = total_credit.to_frame().transpose()
     total_credit_dataframe = total_credit_dataframe.abs()
     total_credit_dataframe.to_csv('balanceHistoryCSV\\Total_credit.csv', index=False, sep = ';')

     dfs = {part_id: group for part_id, group in balances_history.groupby('PartID')}
     for part_id, dataframe in dfs.items():
        credit_limit_row = [None, None] + [-(participants[str(part_id)].get_account('0').get_credit_limit())] * (len(dataframe.columns) - 2)
        dataframe.loc['credit limit'] = credit_limit_row
        dataframe = dataframe.applymap(lambda x: int(x) if pd.notnull(x) and x != '' else '')

        dataframe = dataframe.transpose()
        dataframe = dataframe.reset_index()
        new_header = dataframe.iloc[1]
        dataframe = dataframe.drop(dataframe.index[0])
        dataframe = dataframe.drop(dataframe.index[0])
        dataframe.columns = new_header
        dataframe.rename(columns={'Account ID': 'Time'}, inplace=True)
        dataframe.columns = [*dataframe.columns[:-1], 'Credit limit']

        dataframe.to_csv(f'balanceHistoryCSV\\BalanceHistoryPart{part_id}.csv', index=False, sep = ';')


        dataframe.set_index('Time', inplace=True)
        plt.figure(figsize=(20, 7))
        for column in dataframe.columns[:-1]:  # Exclude the last column which is 'credit limit'
            if column == 0:
                plt.plot(dataframe.index, dataframe[column], label=f'Cash account')
            else:
                plt.plot(dataframe.index, dataframe[column], label=f'Security {column} account')

        plt.plot(dataframe.index, dataframe['Credit limit'], label='Credit Limit', linestyle='--')
        plt.xlabel('Time')
        plt.ylabel('Value')
        plt.title(f'Participant {part_id}: Account Values Over Time')
        plt.xticks(rotation=90)
        plt.legend()
        #plt.grid(True)
        plt.tight_layout()
        plt.savefig(f'balanceHistoryPNG\\BalanceHistoryPart{part_id}.png')
            
    
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
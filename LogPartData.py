import pandas as pd

def get_partacc_data(participants, transactions_entry): 

    balances_status_monetary = pd.DataFrame(columns=['Participant','Account','Account Balance'])
    balances_status_securities = pd.DataFrame(columns=['Participant','Account','Account Balance'])


    for i, value in participants.items():
        securities_amount = 0
        for j in transactions_entry['FromAccountId'].unique():
            if j == '0':
                new_row = pd.DataFrame([[value.get_part_id(), value.get_account(j).get_account_id(), value.get_account(j).get_balance()]],columns=['Participant', 'Account', 'Account Balance'])
                balances_status_monetary = pd.concat([balances_status_monetary, new_row], ignore_index=True)
            else:
                securities_amount += value.get_account(j).get_balance()

        new_row = pd.DataFrame([[value.get_part_id(), 'Aggregated securities', securities_amount]],columns=['Participant', 'Account', 'Account Balance'])
        balances_status_securities = pd.concat([balances_status_securities, new_row], ignore_index=True)
            
        '''
            print('Participant: ')
            print(value.get_part_id())
            print('Account:')
            print(value.get_account(j).get_account_id())
            print('Account Balance: ' )
            print(value.get_account(j).get_balance())'''
    
    return balances_status_monetary, balances_status_securities
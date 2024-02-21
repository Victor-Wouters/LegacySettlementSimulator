import Simulator

for i, value in Simulator.participants.items():
    for j in Simulator.transactions_entry['FromAccountId'].unique():
        print('Participant: ')
        print(value.get_part_id())
        print('Account:')
        print(value.get_account(j).get_account_id())
        print('Account Balance: ' )
        print(value.get_account(j).get_balance())
   
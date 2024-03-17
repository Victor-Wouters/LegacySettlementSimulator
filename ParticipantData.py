import pandas as pd
import random

def generate_transaction_data(amount_participants, amount_securities, min_balance_value, max_balance_value):

    balance_df = pd.DataFrame(columns=[ 'Part ID','Account ID', 'Balance', 'Credit limit'])

    for participant in range(1,amount_participants+1):
        for account_type in range(amount_securities+1):
            balance_value = random.randint(min_balance_value,max_balance_value)
            credit_limit = random.randint(min_balance_value,max_balance_value)
            new_row_balance = pd.DataFrame({ 'Part ID': participant,'Account ID': account_type, 'Balance': balance_value, 'Credit limit': credit_limit}, index=[0])
            balance_df = pd.concat([balance_df, new_row_balance], ignore_index=True)
    
    return balance_df
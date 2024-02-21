import pandas as pd

def settle(time, matched_transactions, queue_2, settled_transactions, participants, event_log):

    #take 2 matched instructions
    if not matched_transactions.empty:
        matched_linkcodes = matched_transactions['Linkcode'].unique()
        for linkcode in matched_linkcodes:
            instructions_for_processing = matched_transactions[matched_transactions['Linkcode'] == linkcode]

            #check balance:
            for i in [0,1]:
                from_part = instructions_for_processing['FromParticipantId'].iloc[i]
                from_acc = instructions_for_processing['FromAccountId'].iloc[i]
                transaction_value = instructions_for_processing['Value'].iloc[i]
                #current_balance = participants.get(from_part).get_account(from_acc).get_balance()
                print(transaction_value)
                #print(current_balance)


    return matched_transactions, queue_2,  settled_transactions, event_log
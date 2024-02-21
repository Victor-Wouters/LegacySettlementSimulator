import pandas as pd
import Eventlog

def settle(time, matched_transactions, queue_2, settled_transactions, participants, event_log):

    # take 2 matched instructions
    if not matched_transactions.empty:
        matched_linkcodes = matched_transactions['Linkcode'].unique()
        for linkcode in matched_linkcodes:
            instructions_for_processing = matched_transactions[matched_transactions['Linkcode'] == linkcode]
            matched_transactions = matched_transactions[matched_transactions['Linkcode'] != linkcode]
            settlement_confirmation = True

            # split in functions return settlement_confirmation = ..., argument instructions for processing
            # check balance and if not ok: reject settlement
            for i in [0,1]:
                from_part = instructions_for_processing['FromParticipantId'].iloc[i]
                from_acc = instructions_for_processing['FromAccountId'].iloc[i]
                transaction_value = instructions_for_processing['Value'].iloc[i]
                current_balance = participants.get(from_part).get_account(from_acc).get_balance()
                credit_limit = participants.get(from_part).get_account(from_acc).get_credit_limit()
                if transaction_value > (current_balance+credit_limit):
                    settlement_confirmation = False



            # return settled_transactions, queue_2, event_log and give as argument participants and settlement confirmation and instructions for processing
            # if settlement confirmed, edit balances, transactions: move from matched transactions to settled transactions
            if settlement_confirmation == True:
                for i in [0,1]:
                    from_part = instructions_for_processing['FromParticipantId'].iloc[i]
                    from_acc = instructions_for_processing['FromAccountId'].iloc[i]
                    to_part = instructions_for_processing['ToParticipantId'].iloc[i]
                    to_acc = instructions_for_processing['ToAccountId'].iloc[i]
                    if from_acc != to_acc:
                        raise ValueError("Not the same account type, transfer between different account types!")
                    transaction_value = instructions_for_processing['Value'].iloc[i]
                    participants.get(from_part).get_account(from_acc).edit_balance(-transaction_value)
                    participants.get(to_part).get_account(to_acc).edit_balance(transaction_value)
                    # Add to a dict: key: to_part, value: to_acc --> use in the same minute to check if in queue 2 transactions can settle?


                settled_transactions = pd.concat([settled_transactions,instructions_for_processing], ignore_index=True)

                for i in [0,1]:
                    event_log = Eventlog.Add_to_eventlog(event_log, time, instructions_for_processing['TID'].iloc[i], activity='Settled successful')



            # if settlement rejected, move transactions to queue 2
            else:
                queue_2 = pd.concat([queue_2,instructions_for_processing], ignore_index=True)
                for i in [0,1]:
                    event_log = Eventlog.Add_to_eventlog(event_log, time, instructions_for_processing['TID'].iloc[i], activity='Added in queue 2')

    return matched_transactions, queue_2,  settled_transactions, event_log
import pandas as pd
import Eventlog
import datetime

def settle(time, end_matching, start_checking_balance, end_checking_balance, start_settlement_execution, end_settlement_execution, queue_2, settled_transactions, participants, event_log, modified_accounts):

    # take 2 matched instructions
    if not end_matching.empty:
        matched_linkcodes = end_matching['Linkcode'].unique()
        for linkcode in matched_linkcodes:
            instructions_for_processing = end_matching[end_matching['Linkcode'] == linkcode].copy()
            end_matching = end_matching[end_matching['Linkcode'] != linkcode]
            instructions_for_processing["Starttime"] = time
            # Save for 2 sec before checking
            start_checking_balance = pd.concat([start_checking_balance,instructions_for_processing], ignore_index=True)

    # duration of checking balance and credit activity       
    rows_to_remove = []
    for index, instruction_checking in start_checking_balance.iterrows():
        if time == (instruction_checking["Starttime"] + datetime.timedelta(seconds=2)):
            instruction_ended_checking = pd.DataFrame([instruction_checking])
            event_log = Eventlog.Add_to_eventlog(event_log, instruction_checking["Starttime"], time, instruction_ended_checking['TID'], activity='Checking balance and credit')
            end_checking_balance = pd.concat([end_checking_balance,instruction_ended_checking], ignore_index=True)
            rows_to_remove.append(index)
    start_checking_balance = start_checking_balance.drop(rows_to_remove)
    
    # check balance and if not ok: reject settlement    
    if not end_checking_balance.empty:
        matched_linkcodes = end_checking_balance['Linkcode'].unique()
        for linkcode in matched_linkcodes:
            instructions_for_processing = end_checking_balance[end_checking_balance['Linkcode'] == linkcode].copy()
            end_checking_balance = end_checking_balance[end_checking_balance['Linkcode'] != linkcode]

            settlement_confirmation = True
            settlement_confirmation = check_balance(settlement_confirmation, instructions_for_processing, participants) # True check balance

            # if settlement confirmed, edit balances, transactions: move from matched transactions to settled transactions
            if settlement_confirmation == True:
                
                instructions_for_processing["Starttime"] = time
                # Save for 2 sec before settlement execution
                start_settlement_execution = pd.concat([start_settlement_execution,instructions_for_processing], ignore_index=True)
            else:
                queue_2 = pd.concat([queue_2,instructions_for_processing], ignore_index=True)
                # log in eventlog
                for i in [0,1]:
                    event_log = Eventlog.Add_to_eventlog(event_log, time, time, instructions_for_processing['TID'].iloc[i], activity='Waiting in queue unsettled')
    rows_to_remove = []
    for index, instruction_executing in start_settlement_execution.iterrows():
        if time == (instruction_executing["Starttime"] + datetime.timedelta(seconds=2)):
            instruction_ended_executing = pd.DataFrame([instruction_executing])
            event_log = Eventlog.Add_to_eventlog(event_log, instruction_executing["Starttime"], time, instruction_ended_executing['TID'], activity='Settling')
            end_settlement_execution = pd.concat([end_settlement_execution,instruction_ended_executing], ignore_index=True)
            rows_to_remove.append(index)
    start_settlement_execution = start_settlement_execution.drop(rows_to_remove)
    if not end_settlement_execution.empty:
        matched_linkcodes = end_settlement_execution['Linkcode'].unique()
        for linkcode in matched_linkcodes:
            instructions_for_processing = end_settlement_execution[end_settlement_execution['Linkcode'] == linkcode].copy()
            end_settlement_execution = end_settlement_execution[end_settlement_execution['Linkcode'] != linkcode]            
               
            settlement_execution(instructions_for_processing, participants)
            modified_accounts = keep_track_modified_accounts(instructions_for_processing, modified_accounts) # Add to a dict: key: to_part, value: to_acc --> use in the same minute to check if in queue 2 transactions can settle?
            settled_transactions = pd.concat([settled_transactions,instructions_for_processing], ignore_index=True)
            

    return end_matching, start_checking_balance, end_checking_balance, start_settlement_execution, end_settlement_execution,  queue_2,  settled_transactions, event_log

def check_balance(settlement_confirmation, instructions_for_processing, participants):

    for i in [0,1]:
                from_part = instructions_for_processing['FromParticipantId'].iloc[i]
                from_acc = instructions_for_processing['FromAccountId'].iloc[i]
                transaction_value = instructions_for_processing['Value'].iloc[i]
                current_balance = participants.get(from_part).get_account(from_acc).get_balance()
                credit_limit = participants.get(from_part).get_account(from_acc).get_credit_limit()
                if transaction_value > (current_balance+credit_limit):
                    settlement_confirmation = False

    return settlement_confirmation

def settlement_execution(instructions_for_processing, participants):
     
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

def keep_track_modified_accounts(instructions_for_processing, modified_accounts):
      
    for _, instruction in instructions_for_processing.iterrows():
        
        modified_accounts[instruction["ToParticipantId"]] = instruction["ToAccountId"]
        
    return modified_accounts

def retry_settle(time, start_again_checking_balance, end_again_checking_balance, start_again_settlement_execution, end_again_settlement_execution, queue_2, settled_transactions, participants, event_log, modified_accounts): 
     
    if not queue_2.empty:
        for key, value in modified_accounts.items():
            first_instruction = queue_2[(queue_2["FromParticipantId"] == key) & (queue_2["FromAccountId"] == value)] 
            retry_linkcodes = first_instruction['Linkcode'].unique()
            for linkcode in retry_linkcodes:
                instructions_for_processing = queue_2[queue_2["Linkcode"] == linkcode].copy()
                queue_2 = queue_2[queue_2['Linkcode'] != linkcode]
                
                instructions_for_processing["Starttime"] = time
                # Save for 2 sec before checking
                start_again_checking_balance = pd.concat([start_again_checking_balance,instructions_for_processing], ignore_index=True)
                
    rows_to_remove = []
    for index, instruction_checking in start_again_checking_balance.iterrows():
        if time == (instruction_checking["Starttime"] + datetime.timedelta(seconds=2)):
            instruction_ended_checking = pd.DataFrame([instruction_checking])
            event_log = Eventlog.Add_to_eventlog(event_log, instruction_checking["Starttime"], time, instruction_ended_checking['TID'], activity='Checking balance and credit')
            end_again_checking_balance = pd.concat([end_again_checking_balance,instruction_ended_checking], ignore_index=True)
            rows_to_remove.append(index)
    start_again_checking_balance = start_again_checking_balance.drop(rows_to_remove)
    
    if not end_again_checking_balance.empty:
        retry_linkcodes = end_again_checking_balance['Linkcode'].unique()
        for linkcode in retry_linkcodes:
            instructions_for_processing = end_again_checking_balance[end_again_checking_balance["Linkcode"] == linkcode].copy()
            end_again_checking_balance = end_again_checking_balance[end_again_checking_balance['Linkcode'] != linkcode]

            settlement_confirmation = True
            
            # check balance and if not ok: reject settlement
            settlement_confirmation = check_balance(settlement_confirmation, instructions_for_processing, participants)

                # if settlement confirmed, edit balances, transactions: move from matched transactions to settled transactions
            if settlement_confirmation == True:
                instructions_for_processing["Starttime"] = time
                # Save for 2 sec before settlement execution
                start_again_settlement_execution = pd.concat([start_again_settlement_execution,instructions_for_processing], ignore_index=True)
            else: 
                queue_2 = pd.concat([queue_2,instructions_for_processing], ignore_index=True)   # if settlement rejected, move transactions to queue 2
                # log in eventlog
                for i in [0,1]:
                        event_log = Eventlog.Add_to_eventlog(event_log, time, time, instructions_for_processing['TID'].iloc[i], activity='Waiting in queue unsettled') #Waiting again in queue unsettled   
                     
    rows_to_remove = []
    for index, instruction_executing in start_again_settlement_execution.iterrows():
        if time == (instruction_executing["Starttime"] + datetime.timedelta(seconds=2)):
            instruction_ended_executing = pd.DataFrame([instruction_executing])
            event_log = Eventlog.Add_to_eventlog(event_log, instruction_executing["Starttime"], time, instruction_ended_executing['TID'], activity='Settling') #Settling from queue unsettled
            end_again_settlement_execution = pd.concat([end_again_settlement_execution,instruction_ended_executing], ignore_index=True)
            rows_to_remove.append(index)
    start_again_settlement_execution = start_again_settlement_execution.drop(rows_to_remove)

    if not end_again_settlement_execution.empty:
        retry_linkcodes = end_again_settlement_execution['Linkcode'].unique()
        for linkcode in retry_linkcodes:
            instructions_for_processing = end_again_settlement_execution[end_again_settlement_execution['Linkcode'] == linkcode].copy()
            end_again_settlement_execution = end_again_settlement_execution[end_again_settlement_execution['Linkcode'] != linkcode]            
               
            settlement_execution(instructions_for_processing, participants)
            modified_accounts = keep_track_modified_accounts(instructions_for_processing, modified_accounts) # Add to a dict: key: to_part, value: to_acc --> use in the same minute to check if in queue 2 transactions can settle?
            settled_transactions = pd.concat([settled_transactions,instructions_for_processing], ignore_index=True)

    return start_again_checking_balance, end_again_checking_balance, start_again_settlement_execution, end_again_settlement_execution, queue_2,  settled_transactions, event_log
import pandas as pd
import Eventlog
import datetime

def matching(time, opening_time, queue_1, start_matching, insert_transactions, event_log):

    if time < opening_time:
        for _, row_entry in insert_transactions.iterrows():
            row_to_add = pd.DataFrame([row_entry])                                  
            queue_1 = pd.concat([queue_1,row_to_add], ignore_index=True)
            event_log = Eventlog.Add_to_eventlog(event_log, time, time, row_to_add['TID'], activity='Added to queue 1')               
    if time == opening_time:
        queue_1, start_matching, event_log = matching_in_queue(queue_1, start_matching, event_log, time)
    if time >= opening_time: 
        queue_1, start_matching, event_log = matching_insertions(insert_transactions, queue_1, start_matching, event_log, time)
    
    ####### ADD AFTER CLOSE
        
    return queue_1, start_matching, event_log

def matching_insertions(insert_transactions, queue_1, start_matching, event_log, time):

    for _, row_entry in insert_transactions.iterrows():                          # Iterate through the inserted transactions
        matched = False                                                             # Consider inserted transaction as unmatched
        if queue_1.empty:                                                           # Check if queue 1 is empty
            row_to_add = pd.DataFrame([row_entry])                                  

            queue_1 = pd.concat([queue_1,row_to_add], ignore_index=True)            # If empty add this transation to queue 1
            event_log = Eventlog.Add_to_eventlog(event_log, time, time, row_to_add['TID'], activity='Added to queue 1')
            continue                                                                # Skip rest
        
        rows_to_remove = []

        for index, row_queue_1 in queue_1.iterrows():                               # Iterate through queue 1
            if row_entry['Linkcode'] == row_queue_1['Linkcode']:                    # Check if inserted transaction matches a waiting transaction
                matched = True                                                      # If true:
                first_transaction = pd.DataFrame([row_queue_1])
                first_transaction["Starttime"] = time
                second_transaction = pd.DataFrame([row_entry])                      # Add matched transaction to matched_transactions
                second_transaction["Starttime"] = time
                start_matching = pd.concat([start_matching,first_transaction], ignore_index=True)
                start_matching = pd.concat([start_matching,second_transaction], ignore_index=True)
                #event_log = Eventlog.Add_to_eventlog(event_log, time, time, first_transaction['TID'], activity='Matched')
                #event_log = Eventlog.Add_to_eventlog(event_log, time, time, second_transaction['TID'], activity='Matched')

                rows_to_remove.append(index)                                        # Remember the transaction of queue 1

        queue_1 = queue_1.drop(rows_to_remove)                                      # If matched transactions, remove the matched transaction from queue 1

        if matched == False:                                                        # If not matched and queue 1 not empty
            row_to_add = pd.DataFrame([row_entry])                                  
            queue_1 = pd.concat([queue_1,row_to_add], ignore_index=True)            # Add transaction to queue 1, waiting to be matched
            event_log = Eventlog.Add_to_eventlog(event_log, time, time, row_to_add['TID'], activity='Added to queue 1')
            
    return queue_1, start_matching, event_log

def matching_in_queue(queue_1, start_matching, event_log, time):
    if not queue_1.empty:
        Linkcode_counts = queue_1['Linkcode'].value_counts()                                        # Counting occurrences in Linkcode
        matching_Linkcodes = Linkcode_counts[Linkcode_counts == 2].index                            # Identifying Linkcodes that occur more than twice
        matched_rows = queue_1[queue_1['Linkcode'].isin(matching_Linkcodes)]                        # Extracting rows with Linkcodes that occur exactly twice
        matched_rows["Starttime"] = time
        start_matching = pd.concat([start_matching,matched_rows], ignore_index=True)    # Add matched transactions to dataframe
        #for _, row_matched_transactions in matched_rows.iterrows():
        #    event_log = Eventlog.Add_to_eventlog(event_log, time, time, row_matched_transactions['TID'], activity='Matched')
        queue_1 = queue_1[~queue_1['Linkcode'].isin(matching_Linkcodes)]                            # Removing rows with Linkcodes that occur exactly twice from original DataFrame

    return queue_1, start_matching, event_log

def matching_execution(start_matching, end_matching, current_time, event_log):
    
    if not start_matching.empty:

        rows_to_remove = []

        for index, instruction_matching in start_matching.iterrows():
            if current_time == (instruction_matching["Starttime"] + datetime.timedelta(seconds=2)):
                instruction_ended_matching = pd.DataFrame([instruction_matching])
                event_log = Eventlog.Add_to_eventlog(event_log, instruction_matching["Starttime"], current_time, instruction_ended_matching['TID'], activity='Matching')
                end_matching = pd.concat([end_matching,instruction_ended_matching], ignore_index=True)
                rows_to_remove.append(index)                                       

        start_matching = start_matching.drop(rows_to_remove)

    return end_matching, start_matching, event_log
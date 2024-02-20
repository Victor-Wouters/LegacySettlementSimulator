import pandas as pd

def matching_insertions(momentary_transactions, queue_1, matched_transactions):

    for _, row_entry in momentary_transactions.iterrows():                          # Iterate through the inserted transactions
        matched = False                                                             # Consider inserted transaction as unmatched
        if queue_1.empty:                                                           # Check if queue 1 is empty
            row_to_add = pd.DataFrame([row_entry])                                  

            queue_1 = pd.concat([queue_1,row_to_add], ignore_index=True)            # If empty add this transation to queue 1
            continue                                                                # Skip rest
        
        rows_to_remove = []

        for index, row_queue_1 in queue_1.iterrows():                               # Iterate through queue 1
            if row_entry['Linkcode'] == row_queue_1['Linkcode']:                    # Check if inserted transaction matches a waiting transaction
                matched = True                                                      # If true:
                first_transaction = pd.DataFrame([row_queue_1])
                second_transaction = pd.DataFrame([row_entry])                      # Add matched transaction to matched_transactions
                matched_transactions = pd.concat([matched_transactions,first_transaction], ignore_index=True)
                matched_transactions = pd.concat([matched_transactions,second_transaction], ignore_index=True)

                rows_to_remove.append(index)                                        # Remember the transaction of queue 1

        queue_1 = queue_1.drop(rows_to_remove)                                      # If matched transactions, remove the matched transaction from queue 1

        if matched == False:                                                        # If not matched
            row_to_add = pd.DataFrame([row_entry])                                  
            queue_1 = pd.concat([queue_1,row_to_add], ignore_index=True)            # Add transaction to queue 1, waiting to be matched
    
    return queue_1, matched_transactions

def matching_in_queue(queue_1, matched_transactions):
    if not queue_1.empty:
        Linkcode_counts = queue_1['Linkcode'].value_counts()                                        # Counting occurrences in Linkcode
        matching_Linkcodes = Linkcode_counts[Linkcode_counts == 2].index                            # Identifying Linkcodes that occur more than twice
        matched_rows = queue_1[queue_1['Linkcode'].isin(matching_Linkcodes)]                        # Extracting rows with Linkcodes that occur exactly twice
        matched_transactions = pd.concat([matched_transactions,matched_rows], ignore_index=True)    # Add matched transactions to dataframe
        queue_1 = queue_1[~queue_1['Linkcode'].isin(matching_Linkcodes)]                            # Removing rows with Linkcodes that occur exactly twice from original DataFrame

    return queue_1, matched_transactions
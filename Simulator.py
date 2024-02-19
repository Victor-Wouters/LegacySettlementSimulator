import PartAccData
import TransData
import pandas as pd

#read in participant and account data:
participants = PartAccData.read_csv_and_create_participants('data\PART.csv') #Dictionary (key:PartID, value:Part Object)

#read in transaction data:
transactions_entry = TransData.read_TRANS('data\TRANS.csv') #Dataframe

queue_1 = pd.DataFrame()

matched_transactions = pd.DataFrame()

for time in range(1,1140):                                                            # For-loop through every minute of the business day
    momentary_transactions = transactions_entry[transactions_entry['Time']==time]   # Take all the transactions inserted on this minute
    

    ############### Matching mechanism ############################

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
    
    ############### Balance and Credit limit review ############################
    




print(queue_1)
print(matched_transactions)






            

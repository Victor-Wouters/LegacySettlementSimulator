import PartAccData
import TransData
import MatchingMechanism
import pandas as pd

#read in participant and account data:
participants = PartAccData.read_csv_and_create_participants('data\PART.csv') #Dictionary (key:PartID, value:Part Object)

#read in transaction data:
transactions_entry = TransData.read_TRANS('data\TRANS.csv') #Dataframe

queue_1 = pd.DataFrame()

matched_transactions = pd.DataFrame()

opening_time = 24

for time in range(1,1440):                                                            # For-loop through every minute of real-time processing of the business day
    momentary_transactions = transactions_entry[transactions_entry['Time']==time]       # Take all the transactions inserted on this minute

    if time < opening_time:
        for _, row_entry in momentary_transactions.iterrows():
            row_to_add = pd.DataFrame([row_entry])                                  
            queue_1 = pd.concat([queue_1,row_to_add], ignore_index=True)            # If empty add this transation to queue 1
    if time == opening_time:
        print('before open')
        print(queue_1)
        queue_1, matched_transactions = MatchingMechanism.matching_in_queue(queue_1, matched_transactions)
        print('after open')
        print(queue_1)
    if time >= opening_time: 
        queue_1, matched_transactions = MatchingMechanism.matching_insertions(momentary_transactions, queue_1, matched_transactions)

            
    ############### Balance and Credit limit review ############################
    




print(queue_1)
print(matched_transactions)






            

import PartAccData
import TransData
import MatchingMechanism
import ConvertTime
import pandas as pd

#read in participant and account data:
participants = PartAccData.read_csv_and_create_participants('data\PARTV1.csv') #Dictionary (key:PartID, value:Part Object)

#read in transaction data:
transactions_entry = TransData.read_TRANS('data\TRANSV1.csv') #Dataframe

queue_1 = pd.DataFrame()

matched_transactions = pd.DataFrame()

event_log = pd.DataFrame(columns=['TID', 'Time', 'Activity'])

opening_time = 601 #10:00

for time in range(1,1441):                                                            # For-loop through every minute of real-time processing of the business day
    momentary_transactions = transactions_entry[transactions_entry['Time']==time]     # Take all the transactions inserted on this minute

    queue_1, matched_transactions, event_log  = MatchingMechanism.matching(time, opening_time, queue_1, matched_transactions, momentary_transactions, event_log)
            
    ############### Balance and Credit limit review ############################
    




print(queue_1)
print(matched_transactions)
print(event_log)


event_log['Time'] = event_log['Time'].apply(ConvertTime.convert_minutes_to_time)
event_log.to_csv('eventlog.csv', index=False, sep = ';')



            

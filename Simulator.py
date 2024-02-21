import PartAccData
import TransData
import MatchingMechanism
import ConvertTime
import SettlementMechanism
import pandas as pd

#read in participant and account data:
participants = PartAccData.read_csv_and_create_participants('data\PARTsmall.csv') #Dictionary (key:PartID, value:Part Object)

#read in transaction data:
transactions_entry = TransData.read_TRANS('data\TRANSsmall.csv') #Dataframe

queue_1 = pd.DataFrame()    # Transations waiting to be matched
matched_transactions = pd.DataFrame()   # Transactions matched, not yet settled
queue_2  = pd.DataFrame()   # Matched, but unsettled
settled_transactions = pd.DataFrame()   # Transations settled and completed
event_log = pd.DataFrame(columns=['TID', 'Time', 'Activity'])   # Event log with all activities

opening_time = 601 #10:00
#add closing time

for time in range(1,1441):    
                                                            # For-loop through every minute of real-time processing of the business day
    momentary_transactions = transactions_entry[transactions_entry['Time']==time]     # Take all the transactions inserted on this minute

    queue_1, matched_transactions, event_log  = MatchingMechanism.matching(time, opening_time, queue_1, matched_transactions, momentary_transactions, event_log)
    
    matched_transactions, queue_2,  settled_transactions, event_log = SettlementMechanism.settle(time, matched_transactions, queue_2, settled_transactions, participants, event_log)
    
    




print(queue_1)
print(matched_transactions)
print(event_log)
print(settled_transactions)
print(queue_2)


#event_log['Time'] = event_log['Time'].apply(ConvertTime.convert_minutes_to_time)
#event_log.to_csv('eventlog.csv', index=False, sep = ';')



            

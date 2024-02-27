import PartAccData
import TransData
import MatchingMechanism
import SettlementMechanism
import pandas as pd
import pandas as pd
import datetime

today = datetime.date.today()
midnight = datetime.datetime.combine(today, datetime.time.min)


#read in participant and account data:
participants = PartAccData.read_csv_and_create_participants('data\PARTbig.csv') #Dictionary (key:PartID, value:Part Object)

#read in transaction data:
transactions_entry = TransData.read_TRANS('data\TRANSbig.csv') #Dataframe

queue_1 = pd.DataFrame()    # Transations waiting to be matched
start_matching = pd.DataFrame()   # Transactions matched, not yet settled
end_matching = pd.DataFrame()

start_checking_balance = pd.DataFrame()
end_checking_balance = pd.DataFrame()

start_again_checking_balance = pd.DataFrame()
end_again_checking_balance = pd.DataFrame()

start_settlement_execution = pd.DataFrame()
end_settlement_execution = pd.DataFrame()

start_again_settlement_execution = pd.DataFrame()
end_again_settlement_execution = pd.DataFrame()

queue_2  = pd.DataFrame()   # Matched, but unsettled
settled_transactions = pd.DataFrame()   # Transations settled and completed
event_log = pd.DataFrame(columns=['TID', 'Starttime', 'Endtime', 'Activity'])   # Event log with all activities

transactions_entry['Time'] = transactions_entry['Time'].apply(lambda x: midnight + datetime.timedelta(minutes=x-1))
start = datetime.datetime.combine(datetime.date.today(), datetime.time.min)

opening_time = start + datetime.timedelta(minutes=600) #50
print(opening_time)
#add closing time

for i in range(86400):   # For-loop through every minute of real-time processing of the business day 86400

    if i % 10000 == 0:
        print(f"Iteration {i}")

    time = start + datetime.timedelta(seconds=i)
    
    modified_accounts = dict() # Keep track of the accounts modified in this minute to use in queue 2 

    insert_transactions = transactions_entry[transactions_entry['Time']==time]     # Take all the transactions inserted on this minute

    queue_1, start_matching, event_log  = MatchingMechanism.matching(time, opening_time, queue_1, start_matching, insert_transactions, event_log) # Match inserted transactions
    
    end_matching, start_matching, event_log = MatchingMechanism.matching_duration(start_matching, end_matching, time, event_log)
    
    end_matching, start_checking_balance, end_checking_balance, start_settlement_execution, end_settlement_execution, queue_2,  settled_transactions, event_log = SettlementMechanism.settle(time, end_matching, start_checking_balance, end_checking_balance, start_settlement_execution, end_settlement_execution, queue_2, settled_transactions, participants, event_log, modified_accounts) # Settle matched transactions
    
    start_again_checking_balance, end_again_checking_balance, start_again_settlement_execution, end_again_settlement_execution, queue_2,  settled_transactions, event_log = SettlementMechanism.retry_settle(time, start_again_checking_balance, end_again_checking_balance, start_again_settlement_execution, end_again_settlement_execution, queue_2, settled_transactions, participants, event_log, modified_accounts)



print("queue 1:")
print(queue_1)
print("matched:")
print(start_matching)
print("settled:")
print(settled_transactions)
print("queue 2:")
print(queue_2)
print("event log:")
print(event_log)


event_log.to_csv('eventlogtest.csv', index=False, sep = ';')



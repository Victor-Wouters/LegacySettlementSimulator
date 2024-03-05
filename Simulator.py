import PartAccData
import TransData
import MatchingMechanism
import SettlementMechanism
import Validation
import pandas as pd
import pandas as pd
import datetime
import time

start_time = datetime.datetime.now()
print("Start Time:", start_time.strftime('%Y-%m-%d %H:%M:%S'))
#read in participant and account data:
participants = PartAccData.read_csv_and_create_participants('data\PARTICIPANTS1.csv') #Dictionary (key:PartID, value:Part Object)

#read in transaction data:
transactions_entry = TransData.read_TRANS('data\TRANSACTION1.csv') #Dataframe

queue_received = pd.DataFrame() # Transactions inserted before and after opening

queue_1 = pd.DataFrame()    # Transations waiting to be matched

start_validating = pd.DataFrame()
end_validating = pd.DataFrame()

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


earliest_datetime = transactions_entry['Time'].min()
earliest_datetime = earliest_datetime.date()
start = earliest_datetime

latest_datetime = transactions_entry['Time'].max()
latest_datetime = latest_datetime + datetime.timedelta(days=1)
latest_datetime = latest_datetime.date()
end = latest_datetime

midnight = datetime.time(0,0,0)
start = datetime.datetime.combine(start, midnight)
end = datetime.datetime.combine(end, midnight)
total_seconds = int((end - start).total_seconds())


opening_time = datetime.time(10,0,0)
closing_time = datetime.time(19,0,0)


for i in range(total_seconds):   # For-loop through every minute of real-time processing of the business day 86400

    if i % 8640 == 0:
        percent_compleet = round((i/total_seconds)*100)
        bar = '█' * percent_compleet + '-' * (100 - percent_compleet)
        print(f'\r|{bar}| {percent_compleet}% ', end='')

    time = start + datetime.timedelta(seconds=i)

    modified_accounts = dict() # Keep track of the accounts modified in this minute to use in queue 2 

    insert_transactions = transactions_entry[transactions_entry['Time']==time]     # Take all the transactions inserted on this minute

    end_validating, start_validating, event_log = Validation.validating_duration(insert_transactions, start_validating, end_validating, time, event_log)
    
    queue_received, queue_1, start_matching, end_validating, event_log  = MatchingMechanism.matching(time, opening_time, closing_time, queue_received, queue_1, start_matching, end_validating, event_log) # Match inserted transactions

    end_matching, start_matching, event_log = MatchingMechanism.matching_duration(start_matching, end_matching, time, event_log)
    
    time_hour = time.time()
    if time_hour >= opening_time and time_hour < closing_time: # Guarantee closed
        end_matching, start_checking_balance, end_checking_balance, start_settlement_execution, end_settlement_execution, queue_2,  settled_transactions, event_log = SettlementMechanism.settle(time, end_matching, start_checking_balance, end_checking_balance, start_settlement_execution, end_settlement_execution, queue_2, settled_transactions, participants, event_log, modified_accounts) # Settle matched transactions
    
        start_again_checking_balance, end_again_checking_balance, start_again_settlement_execution, end_again_settlement_execution, queue_2,  settled_transactions, event_log = SettlementMechanism.retry_settle(time, start_again_checking_balance, end_again_checking_balance, start_again_settlement_execution, end_again_settlement_execution, queue_2, settled_transactions, participants, event_log, modified_accounts)
    if time_hour == closing_time:       # Empty queue 1 at close and put in instructions received
        queue_received, queue_1, event_log = MatchingMechanism.clear_queue_unmatched(queue_received, queue_1, time, event_log)


print("queue 1:")
print(queue_1)
print("queue received:")
print(queue_received)
print("matched:")
print(start_matching)
print("settled:")
print(settled_transactions)
print("queue 2:")
print(queue_2)
print("event log:")
print(event_log)


event_log.to_csv('eventlogtest.csv', index=False, sep = ';')

end_time = datetime.datetime.now()
print("End Time:", end_time.strftime('%Y-%m-%d %H:%M:%S'))
duration = end_time - start_time
print("Execution Duration:", duration)

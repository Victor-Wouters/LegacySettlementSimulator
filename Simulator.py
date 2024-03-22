import PartAccData
import TransData
import MatchingMechanism
import SettlementMechanism
import Validation
import StatisticsOutput
import LogPartData
import SaveQueues
#import Generator
import pandas as pd
import datetime
import time

statistics = pd.DataFrame(columns=['Settlement efficiency'])

for j in range(0,1):

    #Generator.generate_data()

    start_time = datetime.datetime.now()
    print("Start Time:", start_time.strftime('%Y-%m-%d %H:%M:%S'))

    #read in participant and account data:
    participants = PartAccData.read_csv_and_create_participants('InputData\PARTICIPANTS1.csv') #Dictionary (key:PartID, value:Part Object)

    #read in transaction data:
    transactions_entry = TransData.read_TRANS('InputData\TRANSACTION1.csv') #Dataframe of all transactions

     # Keep in mind to have the correct format when reading participants in!
    balances_history = pd.DataFrame(columns=['PartID', "Account ID"])
    for i, value in participants.items():
        for j in transactions_entry['FromAccountId'].unique():
            new_row = pd.DataFrame([[value.get_part_id(), value.get_account(j).get_account_id()]],columns=['PartID', 'Account ID'])
            balances_history = pd.concat([balances_history, new_row], ignore_index=True)
    
    SE_over_time = pd.DataFrame()
    cumulative_inserted = pd.DataFrame()
    total_unsettled_value_over_time = pd.DataFrame()

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
    settled_transactions = pd.DataFrame()   # Transactions settled and completed
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


    opening_time = datetime.time(1,30,0)
    print(opening_time)
    closing_time = datetime.time(19,30,00)
    print(closing_time)

    #for i in range(5000): #for debugging
    for i in range(total_seconds):   # For-loop through every minute of real-time processing of the business day 86400

        if i % 8640 == 0:
            percent_complete = round((i/total_seconds)*100)
            bar = '█' * percent_complete + '-' * (100 - percent_complete)
            print(f'\r|{bar}| {percent_complete}% ', end='')

        time = start + datetime.timedelta(seconds=i)

        modified_accounts = dict() # Keep track of the accounts modified in this minute to use in queue 2 

        insert_transactions = transactions_entry[transactions_entry['Time']==time]     # Take all the transactions inserted on this minute

        cumulative_inserted = pd.concat([cumulative_inserted,insert_transactions], ignore_index=True)
        
        end_validating, start_validating, event_log = Validation.validating_duration(insert_transactions, start_validating, end_validating, time, event_log)
        
        queue_received, queue_1, start_matching, end_validating, event_log  = MatchingMechanism.matching(time, opening_time, closing_time, queue_received, queue_1, start_matching, end_validating, event_log) # Match inserted transactions

        end_matching, start_matching, event_log = MatchingMechanism.matching_duration(start_matching, end_matching, time, event_log)
        
        time_hour = time.time()
        if time_hour >= opening_time and time_hour < closing_time: # Guarantee closed
            end_matching, start_checking_balance, end_checking_balance, start_settlement_execution, end_settlement_execution, queue_2,  settled_transactions, event_log = SettlementMechanism.settle(time, end_matching, start_checking_balance, end_checking_balance, start_settlement_execution, end_settlement_execution, queue_2, settled_transactions, participants, event_log, modified_accounts) # Settle matched transactions
        
            start_again_checking_balance, end_again_checking_balance, start_again_settlement_execution, end_again_settlement_execution, queue_2,  settled_transactions, event_log = SettlementMechanism.atomic_retry_settle(time, start_again_checking_balance, end_again_checking_balance, start_again_settlement_execution, end_again_settlement_execution, queue_2, settled_transactions, participants, event_log, modified_accounts)
        if time_hour == closing_time:       # Empty queue 1 at close and put in instructions received
            queue_received, queue_1, event_log = MatchingMechanism.clear_queue_unmatched(queue_received, queue_1, time, event_log)
        
        if i % 900 == 0:
            balances_status = LogPartData.get_partacc_data(participants, transactions_entry)
            time_hour_str = time_hour.strftime('%H:%M:%S')
            balances_history[time_hour_str] = balances_status['Account Balance']
            SE_timepoint = StatisticsOutput.calculate_SE_over_time(settled_transactions, cumulative_inserted)
            SE_over_time[time_hour_str] = SE_timepoint['Settlement efficiency']
            total_unsettled_value_timepoint = StatisticsOutput.calculate_total_value_unsettled(queue_2)
            total_unsettled_value_over_time[time_hour_str] = total_unsettled_value_timepoint['Total value unsettled']
        if i == (total_seconds-1):
            balances_status = LogPartData.get_partacc_data(participants, transactions_entry)
            time_hour_str = time_hour.strftime('%H:%M:%S')
            balances_history[time_hour_str] = balances_status['Account Balance']
            SE_timepoint = StatisticsOutput.calculate_SE_over_time(settled_transactions, cumulative_inserted)
            SE_over_time[time_hour_str] = SE_timepoint['Settlement efficiency']
            total_unsettled_value_timepoint = StatisticsOutput.calculate_total_value_unsettled(queue_2)
            total_unsettled_value_over_time[time_hour_str] = total_unsettled_value_timepoint['Total value unsettled']


    SaveQueues.save_queues(queue_1,queue_received,settled_transactions,queue_2)
    statistics = StatisticsOutput.calculate_total_SE(transactions_entry, settled_transactions, statistics)
    StatisticsOutput.calculate_SE_per_participant(transactions_entry, settled_transactions)

    #event_log.to_csv(f'eventlog{j}.csv', index=False, sep = ';')
    event_log.to_csv('eventlog\\eventlog.csv', index=False, sep = ';')

    LogPartData.balances_history_calculations(balances_history, participants)

    end_time = datetime.datetime.now()
    print("End Time:", end_time.strftime('%Y-%m-%d %H:%M:%S'))
    duration = end_time - start_time
    print("Execution Duration:", duration)
    
total_unsettled_value_over_time.to_csv('statistics\\total_unsettled_value_over_time.csv', index=False, sep = ';')
SE_over_time.to_csv('statistics\\SE_over_time.csv', index=False, sep = ';')
statistics.to_csv('statistics\\statistics.csv', index=False, sep = ';')


import pandas as pd


def calculate_total_SE(transactions_entry, settled_transactions, statistics):

    if not settled_transactions.empty:

        total_input_value = transactions_entry['Value'].sum()
        total_settled_value = settled_transactions['Value'].sum()
        settlement_efficiency = total_settled_value/total_input_value
        print("\nSettlement efficiency:")
        print(settlement_efficiency)
        

        new_row = pd.DataFrame({'Settlement efficiency': [settlement_efficiency]})
        statistics = pd.concat([statistics, new_row], ignore_index=True, axis=0)

    #statistics.to_csv('statistics.csv', index=False, sep = ';')

    return statistics

def calculate_SE_per_participant(transactions_entry,settled_transactions):

    if not settled_transactions.empty:

        settled_part = settled_transactions.groupby('FromParticipantId')['Value'].sum().reset_index()
        input_part = transactions_entry.groupby('FromParticipantId')['Value'].sum().reset_index()
        merged_df = pd.merge(settled_part, input_part, on='FromParticipantId', suffixes=('_settled', '_input'))
        merged_df['settled_input_ratio'] = merged_df['Value_settled'] / merged_df['Value_input']
        merged_df.to_csv('statistics\\SE_per_participant.csv', index=False, sep = ';')

def calculate_SE_over_time(settled_transactions, cumulative_inserted):

    SE_timepoint = pd.DataFrame(columns=['Settlement efficiency'])

    if not cumulative_inserted.empty and not settled_transactions.empty:
        
        total_input_value = cumulative_inserted['Value'].sum()
        total_settled_value = settled_transactions['Value'].sum()
        settlement_efficiency = total_settled_value/total_input_value
        new_row = pd.DataFrame({'Settlement efficiency': [settlement_efficiency]})
        SE_timepoint = pd.concat([SE_timepoint, new_row], ignore_index=True)

    else: 
        new_row = pd.DataFrame({'Settlement efficiency': [0]})
        SE_timepoint = pd.concat([SE_timepoint, new_row], ignore_index=True)

    return SE_timepoint

def calculate_total_value_unsettled(queue_2):

    total_unsettled_value_timepoint = pd.DataFrame(columns=['Total value unsettled'])

    if not queue_2.empty :
        total_value_unsettled = queue_2['Value'].sum()
        new_row = pd.DataFrame({'Total value unsettled': [int(total_value_unsettled)]})
        total_unsettled_value_timepoint = pd.concat([total_unsettled_value_timepoint, new_row], ignore_index=True)
    else: 
        new_row = pd.DataFrame({'Total value unsettled': [0]})
        total_unsettled_value_timepoint = pd.concat([total_unsettled_value_timepoint, new_row], ignore_index=True)

    return total_unsettled_value_timepoint
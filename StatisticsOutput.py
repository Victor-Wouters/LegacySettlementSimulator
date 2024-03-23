import pandas as pd
import matplotlib.pyplot as plt


def calculate_total_SE(cumulative_inserted, settled_transactions, final_settlement_efficiency):

    if not settled_transactions.empty:

        total_input_value = cumulative_inserted['Value'].sum()
        total_settled_value = settled_transactions['Value'].sum()
        settlement_efficiency = total_settled_value/total_input_value
        print("\nSettlement efficiency:")
        print(settlement_efficiency)
        

        new_row = pd.DataFrame({'Settlement efficiency': [settlement_efficiency]})
        final_settlement_efficiency = pd.concat([final_settlement_efficiency, new_row], ignore_index=True, axis=0)

    #statistics.to_csv('statistics.csv', index=False, sep = ';')

    return final_settlement_efficiency

def calculate_SE_per_participant(cumulative_inserted,settled_transactions):

    if not settled_transactions.empty:

        settled_part = settled_transactions.groupby('FromParticipantId')['Value'].sum().reset_index()
        input_part = cumulative_inserted.groupby('FromParticipantId')['Value'].sum().reset_index()
        merged_df = pd.merge(settled_part, input_part, on='FromParticipantId', suffixes=('_settled', '_input'))
        merged_df['settled_input_ratio'] = merged_df['Value_settled'] / merged_df['Value_input']
        #merged_df['settled_input_ratio'] = merged_df['settled_input_ratio'].apply(lambda x: "{:.2%}".format(x))
        plt.figure(figsize=(20, 7))
        merged_df['FromParticipantId'] = merged_df['FromParticipantId'].astype(int)
        sorted_df = merged_df.sort_values(by='FromParticipantId')
        bars = plt.bar(sorted_df['FromParticipantId'], sorted_df['settled_input_ratio'], color='limegreen')
        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval + 0.005, "{:.2%}".format(yval), ha='center', va='bottom')
        plt.title('Settlement efficiency for each Participant')
        plt.xlabel('Participant')
        plt.ylabel('Settlement efficiency')
        plt.xticks(merged_df['FromParticipantId'])
        plt.grid(axis='y')
        plt.tight_layout()
        plt.savefig(f'statisticsPNG\\Settlement_efficiency_for_each_Participant.png')
        merged_df.to_csv('statisticsCSV\\SE_per_participant.csv', index=False, sep = ';')

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

def statistics_generate_output(total_unsettled_value_over_time, SE_over_time, final_settlement_efficiency):

    unsettled_plot = total_unsettled_value_over_time.iloc[0]
    plt.figure(figsize=(20, 7))
    plt.plot(unsettled_plot.index, unsettled_plot.values)
    plt.title('Total unsettled value over time')
    plt.xlabel('Time')
    plt.ylabel('Value')
    plt.xticks(rotation=90)
    plt.grid(axis='y')
    plt.tight_layout()
    plt.savefig(f'statisticsPNG\\total_unsettled_value_over_time.png')

    SE_plot = SE_over_time.iloc[0]
    plt.figure(figsize=(20, 7))
    plt.plot(SE_plot.index, SE_plot.values)
    plt.title('Cumulative settlement efficiency over time')
    plt.xlabel('Time')
    plt.ylabel('Value')
    plt.xticks(rotation=90)
    plt.grid(axis='y')
    plt.tight_layout()
    plt.savefig(f'statisticsPNG\\SE_over_time.png')

    total_unsettled_value_over_time.to_csv('statisticsCSV\\total_unsettled_value_over_time.csv', index=False, sep = ';')
    SE_over_time.to_csv('statisticsCSV\\SE_over_time.csv', index=False, sep = ';')
    final_settlement_efficiency.to_csv('statisticsCSV\\final_settlement_efficiency.csv', index=False, sep = ';')
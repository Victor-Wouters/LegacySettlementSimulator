import pandas as pd
import Eventlog
import datetime


def validating_duration(insert_transactions, start_validating, end_validating, current_time, event_log):
    
    insert_transactions = insert_transactions.copy()
    insert_transactions["Starttime"] = current_time
    start_validating = pd.concat([start_validating,insert_transactions], ignore_index=True)

    if not start_validating.empty:

        rows_to_remove = []

        for index, instruction_validating in start_validating.iterrows():
            if current_time == (instruction_validating["Starttime"] + datetime.timedelta(seconds=2)):
                instruction_ended_validating = pd.DataFrame([instruction_validating])
                event_log = Eventlog.Add_to_eventlog(event_log, instruction_validating["Starttime"], current_time, instruction_ended_validating['TID'], activity='Validating')
                end_validating = pd.concat([end_validating,instruction_ended_validating], ignore_index=True)
                rows_to_remove.append(index)                                       

        start_validating = start_validating.drop(rows_to_remove)

    return end_validating, start_validating, event_log
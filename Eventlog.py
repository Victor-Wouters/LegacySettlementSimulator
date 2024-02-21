import pandas as pd

def Add_to_eventlog(event_log, time, ID, activity):
    if isinstance(ID, pd.Series):
        ID_value = ID.iloc[0]
    else: ID_value = ID
    new_row = pd.DataFrame([[ID_value, time, activity]],columns=['TID', 'Time', 'Activity'])
    event_log = pd.concat([event_log, new_row], ignore_index=True)

    return event_log
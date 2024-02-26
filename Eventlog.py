import pandas as pd

def Add_to_eventlog(event_log, starttime, endtime, ID, activity):
    if isinstance(ID, pd.Series):
        ID_value = ID.iloc[0]
    else: ID_value = ID
    new_row = pd.DataFrame([[ID_value, starttime, endtime, activity]],columns=['TID', 'Starttime', 'Endtime', 'Activity'])
    event_log = pd.concat([event_log, new_row], ignore_index=True)

    return event_log
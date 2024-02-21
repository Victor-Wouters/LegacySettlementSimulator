

def convert_minutes_to_time(minute_of_day):
    if minute_of_day < 1 or minute_of_day > 1440:
        raise ValueError("Minute of the day must be between 1 and 1440")

    hours = (minute_of_day - 1) // 60
    minutes = (minute_of_day - 1) % 60

    return f"{hours:02d}:{minutes:02d}"
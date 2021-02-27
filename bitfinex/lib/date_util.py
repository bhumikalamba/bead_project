import arrow


def get_yesterday_date():
    yesterday_date = arrow.utcnow().to('Asia/Singapore').shift(days=-1).date()
    # unix_time = today_utc.timestamp
    return yesterday_date


def get_todays_date():
    today_date = arrow.utcnow().to('Asia/Singapore').date()
    return today_date


def get_datetime_bounds(yesterday_date):
    end_date = arrow.utcnow().date()
    return f'{yesterday_date}' + ' 00:00', f'{str(end_date)}' + ' 00:00'


def convert_date_str_to_unix(date_str):
    # takes the string as UTC time.
    unix = arrow.get(date_str, 'YYYY-MM-DD HH:mm').replace(tzinfo='Asia/Singapore').timestamp
    return unix


# function applies of a dataframe
def convert_time_stamp_to_int_date(row):
    output = int(row['time'].strftime("%Y%m%d"))
    # no need to use dt here since it is a row not a dataframe. df['time'].dt.tz_convert
    return output

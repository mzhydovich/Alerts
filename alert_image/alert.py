import pandas as pd
import logging
from datetime import datetime


def get_records_with_more_than_10_errors_per_minute(df):
    """get dataframe with records with more than 10 errors per minute"""

    fatal_error_data = df[df['severity'] == 'Error']
    fatal_error_data_grouped_by_minute = fatal_error_data.groupby([fatal_error_data['date'].dt.year, fatal_error_data['date'].dt.month, fatal_error_data['date'].dt.day, fatal_error_data['date'].dt.hour, fatal_error_data['date'].dt.minute])
    fatal_errors_more_than_10_per_minute = fatal_error_data_grouped_by_minute['severity'].value_counts().loc[lambda errors: errors > 10]

    return fatal_errors_more_than_10_per_minute


def get_records_with_more_than_10_errors_per_hour(df):
    """get dataframe with records with more than 10 errors per hour for specific bundle_id"""

    fatal_error_data = df[df['severity'] == 'Error']
    fatal_error_data_grouped_by_hour = fatal_error_data.groupby([fatal_error_data['bundle_id'], fatal_error_data['date'].dt.year, fatal_error_data['date'].dt.month, fatal_error_data['date'].dt.day, fatal_error_data['date'].dt.hour])
    fatal_errors_more_than_10_per_hour = fatal_error_data_grouped_by_hour['severity'].value_counts().loc[lambda errors: errors > 10]

    return fatal_errors_more_than_10_per_hour


# read data
data = pd.read_csv("data.csv")

# rename columns
data = data.rename(columns={
    '0': 'error_code',
    '1': 'error_message',
    '2': 'severity',
    '3': 'log_location',
    '4': 'mode',
    '5': 'model',
    '6': 'graphics',
    '7': 'session_id',
    '8': 'sdkv',
    '9': 'test_mode',
    '10': 'flow_id',
    '11': 'flow_type',
    '12': 'sdk_date',
    '13': 'publisher_id',
    '14': 'game_id',
    '15': 'bundle_id',
    '16': 'appv',
    '17': 'language',
    '18': 'os',
    '19': 'adv_id',
    '20': 'gdpr',
    '21': 'ccpa',
    '22': 'country_code',
    '23': 'date',
})

# convert date from timestamp
data['date'] = [datetime.fromtimestamp(d) for d in data['date']]

# logging data
logging.basicConfig(level=logging.INFO)
logging.info('\n\t'+ get_records_with_more_than_10_errors_per_minute(data).to_string().replace('\n', '\n\t'))
logging.info('\n\t'+ get_records_with_more_than_10_errors_per_hour(data).to_string().replace('\n', '\n\t'))

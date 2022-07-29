import logging
import json
import time
import os

from datetime import datetime
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from watchdog.events import FileCreatedEvent


def prepared_data(file_path: str, columns: dict) -> pd.DataFrame:
    """pre-processing data for future analysis: read, rename columns and fix date format"""

    # read data
    data = pd.read_csv(file_path)

    # rename columns
    data = data.rename(columns={str(i): col for i, col in enumerate(columns)})

    # convert date from timestamp
    data['date'] = [datetime.fromtimestamp(d) for d in data['date']]

    return data


def get_records_with_more_than_10_errors_per_minute(df: pd.DataFrame) -> pd.Series:
    """get dataframe with records with more than 10 errors per minute"""

    fatal_error_data = df[df['severity'] == 'Error']
    fatal_error_data_grouped_by_minute = fatal_error_data.groupby([fatal_error_data['date'].dt.year, fatal_error_data['date'].dt.month, fatal_error_data['date'].dt.day, fatal_error_data['date'].dt.hour, fatal_error_data['date'].dt.minute])
    fatal_errors_more_than_10_per_minute = fatal_error_data_grouped_by_minute['severity'].value_counts().loc[lambda errors: errors > 10]
    
    return fatal_errors_more_than_10_per_minute


def get_records_with_more_than_10_errors_per_hour(df: pd.DataFrame) -> pd.Series:
    """get dataframe with records with more than 10 errors per hour for specific bundle_id"""

    fatal_error_data = df[df['severity'] == 'Error']
    fatal_error_data_grouped_by_hour = fatal_error_data.groupby([fatal_error_data['bundle_id'], fatal_error_data['date'].dt.year, fatal_error_data['date'].dt.month, fatal_error_data['date'].dt.day, fatal_error_data['date'].dt.hour])
    fatal_errors_more_than_10_per_hour = fatal_error_data_grouped_by_hour['severity'].value_counts().loc[lambda errors: errors > 10]

    return fatal_errors_more_than_10_per_hour


def on_created(event: FileCreatedEvent) -> None:
    """handle created files"""
    print(f"{event.src_path} has been created!")

    data = prepared_data(event.src_path, variables['columns'])

    log_data = get_records_with_more_than_10_errors_per_minute(data)
    logging.info('\n\t'+ log_data.to_string().replace('\n', '\n\t'))

    log_data = get_records_with_more_than_10_errors_per_hour(data)
    logging.info('\n\t'+ log_data.to_string().replace('\n', '\n\t'))
    

# read variables from json
json_file = open("../config.json")
variables = json.load(json_file)
json_file.close()

# config logger
logging.basicConfig(level=variables['logging_level'])

# check existing files
for file in os.listdir(variables['dir']):
    full_path = os.path.join(variables['dir'], file)

    print(f"{full_path} already exists!")

    data = prepared_data(full_path, variables['columns'])

    log_data = get_records_with_more_than_10_errors_per_minute(data)
    logging.info('\n\t'+ log_data.to_string().replace('\n', '\n\t'))

    log_data = get_records_with_more_than_10_errors_per_hour(data)
    logging.info('\n\t'+ log_data.to_string().replace('\n', '\n\t'))

# create event handler
patterns = ["*.csv"]
ignore_patterns = None
ignore_directories = False
case_sensitive = True
my_event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)
my_event_handler.on_created = on_created

# create observer
my_observer = Observer()
my_observer.schedule(my_event_handler, variables['dir'], recursive=True)

my_observer.start()
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    # press ctrl+c to stop observer
    my_observer.stop()

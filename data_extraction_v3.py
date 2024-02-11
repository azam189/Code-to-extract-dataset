"""
This script extracts necessary data from different parts of the Yelp dataset in different formats and compresses it into 2 major files/datasets:
1. 'output_1' is the main dataframe consisting of businesses and their attributes expanded by check-in information. Only Businesses in Missouri are kept as this is the state with the highest variance in weather.
2. 'yelp_academic_dataset_review_reduced' holds review data for all relevant businesses from the main dataframe and helps to reduce file size significantly (20x).
"""

import pandas as pd
from pandas import json_normalize
import numpy as np
import json
import os
import time
from datetime import datetime

def load_and_process_business_data(input_path):
    # Flatten JSON format
    df_business = pd.read_csv(input_path)
    df_business['j'] = df_business['j'].apply(json.loads)
    flattened_data = json_normalize(df_business['j'])
    df_business = pd.concat([df_business.drop('j', axis=1), flattened_data], axis=1)

    # Filter and keep only businesses from Missouri to keep the dataset small
    df_business = df_business[df_business['state'] == 'MO']

    # Drop empty columns
    empty_cols_business = [col for col in df_business.columns if df_business[col].isnull().all()]
    df_business.drop(empty_cols_business, axis=1, inplace=True)
    return df_business

def load_and_filter_reviews(input_path, start_date, end_date):
    df_reviews = pd.read_json(input_path, lines=True)
    df_reviews['date'] = pd.to_datetime(df_reviews['date'])
    return df_reviews

def create_review_table(df_reviews, business_ids):
    filtered_chunks = [chunk[chunk['business_id'].isin(business_ids)] for chunk in np.array_split(df_reviews, 24)]
    return pd.concat(filtered_chunks)

def get_checkin_date(date_str, position):
    if isinstance(date_str, str):
        dates = date_str.split(',')
        date = dates[0 if position == 'first' else -1].strip()
        return datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    return datetime.min

def main():
    start_time = time.time()

    # Set working directory to where python file is located
    wd = os.path.dirname(os.path.abspath(__file__))
    os.chdir(wd)

    # Set paths and timeframe
    input_path_business = 'business_data.csv'
    input_path_checkin = 'yelp_academic_dataset_checkin.json'
    input_path_reviews = 'yelp_academic_dataset_review.json'
    timeframe_start = datetime.strptime('2018-01-01', '%Y-%m-%d')
    timeframe_end = datetime.strptime('2019-12-31', '%Y-%m-%d')

    # Load data
    df_business = load_and_process_business_data(input_path_business)
    df_checkin = pd.read_json(input_path_checkin, lines=True)
    df_reviews = load_and_filter_reviews(input_path_reviews, timeframe_start, timeframe_end)

    # Merge dataframes
    df = pd.merge(df_business, df_checkin, on='business_id', how='left')

    # Process review data
    if not os.path.exists('yelp_academic_dataset_review_reduced.csv'):
        business_id_set = set(df['business_id'].unique())
        df_reviews_reduced = create_review_table(df_reviews, business_id_set)
        df_reviews_reduced.to_csv('yelp_academic_dataset_review_reduced.csv', index=False)

    # Filter data based on check-in dates
    df['last_checkin'] = df['date'].apply(lambda x: get_checkin_date(x, 'last'))
    df['first_checkin'] = df['date'].apply(lambda x: get_checkin_date(x, 'first'))
    df = df[(df['last_checkin'] >= timeframe_start) & (df['first_checkin'] <= timeframe_end)]

    # Remove duplicates
    duplicates = df['business_id'].duplicated().sum()
    print(f"Number of duplicates in 'business_id': {duplicates}")
    if duplicates > 0:
        df.drop_duplicates(subset='business_id', inplace=True)

    # Export Data
    with pd.ExcelWriter('output_1.xlsx') as writer:
        df.to_excel(writer, sheet_name='sheet_1', index=False)
    df.to_csv('output_1.csv', index=False)

    print('--- Runtime: %s seconds ---' % (time.time() - start_time))

if __name__ == "__main__":
    main()

import pandas as pd
import numpy as np

import os

#os.chdir('PI_Bots_President')
DATASETS_PATH = '../'

TWIBOT20_FOLDER = 'Twibot-20'
TWIBOT20_PROBE_TIME = '2020-09-06'

def feature_engineering(profiles):
    profiles['user_age'] = (profiles['probe_date'] - profiles['created_at']).dt.total_seconds()

    profiles['tweet_freq'] = profiles['statuses_count'].div(profiles['user_age'])
    profiles['followers_growth_rate'] = profiles['followers_count'].div(profiles['user_age'])
    profiles['friends_growth_rate'] = profiles['friends_count'].div(profiles['user_age'])
    profiles['favourites_growth_rate'] = profiles['favourites_count'].div(profiles['user_age'])
    profiles['listed_growth_rate'] = profiles['listed_count'].div(profiles['user_age'])
    
    profiles['followers_friends_ratio'] = np.nan_to_num(profiles['followers_count'].div(profiles['friends_count']), posinf=0.0)
    
    profiles['name_length'] = profiles['name'].str.len()
    profiles['screen_name_length'] = profiles['screen_name'].str.len()
    profiles['description_length'] = profiles['description'].str.len()
    
    profiles['num_digits_in_name'] = profiles['name'].str.count('\d')
    profiles['num_digits_in_screen_name'] = profiles['screen_name'].str.count('\d')

    profiles['has_url'] = profiles['url'].isna()
    #profiles['has_location'] = profiles['location'].isna()

    #profiles.drop(['url', 'location'], axis=1, inplace=True)
    
    return profiles

def remove_useless_api_columns(profiles):
    return profiles.drop(['protected', 'contributors_enabled', 'is_translator', 'is_translation_enabled', 'utc_offset', 'time_zone', 'lang', 'profile_background_color', 'profile_background_image_url', 'profile_image_url', 'profile_link_color', 'profile_sidebar_border_color', 'profile_sidebar_fill_color', 'profile_text_color', 'profile_location', 'entities', 'has_extended_profile'], axis=1, errors='ignore')

def remove_cresci17_extra_columns(profiles):
    return profiles.drop(['following', 'timestamp', 'test_set_1', 'test_set_2', 'crawled_at', 'notifications', 'profile_banner_url', 'follow_request_sent'], axis=1)

def remove_midterm18_extra_columns(profiles):
    return profiles.drop(['tid'], axis=1)

def load_twibot20(dataset='train', probe=TWIBOT20_PROBE_TIME, feature_engineering=None):
    probe_date = pd.to_datetime(probe, utc=True)

    raw_json = pd.read_json(
        os.path.join(
            DATASETS_PATH, 
            TWIBOT20_FOLDER, 
            f"{dataset}.json"
        )
    )

    profiles = pd.DataFrame(list(raw_json['profile']))
    profiles.drop(['id_str', 
                   'profile_background_image_url_https', 
                   'profile_image_url_https'], axis=1, inplace=True)
    profiles['label'] = raw_json['label']
    profiles['probe_date'] = probe_date

    # Convert columns to integer
    profiles[['id', 'followers_count', 'friends_count', 'listed_count', 'favourites_count', 'statuses_count', 'label']] = profiles[['id', 'followers_count', 'friends_count', 'listed_count', 'favourites_count', 'statuses_count', 'label']].apply(pd.to_numeric)
    profiles.set_index('id', inplace=True)

    # Convert strings to dates
    profiles['created_at'] = profiles['created_at'].apply(pd.to_datetime, utc=True)

    # Replace "True ", "False ", "None " by python types
    profiles.replace({'True ': True, 'False ': False, 'None ': None}, inplace=True)

    return profiles


def date_or_timestamp_converter(dt):
    try:
        return pd.to_datetime(dt, utc=True)
    except:
        return pd.to_datetime("1185440851000", unit="ms", utc=True)

def load_cresci17():
    files_genuine = ["../cresci-2017.csv/genuine_accounts.csv/users.csv"]
    files_bots = ["../cresci-2017.csv/traditional_spambots_1.csv/users.csv",
              "../cresci-2017.csv/traditional_spambots_2.csv/users.csv",
              "../cresci-2017.csv/traditional_spambots_3.csv/users.csv",
              "../cresci-2017.csv/traditional_spambots_4.csv/users.csv",
              "../cresci-2017.csv/social_spambots_1.csv/users.csv",
              "../cresci-2017.csv/social_spambots_2.csv/users.csv",
              "../cresci-2017.csv/social_spambots_3.csv/users.csv"]

    raw_data_genuine = pd.concat((pd.read_csv(f) for f in files_genuine))
    raw_data_genuine['label'] = 0

    raw_data_bots = pd.concat((pd.read_csv(f) for f in files_bots))
    raw_data_bots['label'] = 1

    raw_data = pd.concat((raw_data_genuine, raw_data_bots))
    raw_data.rename(columns={'updated': 'probe_date'}, inplace=True)

    profiles = pd.DataFrame(raw_data)
    profiles.drop(['profile_background_image_url_https', 
                   'profile_image_url_https'], axis=1, inplace=True)

    # Replace values
    profiles['name'].fillna(profiles['screen_name'], inplace=True)
    profiles['description'].fillna('', inplace=True)

    # Convert columns to integer
    profiles[['id', 'followers_count', 'friends_count', 'listed_count', 'favourites_count', 'statuses_count', 'label']] = profiles[['id', 'followers_count', 'friends_count', 'listed_count', 'favourites_count', 'statuses_count', 'label']].apply(pd.to_numeric)
    profiles.set_index('id', inplace=True)

    # Convert strings to dates
    profiles['created_at'] = profiles['created_at'].apply(date_or_timestamp_converter)
    profiles['probe_date'] = profiles['probe_date'].apply(date_or_timestamp_converter)

    # Convert to bool
    profiles['geo_enabled'] = profiles['geo_enabled'] == 1
    profiles['verified'] = profiles['verified'] == 1
    profiles['profile_background_tile'] = profiles['profile_background_tile'] == 1
    profiles['profile_use_background_image'] = profiles['profile_use_background_image'] == 1
    profiles['default_profile'] = profiles['default_profile'] == 1
    profiles['default_profile_image'] = profiles['default_profile_image'] == 1

    return profiles


def load_midterm18():
    file_users = "../midterm-2018_processed_user_objects.json"
    file_bots = "../midterm-2018.tsv"

    profiles = pd.read_json(file_users)
    labels = pd.read_csv(file_bots, names=['id', 'label'], sep='\t')    

    profiles.rename(columns={'user_id': 'id', 'probe_timestamp': 'probe_date', 'user_created_at': 'created_at'}, inplace=True)

    # Replace values
    profiles['name'].fillna(profiles['screen_name'], inplace=True)
    profiles['description'].fillna('', inplace=True)

    # Convert columns to integer
    profiles[['id', 'followers_count', 'friends_count', 'listed_count', 'favourites_count', 'statuses_count']] = profiles[['id', 'followers_count', 'friends_count', 'listed_count', 'favourites_count', 'statuses_count']].apply(pd.to_numeric)
    
    # Set labels
    profiles.set_index('id', inplace=True)
    labels.set_index('id', inplace=True)
    profiles['label'] = labels.label.apply(lambda l: 1 if l == 'bot' else 0)

    # Convert strings to dates
    profiles['created_at'] = profiles['created_at'].apply(date_or_timestamp_converter)
    profiles['probe_date'] = profiles['probe_date'].apply(date_or_timestamp_converter)

    # Convert to bool
    profiles['geo_enabled'] = profiles['geo_enabled'] == 1
    profiles['verified'] = profiles['verified'] == 1
    #profiles['profile_background_tile'] = profiles['profile_background_tile'] == 1
    #profiles['profile_use_background_image'] = profiles['profile_use_background_image'] == 1
    #profiles['default_profile'] = profiles['default_profile'] == 1
    #profiles['default_profile_image'] = profiles['default_profile_image'] == 1

    return profiles

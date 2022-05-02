import profile
import pandas as pd
import numpy as np

import os
import botometer

#os.chdir('PI_Bots_President')
DATASETS_PATH = '../'

TWIBOT20_FOLDER = 'Twibot-20'
TWIBOT20_PROBE_TIME = '2020-09-06'
MODEL_FEATURES = ['default_profile',
                 'description_length', 
                 'favourites_count', 
                 'favourites_growth_rate',
                 'followers_count',
                 'followers_friends_ratio',
                 'followers_growth_rate',
                 'friends_count',
                 'friends_growth_rate',
                 'geo_enabled',
                 'has_url',
                 'listed_count',
                 'listed_growth_rate',
                 'name_length',
                 'num_digits_in_name',
                 'num_digits_in_screen_name',
                 'profile_use_background_image',
                 'screen_name_length',
                 'statuses_count',
                 'tweet_freq',
                 'user_age']

def feature_engineering(profiles, check_url=True):
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

    profiles['has_url'] = ~profiles['url'].isna()
    #profiles['has_location'] = profiles['location'].isna()

    #profiles.drop(['url', 'location'], axis=1, inplace=True)
    
    return profiles

def get_botometer_scores(accounts_scrren_name):

    rapidapi_key = os.environ.get("rapidapi_key")
    twitter_app_auth = {
    'consumer_key': os.environ.get("consumer_key"),
    'consumer_secret': os.environ.get("consumer_secret"),
    'access_token': os.environ.get("access_token"),
    'access_token_secret': os.environ.get("access_token_secret")
  }
    bom = botometer.Botometer(wait_on_ratelimit=True,
                          rapidapi_key=rapidapi_key,
                          **twitter_app_auth)

    result = []
    for account in accounts_scrren_name:
        try:
            result.append(bom.check_account(account)['display_scores']['universal']['overall'])
        except:
            result.append(-1)
    return result

    


def remove_presidential22_extra_columns(profiles):
    #return profiles.drop(['nb_tweets', 'timestamp', 'test_set_1', 'test_set_2', 'crawled_at', 'notifications', 'profile_banner_url', 'follow_request_sent'], axis=1)
    return profiles[MODEL_FEATURES]

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


def load_presidential22(floor_nb_tweets):
    file= "twitter_api/data/feature/features_1649666566.850515.json"
    profiles = pd.read_json(file)
    profiles = profiles.T

    profiles = profiles[profiles['nb_tweets'] >= floor_nb_tweets]
    profiles.rename(columns={'probe_time': 'probe_date'}, inplace=True)
    profiles.rename(columns={'has_url': 'url'}, inplace=True)

    # Delete rows with invalid data (probably banned account)
    profiles.drop(profiles[profiles['friends_count'].isna()].index, inplace=True)
    
    # Convert columns to integer
    profiles[['followers_count', 'friends_count', 'listed_count', 'favourites_count', 'statuses_count']] = profiles[['followers_count', 'friends_count', 'listed_count', 'favourites_count', 'statuses_count']].apply(pd.to_numeric)

    # Convert strings to dates
    profiles['created_at'] = profiles['created_at'].apply(pd.to_datetime, utc=True)

    # Replace "True ", "False ", "None " by python types
    profiles.replace({'True': True, 'False': False, 'None': None}, inplace=True)

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

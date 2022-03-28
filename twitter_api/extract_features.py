import requests
import os
import json
from datetime import datetime
import utils
import sys

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARER_TOKEN")
CHUNK_SIZE = 100


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r

def get_users_feature(user_dict):
    dict_keys = list(user_dict.keys())
    chunks = [dict_keys[x:x+CHUNK_SIZE] for x in range(0, len(dict_keys), CHUNK_SIZE)]
    for chunk in chunks:
        ids = ','.join(chunk)

        response = requests.get(
            "https://api.twitter.com/1.1/users/lookup.json?user_id={}".format(ids),
            auth=bearer_oauth
            )
        if response.status_code != 200:
            raise Exception(
                "Cannot retrieve users details (HTTP {}): {}".format(response.status_code, response.text)
            )
        json_response = response.json()
        for user in json_response:
            user_id = user['id_str']
            user_dict[user_id]['statuses_count'] = user["statuses_count"]
            user_dict[user_id]['followers_count'] = user["followers_count"]
            user_dict[user_id]['friends_count'] = user["friends_count"]
            user_dict[user_id]['favourites_count'] = user["favourites_count"]
            user_dict[user_id]['listed_count'] = user["listed_count"]
            user_dict[user_id]['description'] = user['description']
            user_dict[user_id]['screen_name'] = user['screen_name']
            user_dict[user_id]['name'] = user['name']
            user_dict[user_id]['geo_enabled'] = user['geo_enabled']
            user_dict[user_id]['verified'] = user['verified']
            user_dict[user_id]['created_at'] = user['created_at']
            user_dict[user_id]['profile_background_tile'] = user['profile_background_tile']
            user_dict[user_id]['profile_use_background_image'] = user['profile_use_background_image']
            user_dict[user_id]['default_profile'] = user['default_profile']
            user_dict[user_id]['default_profile_image'] = user['default_profile_image']
            user_dict[user_id]['has_url'] = user['url']
            user_dict[user_id]['has_location'] = True if 'location' in user else False
            
    filename = "feature/features_{}.json".format(datetime.timestamp(datetime.now()))
    utils.save_to_JSON_file(user_dict, filename)


def merge_json_files(directory):
    total_dict = {}
    for filename in os.listdir(directory):
        print(filename)
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f):
            with open(f, 'r') as outfile:
                users_dict = json.load(outfile)
                for key in users_dict:
                    user_dict_date = datetime.strptime(users_dict[key]['probe_time'],"%Y-%m-%d %H:%M:%S.%f")
                    if key in total_dict:
                        total_dict[key]['nb_tweets'] += users_dict[key]['nb_tweets']
                        for rule in users_dict[key]['matching_rules']:
                            if rule not in total_dict[key]['matching_rules']:
                                total_dict[key]['matching_rules'].append(rule)

                        total_dict_date = datetime.strptime(total_dict[key]['probe_time'],"%Y-%m-%d %H:%M:%S.%f")
                        # On prend le timestamp le plus vieux
                        if total_dict_date > user_dict_date:
                            total_dict[key]['probe_time'] = users_dict[key]['probe_time']
                    else:
                        total_dict[key] = {}
                        total_dict[key]['nb_tweets'] = users_dict[key]['nb_tweets']
                        total_dict[key]['matching_rules'] = users_dict[key]['matching_rules']
                        total_dict[key]['probe_time'] = users_dict[key]['probe_time']
    
    filename = "merged/merged_{}.json".format(datetime.timestamp(datetime.now()))
    utils.save_to_JSON_file(total_dict, filename)
    return total_dict




if __name__ == "__main__":
    sys.stdin.reconfigure(encoding='utf-8')
    sys.stdout.reconfigure(encoding='utf-8')
    total_dict = merge_json_files('data/output')
    get_users_feature(total_dict)
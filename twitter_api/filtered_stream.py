import requests
import os
import json
from datetime import datetime
import utils

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARER_TOKEN")


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(delete):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": "context:38.1483827056597110000", "tag": "French Presidential Elections 2022"},
        {"value": "context:38.1483827056597110787", "tag": "French Presidential Elections 2022 v2"},
        {"value": "context:35.822153632002904064", "tag": "Emmanuel Macron"},
        {"value": "context:35.822417319812943873", "tag": "Nicolas Sarkozy"},
        {"value": "context:35.1466465464691822594", "tag": "Valérie Pécresse"},
        {"value": "context:35.1466058925661245445", "tag": "Jean-Luc Mélenchon"},
        {"value": "context:35.840172130562064384", "tag": "Nathalie Arthaud"},
        {"value": "context:35.1466053355365548040", "tag": "Eric Zemmour"},
        {"value": "context:35.822153193526169600", "tag": "Marine Le Pen"},
        {"value": "context:35.828643761416658944", "tag": "Jean-Luc Melenchon"},
        {"value": "context:35.1118103044795748352", "tag": "Yannick Jadot"},
        {"value": "context:35.840176055679766530", "tag": "Philippe Poutou"},
        {"value": "context:35.1117814891144810497", "tag": "Nicolas Dupont-Aignan"},
        {"value": "context:35.1230925906685771777", "tag": "Anne Hidalgo"},
        {"value": "context:35.1466081450558570498", "tag": "Fabien Roussel"},

    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))



def get_stream(set):
    global_count = 0
    users_dict = {}
    expansions = "author_id"
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream?expansions={}".format(expansions), auth=bearer_oauth, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            global_count += 1
            json_response = json.loads(response_line)
            #print(json_response)

            tweet_author_id = json_response["data"]["author_id"]
            
            # a new user is detected
            if tweet_author_id not in users_dict:
                current_user = {}
                current_user['user_id'] = json_response["data"]["author_id"]
                current_user['probe_time'] = datetime.now()
                current_user['matching_rules'] = json_response['matching_rules']
                current_user['nb_tweets'] = 1
                users_dict[tweet_author_id] = current_user
            else:
                users_dict[tweet_author_id]['nb_tweets'] += 1
                users_dict[tweet_author_id]['matching_rules'] = users_dict[tweet_author_id]['matching_rules'] + json_response['matching_rules']

            print(users_dict[tweet_author_id])
            print('\n')

            if global_count % 1000 == 0:
                filename = "output/output_{}.json".format(datetime.timestamp(datetime.now()))
                utils.save_to_JSON_file(users_dict, filename=filename)
                users_dict = {}


  




def main():
    #rules = get_rules()
    #delete = delete_all_rules(rules)
    #set = set_rules(delete)
    utils.remove_temp_files()
    get_stream(set)


if __name__ == "__main__":
    main()

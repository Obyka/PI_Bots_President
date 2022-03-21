import requests
import os
import json
import datetime

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARER_TOKEN")


def create_url():
    #tweet_fields = "tweet.fields=lang,author_id"
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    # You can adjust ids to include a single Tweets.
    # Or you can add to up to 100 comma-separated IDs

    # default value
    max_results = 10
    query = "Fabien Roussel"
    tweet_fields="context_annotations"
    d = datetime.datetime(2021, 12, 31, 18, 00)  # <-- get time in UTC
    start_time = d.isoformat("T") + "Z"

    url = "https://api.twitter.com/2/tweets/search/all?max_results={}&query={}&start_time={}&tweet.fields={}".format(max_results, query, start_time, tweet_fields)
    return url


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2TweetLookupPython"
    return r


def connect_to_endpoint(url):
    response = requests.request("GET", url, auth=bearer_oauth)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def main():
    url = create_url()
    json_response = connect_to_endpoint(url)
    for elem in json_response["data"]:
        if "context_annotations" in elem:
            for single_annotation in elem["context_annotations"]:
                print(single_annotation["domain"]["id"] +" "+single_annotation["domain"]["name"])
                print(single_annotation["entity"]["id"] +" "+single_annotation["entity"]["name"])
                print("")
    #print(json.dumps(json_response, indent=4, sort_keys=True))


if __name__ == "__main__":
    main()

from TwitterAPI import TwitterAPI, TwitterOAuth, TwitterRequestError, TwitterConnectionError, TwitterPager
import json
import re
from csv import DictWriter

QUERY = '(covid) place_country:GB'
TOTAL_TWEETS = 1000


def flatten(item, prefix=''):
    flat = {}
    for k, v in item.items():
        if isinstance(v, dict):
            flat.update(flatten(v, k))
        else:
            if prefix:
                flat[prefix + '.' + k] = v
            else:
                flat[k] = v
    return flat



def clean(tweet):
    flat_ = flatten(tweet)
    if flat_.get('referenced_tweets'):
        for ref in flat_['referenced_tweets']:
            flat_['referenced_tweet_' + ref['type']] = ref['id']
        del flat_['referenced_tweets']

    for k, v in flat_.items():
        if isinstance(v, str):
            flat_[k] = v.replace('\t', ' ')

    flat_['text'] = flat_['text'].replace('\n', ' ')
    return flat_

def write_to_file(tweets):
    all_keys = set()
    for tweet in tweets:
        all_keys = all_keys.union(tweet.keys())

    with open('tweets.csv', 'w') as out:
        csv_writer = DictWriter(out, all_keys, dialect='excel-tab')
        csv_writer.writeheader()
        for tweet in tweets:
            csv_writer.writerow(tweet)

try:
    o = TwitterOAuth.read_file()
    api = TwitterAPI(
        o.consumer_key,
        o.consumer_secret,
        o.access_token_key,
        o.access_token_secret,
        auth_type='oAuth2',
        api_version='2'
    )
    pager = TwitterPager(api,f'tweets/search/all',
    {
        'query':QUERY,
        'tweet.fields':'author_id,created_at,public_metrics,referenced_tweets,in_reply_to_user_id',
        'expansions':'author_id,referenced_tweets.id,referenced_tweets.id.author_id,in_reply_to_user_id,attachments.media_keys',
        'media.fields':'url',
        'user.fields':'username,name',
        'start_time': '2021-02-25T06:00:00Z',
        'end_time': '2021-03-29T12:00:00Z',
        'max_results': 500
    })
    tweets = []
    tweet_iterator = pager.get_iterator(wait=2)
    for _ in range(0, TOTAL_TWEETS):
        tweet = tweet_iterator.__next__()
        print(json.dumps(tweet))
        tweets.append(clean(tweet))
    write_to_file(tweets)

except TwitterRequestError as e:
    print(e.status_code)
    for msg in iter(e):
        print(msg)

except TwitterConnectionError as e:
    print(e)

except Exception as e:
    print(e)

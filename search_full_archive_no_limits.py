from typing import final
from TwitterAPI import (
    TwitterAPI, TwitterOAuth, TwitterRequestError, TwitterConnectionError,
    TwitterPager
)
from datetime import datetime
import os
from csv import DictWriter
import time

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

def write_to_file(name, tweets):
    keys = [
        'id',
        'author_id',
        'created_at',
        'text',
        'in_reply_to_user_id',
        'public_metrics.like_count',
        'public_metrics.quote_count',
        'public_metrics.reply_count',
        'public_metrics.retweet_count',
        'referenced_tweet_quoted',
        'referenced_tweet_replied_to',
        'attachments.media_keys',
    ]

    mode = 'a' if os.path.exists(name) else 'w'

    with open(name, mode) as out:
        csv_writer = DictWriter(out, keys, dialect='excel-tab')
        if mode == 'w':
            csv_writer.writeheader()
        for tweet in tweets:
            csv_writer.writerow(tweet)

def query_api():
    o = TwitterOAuth.read_file()
    api = TwitterAPI(
        o.consumer_key,
        o.consumer_secret,
        o.access_token_key,
        o.access_token_secret,
        auth_type='oAuth2',
        api_version='2'
    )
    pager = TwitterPager(
        api,
        'tweets/search/all',
        {
            'query':QUERY,
            'tweet.fields': ','.join([
                'author_id',
                'created_at',
                'public_metrics',
                'referenced_tweets',
                'in_reply_to_user_id',
            ]),
            'expansions': ','.join([
                'author_id',
                'referenced_tweets.id',
                'referenced_tweets.id.author_id',
                'in_reply_to_user_id',
                'attachments.media_keys',
            ]),
            'media.fields': 'url',
            'user.fields': 'username,name',
            'start_time': '2021-02-25T06:00:00Z',
            'end_time': '2021-03-29T12:00:00Z',
            'max_results': 500
        }
    )
    return pager.get_iterator(wait=2)


def fetch_and_write(pager, name):
    try:
        batch = []
        for _ in range(0, TOTAL_TWEETS):
            batch.append(clean(pager.__next__()))

            if len(batch) % 100 == 0:
                write_to_file(name, batch)
                batch = []
    finally:
        write_to_file(name, batch)


def run_with_retries(func, retries=5):
    try:
        func()
    except TwitterRequestError as e:
        print('Error:')
        print(e.status_code)
        for msg in iter(e):
            print(msg)
        print('Trying to continue in 10 secs')
        time.sleep(10)
        run_with_retries(func, retries=retries-1)
    except TwitterConnectionError as e:
        print('Error:')
        print(e)
        print('Trying to continue in 10 secs')
        time.sleep(10)
        run_with_retries(func, retries=retries-1)



name = '{}_tweets.csv'.format(datetime.strftime(
    datetime.now(),
    '%d_%m_%Y_%H_%M_%S'
))
tweet_iterator = query_api()

run_with_retries(lambda: fetch_and_write(tweet_iterator, name))

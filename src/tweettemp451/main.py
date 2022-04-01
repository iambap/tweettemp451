import argparse
import functools
import sys

import trio

from . import __name__ as program_name
from .contextmanager import aclosing
from .diagnostics import configure_logging
from .pipeline import observe_twitter_sample_stream, extract_time_and_coordinates, get_temperature, \
    compute_sliding_average, write_data
from .twitter import TwitterClient, TweetTransformer
from .weather import WeatherClient


def entrypoint(verbosity, **kw):
    instruments = configure_logging(verbosity)
    print('Data collection may take a few moments to start. Exit with Ctrl+C. There will be no further message.',
          file=sys.stderr)
    return trio.run(functools.partial(async_main, **kw), instruments=instruments)


async def async_main(window_size, temperature_file_name, average_file_name, twitter_token, weather_api_key):
    async with aclosing(TwitterClient(twitter_token)) as twitter_client, \
               aclosing(WeatherClient(weather_api_key)) as weather_client, \
               trio.open_nursery() as nursery:
        # Tweet pipeline
        tweet_transformer = TweetTransformer(twitter_client)
        send_tweet, receive_tweet = trio.open_memory_channel(0)
        send_location, receive_location = trio.open_memory_channel(0)
        nursery.start_soon(observe_twitter_sample_stream, twitter_client, send_tweet)
        nursery.start_soon(extract_time_and_coordinates, tweet_transformer, receive_tweet, send_location)

        # Temperature pipeline
        send_temperature_to_file, receive_temperature_to_file = trio.open_memory_channel(0)
        send_temperature_to_average, receive_temperature_to_average = trio.open_memory_channel(0)
        nursery.start_soon(
            get_temperature,
            weather_client,
            receive_location,
            send_temperature_to_file,
            send_temperature_to_average)
        nursery.start_soon(write_data, receive_temperature_to_file, temperature_file_name)

        # Sliding average pipeline
        send_average, receive_average = trio.open_memory_channel(0)
        nursery.start_soon(compute_sliding_average, window_size, receive_temperature_to_average, send_average)
        nursery.start_soon(write_data, receive_average, average_file_name)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog=program_name,
        description='Aggregate temperature data for a stream of sample tweets'
        )
    parser.add_argument(
        '--window-size',
        help='calculate the sliding average over the last N tweets',
        default=5, type=int, choices=range(2, 101), metavar='N',
        )
    parser.add_argument(
        '--temperature-file',
        help='output momentary temperature data to this file',
        type=trio.Path, required=True, metavar='FILE', dest='temperature_file_name'
        )
    parser.add_argument(
        '--average-file',
        help='output average temperature data to this file',
        type=trio.Path, required=True, metavar='FILE', dest='average_file_name'
        )
    parser.add_argument(
        '-v',
        help='show verbose output',
        action='count', default=0, dest='verbosity'
        )
    parser.add_argument(
        '--twitter-token',
        help='twitter API app-only authentication token',
        required=True, metavar='TOKEN'
        )
    parser.add_argument(
        '--weather-api-key',
        help='weatherapi.com API Key',
        required=True, metavar='APIKEY'
        )
    return vars(parser.parse_args(argv[1:]))

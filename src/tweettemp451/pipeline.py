"""Pipeline steps for transforming a stream of tweets into temperature data and writing that data to files."""

import datetime
import decimal
import logging

import trio
import trio.abc

from .contextmanager import aclosing
from .data import Coordinates
from .twitter import TwitterClient, TweetTransformer
from .weather import WeatherClient

__all__ = [
    'observe_twitter_sample_stream',
    'extract_time_and_coordinates',
    'get_temperature',
    'compute_sliding_average',
    'write_data',
    ]


async def observe_twitter_sample_stream(
        twitter_client: TwitterClient,
        send_tweet: trio.abc.SendChannel[str],
        task_status = trio.TASK_STATUS_IGNORED):
    """Stream sample tweets from twitter into the pipeline."""
    logger = logging.getLogger(observe_twitter_sample_stream.__name__)
    task_status.started()
    async with aclosing(twitter_client.stream()) as stream:
        async for line in stream:
            logger.info(line)
            await send_tweet.send(line)


async def extract_time_and_coordinates(
        tweet_transformer: TweetTransformer,
        receive_tweet: trio.abc.ReceiveChannel[str],
        send_location: trio.abc.SendChannel[tuple[datetime.datetime, Coordinates]]):
    """Convert each json string representing a single tweet into a (timestamp, coordinates) ordered pair."""
    logger = logging.getLogger(extract_time_and_coordinates.__name__)
    async with receive_tweet, send_location:
        async for tweet_json in receive_tweet:
            tweet = tweet_transformer.parse_tweet(tweet_json)
            if not tweet:
                logger.info(f'{tweet_json} could not be parsed.')
                continue

            timestamp = tweet_transformer.get_timestamp(tweet)
            if not timestamp:
                logger.info(f'{tweet_json} has no timestamp.')
                continue

            coordinates = await tweet_transformer.get_coordinates(tweet)
            if not coordinates:
                logger.info(f'{tweet_json} has no coordinates.')
                continue

            logger.info(f'Extracted timestamp {timestamp} and coordinates {coordinates} from {tweet_json}.')
            await send_location.send((timestamp, coordinates))


async def get_temperature(
        weather_client: WeatherClient,
        receive_location: trio.abc.ReceiveChannel[tuple[datetime.datetime, Coordinates]],
        send_temperature_to_file: trio.abc.SendChannel[tuple[datetime.datetime, decimal.Decimal]],
        send_temperature_to_average: trio.abc.SendChannel[tuple[datetime.datetime, decimal.Decimal]]):
    """Convert each (timestamp, coordinates) pair into a (timestamp, temperature) pair using the weather API."""
    logger = logging.getLogger(get_temperature.__name__)
    async with receive_location, send_temperature_to_file, send_temperature_to_average:
        async for timestamp, coordinates in receive_location:
            temperature = await weather_client.get_current_temperature_for_coordinates(coordinates)
            if temperature is not None:
                logger.info(f'Found temperature {temperature} for coordinates {coordinates}.')
                datum = (timestamp, temperature)
                await send_temperature_to_file.send(datum)
                await send_temperature_to_average.send(datum)
            else:
                logger.info(f'Unable to get weather for {coordinates}.')


async def compute_sliding_average(
        window_size: int,
        receive_temperature: trio.abc.ReceiveChannel[tuple[datetime.datetime, decimal.Decimal]],
        send_average: trio.abc.SendChannel[tuple[datetime.datetime, decimal.Decimal]]):
    """Convert each (timestamp, temperature) pair into a (timestamp, average) pair using the sliding average."""
    buffer = [decimal.Decimal(0)] * window_size
    count = decimal.Decimal(0)
    current = 0
    total = decimal.Decimal(0)
    async with receive_temperature, send_average:
        async for timestamp, temperature in receive_temperature:
            if count == window_size:
                total -= buffer[current]
            buffer[current] = temperature
            total += temperature
            current = (current + 1) % window_size
            if count != window_size:
                count += 1
            average = total / count
            await send_average.send((timestamp, average))


async def write_data(
        data_source: trio.abc.ReceiveChannel[tuple[datetime.datetime, decimal.Decimal]],
        output_path: trio.Path):
    """Write data to a file at the end of the pipeline."""
    async with data_source, await output_path.open('wt', encoding='UTF-8') as output_file:
        async for timestamp, datum in data_source:
            await output_file.write(f'{timestamp},{datum}\n')

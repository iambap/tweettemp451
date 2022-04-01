import json
import logging

import dateutil
import dateutil.parser
import httpx
import tenacity
import typing

from .data import Coordinates

logger = logging.getLogger(__name__)


class TwitterClient:
    """A simple client to talk to the twitter API."""
    def __init__(self, bearer_token):
        self._http = httpx.AsyncClient(
            headers={'Authorization': f'Bearer {bearer_token}'},
            base_url='https://api.twitter.com'
            )

    @tenacity.retry(
        wait=tenacity.wait_exponential(min=1, max=10),
        stop=tenacity.stop_after_attempt(5),
        retry=tenacity.retry_if_exception_type(httpx.TransportError),
        )
    async def stream(self) -> typing.AsyncIterator[str]:
        """Stream sample tweets.

        https://developer.twitter.com/en/docs/twitter-api/tweets/volume-streams/api-reference/get-tweets-sample-stream
        """
        async with self._http.stream(
                'GET',
                '/2/tweets/sample/stream?expansions=geo.place_id&tweet.fields=created_at,geo&place.fields=geo'
                ) as response:
            async for line in response.aiter_lines():
                yield line.rstrip()

    async def aclose(self):
        await self._http.aclose()


class TweetTransformer:
    """TweetTransformer knows how to deserialize streaming data from twitter and extract interesting features."""

    def __init__(self, twitter_client):
        self._twitter_client = twitter_client

    @staticmethod
    def extract_coordinates_from_geo(geo):
        """Given a GeoJSON object representing a tweet's location, attempt to extract the longitude and latitude."""
        if not geo:
            return None

        coordinates = geo.get('coordinates')
        if isinstance(coordinates, dict):
            coordinates = coordinates.get('coordinates')
        if isinstance(coordinates, list) and len(coordinates) in (2, 3):
            try:
                return Coordinates(longitude=float(coordinates[0]), latitude=float(coordinates[1]))
            except (ValueError, TypeError) as ex:
                logger.debug(f'Failed to convert coordinates {coordinates}.', exc_info=ex)
        elif coordinates:
            logger.debug(f"Didn't understand coordinates {coordinates}.")

        bbox = geo.get('bbox')
        if isinstance(bbox, list) and len(bbox) in (4, 6):
            try:
                return Coordinates(longitude=(bbox[0] + bbox[2]) / 2.0, latitude=(bbox[1] + bbox[3]) / 2.0)
            except TypeError:
                logger.debug(f'Failed arithmetic on bounding box {bbox}.', exc_info=ex)
                pass
        elif bbox:
            logger.debug(f"Didn't understand bounding box {bbox}.")

        return None

    async def get_coordinates(self, tweet):
        """Given a tweet object from the streaming API, extract the longitude and latitude if present."""
        geo = tweet.get('data', {}).get('geo', {})
        coordinates = self.extract_coordinates_from_geo(geo)
        if not coordinates:
            places = tweet.get('includes', {}).get('places', [])
            geos = [x for x in map(lambda x: x.get('geo'), places) if x]
            coordinates = next((x for x in map(self.extract_coordinates_from_geo, geos) if x), None)
        return coordinates

    @staticmethod
    def parse_tweet(tweet_json):
        """Convert a json string representing a single tweet even into a python dict."""
        try:
            tweet = json.loads(tweet_json)
        except json.JSONDecodeError as ex:
            logger.debug(f'Failed to decode json {tweet_json}.', exc_info=ex)
            return None
        if not isinstance(tweet, dict):
            logger.debug(f'Decoded json {tweet_json} was a {type(tweet)}, not a dict.')
            return None
        return tweet

    @staticmethod
    def get_timestamp(tweet):
        """Given a tweet object from the streaming API, extract the tweet's timestamp."""
        created_at = tweet.get('data', {}).get('created_at')
        if not isinstance(created_at, str):
            return None
        try:
            timestamp = dateutil.parser.isoparse(created_at)
        except (dateutil.parser.ParserError, dateutil.parser.UnknownTimezoneWarning, OverflowError) as ex:
            logger.debug(f'Failed to parse {created_at} as date.', exc_info=ex)
            return None
        else:
            return timestamp

import enum
import json
import logging
import decimal

import httpx
import tenacity

from .data import Coordinates

logger = logging.getLogger(__name__)


class ErrorCodes(enum.IntEnum):
    """Weather API Error Codes

    Error codes may be returned as the code property of the error object in a response from the API.
    """
    NO_MATCHING_LOCATION = 1006


class WeatherClient:
    """A simple client to talk to the weatherapi.com API."""
    def __init__(self, api_key):
        self._api_key = api_key
        self._http = httpx.AsyncClient(
            base_url='http://api.weatherapi.com',
            params={'key': api_key}
        )

    @tenacity.retry(
        wait=tenacity.wait_exponential(min=1, max=10),
        stop=tenacity.stop_after_attempt(5),
        retry=tenacity.retry_if_exception_type(httpx.TransportError),
    )
    async def get_current_temperature_for_coordinates(self, coordinates: Coordinates):
        """Get the current temperature for a given longitude and latitude."""
        query_coords = f'{coordinates.latitude},{coordinates.longitude}'
        response = await self._http.get('/v1/current.json', params={'q': query_coords, 'aqi': 'no'})
        if self.should_ignore_error(response):
            return None
        response.raise_for_status()

        try:
            weather = response.json(parse_float=decimal.Decimal)
        except (json.JSONDecodeError, decimal.InvalidOperation, decimal.Overflow) as ex:
            logger.debug(f'Failed to decode json {response.text}.', exc_info=ex)
            return None
        else:
            if not isinstance(weather, dict):
                logger.debug(f'Decoded json {response.text} was a {type(weather)}, not a dict.')
                return None

        temperature = weather.get('current', {}).get('temp_f')
        if isinstance(temperature, decimal.Decimal):
            return temperature
        if isinstance(temperature, str):
            try:
                temperature_str = temperature
                temperature = decimal.Decimal(temperature_str)
            except (decimal.InvalidOperation, decimal.Overflow) as ex:
                logger.debug(f"Failed to convert {temperature_str} to decimal.", exc_info=ex)
                return None
            else:
                return temperature
        if temperature is None:
            logger.debug(f"Weather {weather} didn't have temp_f")
        else:
            logger.debug(f"Didn't understand temperature {temperature}.")
        return None

    @staticmethod
    def should_ignore_error(response):
        """If the error code indicates the location was unknown, that is considered part of normal operation."""
        if response and response.status_code == httpx.codes.BAD_REQUEST:
            try:
                error = response.json().get('error', {})
                error_code = error.get('code') if error else None
            except json.JSONDecodeError as ex:
                logger.debug(f'Failed to decode json {response.text}.', exc_info=ex)
            else:
                logger.debug(f"Got error {error} from API.")
                if error_code == ErrorCodes.NO_MATCHING_LOCATION:
                    return True
        return False

    async def aclose(self):
        await self._http.aclose()

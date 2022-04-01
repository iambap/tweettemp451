# TweetTemp 451

A simple tool to collect temperature information from a sample stream of tweets.

# Prerequisites

- Python 3.9
- [Twitter API app access tokens](https://developer.twitter.com/en/docs/twitter-api/getting-started/getting-access-to-the-twitter-api)
- [WeatherAPI.com access key](https://www.weatherapi.com/docs/)

# Getting Started

```
$ git clone git@github.com:iambap/tweettemp451.git
$ cd tweettemp451
$ python3 setup.py install
$ python3 -m tweettemp451 --temperature-file temperatures.csv --average-file averages.csv --twitter-token "$TWITTER_TOKEN" --weather-api-key "$WEATHER_API_KEY"
```


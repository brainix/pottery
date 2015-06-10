# Pottery: Redis for Humans

[Redis](http://redis.io/) is awesome :grinning:, but Redis clients are not awesome :rage:.  Pottery is a Pythonic way to access Redis :question:.  If you know how to use Python dicts and sets, then you already know how to use Pottery :exclamation:.

## Installation

    $ pip3 install pottery

## Usage

First, set up your Redis client :alien::

    >>> import urllib.parse
    >>> from redis import Redis
    >>> url = urllib.parse.urlparse('http://localhost:6379/')
    >>> redis = Redis(host=url.hostname, port=url.port, password=url.password)

That was the hardest part.  :grimacing:  Next, create a `RedisDict`:

    >>> from pottery import RedisDict
    >>> d = RedisDict(redis, 'person')

The first argument to `RedisDict()` is your Redis client.  The second argument is the Redis key name for your dict.  Other than that, you can use your `RedisDict` the same way that you use any other Python dict:

    >>> d['first_name'] = 'Raj'
    >>> d['last_name'] = 'Shah'
    >>> len(d)
    2
    >>> d['first_name']
    'Raj'
    >>> d['middle_name']
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "/Users/raj/Documents/Code/pottery/pottery/containers.py", line 82, in __getitem__
        raise KeyError(key)
    KeyError: 'middle_name'

# Pottery: Redis for Humans

[Redis](http://redis.io/) is awesome, :grinning: but Redis clients are not awesome. :rage:  Pottery is a Pythonic way to access Redis.  If you know how to use Python dicts and sets, then you already know how to use Pottery.

## Installation

    $ pip3 install pottery

## Usage

First, set up your Redis client: :alien:

    >>> import urllib.parse
    >>> from redis import Redis
    >>> url = urllib.parse.urlparse('http://localhost:6379/')
    >>> redis = Redis(host=url.hostname, port=url.port, password=url.password)

That was the hardest part. :grimacing:

### Dicts

Create a `RedisDict`:

    >>> from pottery import RedisDict
    >>> raj = RedisDict(redis, 'raj')

The first argument to `RedisDict()` is your Redis client.  The second argument is the Redis key name for your dict.  Other than that, you can use your `RedisDict` the same way that you use any other Python dict:

    >>> raj['job'] = 'computers'
    >>> raj['hobby'] = 'music'
    >>> raj['vegetarian'] = True
    >>> len(raj)
    3
    >>> raj['job']
    'computers'
    >>> raj['girlfriend']
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "/Users/raj/Documents/Code/pottery/pottery/containers.py", line 82, in __getitem__
        raise KeyError(key)
    KeyError: 'girlfriend'

### Sets

Create a `RedisSet`:

    >>> from pottery import RedisSet
    >>> edible = RedisSet(redis, 'edible')
    >>> edible.add('tofu')
    >>> edible.add('avocado')
    >>> len(edible)
    2
    >>> 'tofu' in edible
    True
    >>> 'bacon' in edible
    False

# Pottery: Redis for Humans

[Redis](http://redis.io/) is awesome, :grinning: but Redis clients are not
awesome. :rage:  Pottery is a Pythonic way to access Redis.  If you know how to
use Python dicts and sets, then you already know how to use Pottery.

## Installation

    $ pip3 install pottery

## Usage

First, set up your Redis client: :alien:

    >>> from redis import Redis
    >>> redis = Redis.from_url('http://localhost:6379/')

That was the hardest part. :grimacing:

### Dicts

Create a `RedisDict`:

    >>> from pottery import RedisDict
    >>> raj = RedisDict(redis, 'raj', job='computers', hobby='music')

The first argument to `RedisDict()` is your Redis client.  The second argument
is the Redis key name for your dict.  Other than that, you can use your
`RedisDict` the same way that you use any other Python dict:

    >>> len(raj)
    2
    >>> raj['hobby']
    'music'
    >>> raj['vegetarian'] = True
    >>> raj['vegetarian']
    True
    >>> raj['girlfriend']
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "/Users/raj/Documents/Code/pottery/pottery/containers.py", line 82, in __getitem__
        raise KeyError(key)
    KeyError: 'girlfriend'

### Sets

Create a `RedisSet`:

    >>> from pottery import RedisSet
    >>> edible = RedisSet(redis, 'edible', ['tofu', 'avocado'])

Again, the first argument to `RedisSet()` is your Redis client.  The second
argument is the Redis key name for your set.  Other than that, you can use your
`RedisSet` the same way that you use any other Python set:

    >>> len(edible)
    2
    >>> 'tofu' in edible
    True
    >>> 'bacon' in edible
    False
    >>> edible.add('strawberries')
    >>> 'strawberries' in edible
    True

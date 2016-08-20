# Pottery: Redis for Humans

[Redis](http://redis.io/) is awesome, :grinning: but [Redis
commands](http://redis.io/commands) are not always fun. :rage:  Pottery is a
Pythonic way to access Redis.  If you know how to use Python dicts and sets,
then you already know how to use Pottery.

[![Build Status](https://travis-ci.org/brainix/pottery.svg)](https://travis-ci.org/brainix/pottery)

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
    >>> raj = RedisDict(redis=redis, key='raj')

Notice the two keyword arguments to `RedisDict()`:  The first is your Redis
client.  The second is the Redis key name for your dict.  Other than that, you
can use your `RedisDict` the same way that you use any other Python dict:

    >>> raj['hobby'] = 'music'
    >>> raj['vegetarian'] = True
    >>> len(raj)
    2
    >>> raj['vegetarian']
    True

### Sets

Create a `RedisSet`:

    >>> from pottery import RedisSet
    >>> edible = RedisSet(redis=redis, key='edible')

Again, notice the two keyword arguments to `RedisSet()`:  The first is your
Redis client.  The second is the Redis key name for your set.  Other than that,
you can use your `RedisSet` the same way that you use any other Python set:

    >>> edible.add('tofu')
    >>> edible.add('avocado')
    >>> len(edible)
    2
    >>> 'bacon' in edible
    False

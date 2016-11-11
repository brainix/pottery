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

## Contributing

### Install prerequisites

1. Install [Xcode](https://developer.apple.com/xcode/downloads/).

### Obtain source code

1. Clone the git repo:
  1. `$ git clone git@github.com:brainix/pottery.git`
  2. `$ cd pottery/`
2. Install project-level dependencies:
  1. `$ make install`

### Run tests

1. In one Terminal session:
  1. `$ cd pottery/`
  2. `$ redis-server`
2. In a second Terminal session:
  1. `$ cd pottery/`
  2. `$ coverage3 run -m unittest discover --start-directory tests --verbose`
  3. `$ coverage3 report`

# Pottery: Redis for Humans

[Redis](http://redis.io/) is awesome, :grinning: but [Redis
commands](http://redis.io/commands) are not always fun. :rage:  Pottery is a
Pythonic way to access Redis.  If you know how to use Python dicts, then you
already know how to use Pottery.

[![Build Status](https://travis-ci.org/brainix/pottery.svg?branch=master)](https://travis-ci.org/brainix/pottery)

If you enjoy Pottery, then please consider adding a :thumbsup: to [this
comment](https://github.com/vinta/awesome-python/pull/1088#issue-198200204) so
that Pottery gets added to the [Awesome
Python](https://github.com/vinta/awesome-python#awesome-python-) list!

## Installation

    $ pip3 install pottery

## Usage

First, set up your Redis client: :alien:

    >>> from redis import Redis
    >>> redis = Redis.from_url('redis://localhost:6379/')

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
    >>> raj
    RedisDict{'hobby': 'music', 'vegetarian': True}
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

    >>> edible.add('eggs')
    >>> edible.extend({'beans', 'tofu', 'avocado'})
    >>> edible
    RedisSet{'tofu', 'avocado', 'eggs', 'beans'}
    >>> len(edible)
    4
    >>> 'bacon' in edible
    False



### Lists

Create a `RedisList`:

    >>> from pottery import RedisList
    >>> lyrics = RedisList(redis=redis, key='lyrics')

Again, notice the two keyword arguments to `RedisList()`:  The first is your
Redis client.  The second is the Redis key name for your list.  Other than
that, you can use your `RedisList` the same way that you use any other Python
list:

    >>> lyrics.append('everything')
    >>> lyrics.extend(['in', 'its', 'right', '...'])
    >>> lyrics
    RedisList['everything', 'in', 'its', 'right', '...']
    >>> len(lyrics)
    5
    >>> lyrics[0]
    'everything'
    >>> lyrics[4] = 'place'
    >>> lyrics
    RedisList['everything', 'in', 'its', 'right', 'place']



### NextId

`NextId` safely and reliably produces increasing IDs across threads, processes,
and even machines, without a single point of failure.  [Rationale and algorithm
description.](http://antirez.com/news/102)

Instantiate an ID generator:

    >>> from pottery import NextId
    >>> user_ids = NextId(key='user-ids', masters={redis})

The `key` argument represents the sequence (so that you can have different
sequences for user IDs, comment IDs, etc.), and the `masters` argument
specifies your Redis masters across which to distribute ID generation (in
production, you should have 5 Redis masters).  Now, whenever you need a user
ID, call `next()` on the ID generator:

    >>> next(user_ids)
    1
    >>> next(user_ids)
    2
    >>> next(user_ids)
    3

Two caveats:

1. If many clients are generating IDs concurrently, then there may be &ldquo;holes&rdquo; in the sequence of IDs (e.g.: 1, 2, 6, 10, 11, 21, &hellip;).
2. This algorithm scales to about 5,000 IDs per second (with 5 Redis masters).  If you need IDs faster than that, then you may want to consider other techniques.



### Redlock

`Redlock` is a safe and reliable lock to coordinate access to a resource shared
across threads, processes, and even machines, without a single point of
failure.  [Rationale and algorithm
description.](http://redis.io/topics/distlock)

`Redlock` implements Python's excellent
[`threading.Lock`](https://docs.python.org/3/library/threading.html#lock-objects)
API as closely as is feasible.  In other words, you can use `Redlock` the same
way that you use `threading.Lock`.

Instantiate a `Redlock`:

    >>> from pottery import Redlock
    >>> lock = Redlock(key='printer', masters={redis})

The `key` argument represents the resource, and the `masters` argument
specifies your Redis masters across which to distribute the lock (in
production, you should have 5 Redis masters).  Now you can protect access to
your resource:

    >>> lock.acquire()
    >>> # Critical section - print stuff here.
    >>> lock.release()

Or you can protect access to your resource inside a context manager:

    >>> with lock:
    ...   # Critical section - print stuff here.

`Redlock`s time out (by default, after 10 seconds).  You should take care to
ensure that your critical section completes well within the timeout.  The
reasons that `Redlock`s time out are to preserve
[&ldquo;liveness&rdquo;](http://redis.io/topics/distlock#liveness-arguments)
and to avoid deadlocks (in the event that a process dies inside a critical
section before it releases its lock).

    >>> import time
    >>> lock.acquire()
    True
    >>> bool(lock.locked())
    True
    >>> # Critical section - print stuff here.
    >>> time.sleep(10)
    >>> bool(lock.locked())
    False

If 10 seconds isn't enough to complete executing your critical section, then
you can specify your own timeout:

    >>> lock = Redlock(key='printer', auto_release_time=15*1000)
    >>> lock.acquire()
    True
    >>> bool(lock.locked())
    True
    >>> # Critical section - print stuff here.
    >>> time.sleep(10)
    >>> bool(lock.locked())
    True
    >>> time.sleep(5)
    >>> bool(lock.locked())
    False



### ContextTimer

`ContextTimer` helps you easily and accurately measure elapsed time.  Note that
`ContextTimer` measures wall (real-world) time, not CPU time; and that
`elapsed()` returns time in milliseconds.

You can use `ContextTimer` stand-alone&hellip;

    >>> import time
    >>> from pottery import ContextTimer
    >>> timer = ContextTimer()
    >>> timer.start()
    >>> time.sleep(0.1)
    >>> 100 <= timer.elapsed() < 200
    True
    >>> timer.stop()
    >>> time.sleep(0.1)
    >>> 100 <= timer.elapsed() < 200
    True

&hellip;or as a context manager:

    >>> tests = []
    >>> with ContextTimer() as timer:
    ...     time.sleep(0.1)
    ...     tests.append(100 <= timer.elapsed() < 200)
    >>> time.sleep(0.1)
    >>> tests.append(100 <= timer.elapsed() < 200)
    >>> tests
    [True, True]



### HyperLogLogs

HyperLogLogs are an interesting data structure that allow you to answer the
question, *&ldquo;How many distinct elements have I seen?&rdquo;* but not the
questions, *&ldquo;Have I seen this element before?&rdquo;* or *&ldquo;What are
all of the elements that I&rsquo;ve seen before?&rdquo;*  So think of
HyperLogLogs as Python sets that you can add elements to and get the length of,
but that you can&rsquo;t use to test element membership, iterate through, or
get elements back out of.



### Bloom filters

Bloom filters are a powerful data structure that help you to answer the
questions, *&ldquo;Have I seen this element before?&rdquo;* and *&ldquo;How
many distinct elements have I seen?&rdquo;* but not the question, *&ldquo;What
are all of the elements that I&rsquo;ve seen before?&rdquo;*  So think of Bloom
filters as Python sets that you can add elements to, use to test element
membership, and get the length of, but that you can&rsquo;t iterate through or
get elements back out of.

Bloom filters are probabilistic, which means that they can sometimes generate
false positives (as in, they may report that you&rsquo;ve seen a particular
element before even though you haven&rsquo;t).  But they will never generate
false negatives (so every time that they report that you haven&rsquo;t seen a
particular element before, you really must never have seen it).  You can tune
your acceptable false positive probability, though at the expense of the
storage size and the element insertion/lookup time of your Bloom filter.

Create a `BloomFilter`:

    >>> from pottery import BloomFilter
    >>> dilberts = BloomFilter(
    ...     num_values=100,
    ...     false_positives=0.01,
    ...     redis=redis,
    ...     key='dilberts',
    ... )

Here, `num_values` represents the number of elements that you expect to insert
into your `BloomFilter`, and `false_positives` represents your acceptable false
positive probability.  Using these two parameters, `BloomFilter` automatically
computes its own storage size and number of times to run its hash functions on
element insertion/lookup such that it can guarantee a false positive rate at or
below what you can tolerate, given that you&rsquo;re going to insert your
specified number of elements.

Insert an element into the `BloomFilter`:

    >>> dilberts.add('rajiv')

Test for membership in the `BloomFilter`:

    >>> 'rajiv' in dilberts
    True
    >>> 'raj' in dilberts
    False
    >>> 'dan' in dilberts
    False

See how many elements we&rsquo;ve inserted into the `BloomFilter`:

    >>> len(dilberts)
    1

Note that `BloomFilter.__len__()` is an approximation, so please don&rsquo;t
rely on it for anything important like financial systems or cat gif websites.

Insert multiple elements into the `BloomFilter`:

    >>> dilberts.update({'raj', 'dan'})

Remove all of the elements from the `BloomFilter`:

    >>> dilberts.clear()



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
  2. `$ make test`

`make test` runs all of the unit tests as well as the coverage test.  However,
sometimes, when debugging, it can be useful to run an individual test module,
class, or method:

1. In one Terminal session:
  1. `$ cd pottery/`
  2. `$ redis-server`
2. In a second Terminal session:
  1. Run a test module with `$ make test tests=tests.test_dict`
  2. Run a test class with: `$ make test tests=tests.test_dict.DictTests`
  3. Run a test method with: `$ make test tests=tests.test_dict.DictTests.test_keyexistserror`

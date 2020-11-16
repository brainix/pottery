# Pottery: Redis for Humans

[Redis](http://redis.io/) is awesome, but [Redis
commands](http://redis.io/commands) are not always fun.  Pottery is a Pythonic
way to access Redis.  If you know how to use Python dicts, then you already
know how to use Pottery.

[![Build Status](https://travis-ci.com/brainix/pottery.svg?branch=master)](https://travis-ci.com/brainix/pottery)
[![Coverage Status](https://coveralls.io/repos/github/brainix/pottery/badge.svg?branch=master)](https://coveralls.io/github/brainix/pottery?branch=master)
![Libraries.io dependency status for GitHub repo](https://img.shields.io/librariesio/github/brainix/pottery)
[![PyPI version](https://badge.fury.io/py/pottery.svg)](https://badge.fury.io/py/pottery)

![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pottery)

[![Downloads](https://pepy.tech/badge/pottery)](https://pepy.tech/project/pottery)
[![Downloads](https://pepy.tech/badge/pottery/month)](https://pepy.tech/project/pottery)
[![Downloads](https://pepy.tech/badge/pottery/week)](https://pepy.tech/project/pottery)

## Installation

    $ pip3 install pottery

## Usage

First, set up your Redis client:

```python
>>> from redis import Redis
>>> redis = Redis.from_url('redis://localhost:6379/')
```

That was the hardest part.



### Dicts

Create a `RedisDict`:

```python
>>> from pottery import RedisDict
>>> raj = RedisDict(redis=redis, key='raj')
```

Notice the two keyword arguments to `RedisDict()`:  The first is your Redis
client.  The second is the Redis key name for your dict.  Other than that, you
can use your `RedisDict` the same way that you use any other Python dict:

```python
>>> raj['hobby'] = 'music'
>>> raj['vegetarian'] = True
>>> raj
RedisDict{'hobby': 'music', 'vegetarian': True}
>>> len(raj)
2
>>> raj['vegetarian']
True
```



### Sets

Create a `RedisSet`:

```python
>>> from pottery import RedisSet
>>> edible = RedisSet(redis=redis, key='edible')
```

Again, notice the two keyword arguments to `RedisSet()`:  The first is your
Redis client.  The second is the Redis key name for your set.  Other than that,
you can use your `RedisSet` the same way that you use any other Python set:

```python
>>> edible.add('eggs')
>>> edible.extend({'beans', 'tofu', 'avocado'})
>>> edible
RedisSet{'tofu', 'avocado', 'eggs', 'beans'}
>>> len(edible)
4
>>> 'bacon' in edible
False
```



### Lists

Create a `RedisList`:

```python
>>> from pottery import RedisList
>>> lyrics = RedisList(redis=redis, key='lyrics')
```

Again, notice the two keyword arguments to `RedisList()`:  The first is your
Redis client.  The second is the Redis key name for your list.  Other than
that, you can use your `RedisList` the same way that you use any other Python
list:

```python
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
```



### NextId

`NextId` safely and reliably produces increasing IDs across threads, processes,
and even machines, without a single point of failure.  [Rationale and algorithm
description.](http://antirez.com/news/102)

Instantiate an ID generator:

```python
>>> from pottery import NextId
>>> user_ids = NextId(key='user-ids', masters={redis})
```

The `key` argument represents the sequence (so that you can have different
sequences for user IDs, comment IDs, etc.), and the `masters` argument
specifies your Redis masters across which to distribute ID generation (in
production, you should have 5 Redis masters).  Now, whenever you need a user
ID, call `next()` on the ID generator:

```python
>>> next(user_ids)
1
>>> next(user_ids)
2
>>> next(user_ids)
3
```

Two caveats:

1. If many clients are generating IDs concurrently, then there may be &ldquo;holes&rdquo; in the sequence of IDs (e.g.: 1, 2, 6, 10, 11, 21, &hellip;).
2. This algorithm scales to about 5,000 IDs per second (with 5 Redis masters).  If you need IDs faster than that, then you may want to consider other techniques.



### Redlock

`Redlock` is a safe and reliable lock to coordinate access to a resource shared
across threads, processes, and even machines, without a single point of
failure.  [Rationale and algorithm
description.](http://redis.io/topics/distlock)

`Redlock` implements Python&rsquo;s excellent
[`threading.Lock`](https://docs.python.org/3/library/threading.html#lock-objects)
API as closely as is feasible.  In other words, you can use `Redlock` the same
way that you use `threading.Lock`.

Instantiate a `Redlock`:

```python
>>> from pottery import Redlock
>>> lock = Redlock(key='printer', masters={redis})
```

The `key` argument represents the resource, and the `masters` argument
specifies your Redis masters across which to distribute the lock (in
production, you should have 5 Redis masters).  Now you can protect access to
your resource:

```python
>>> lock.acquire()
>>> # Critical section - print stuff here.
>>> lock.release()
```

Or you can protect access to your resource inside a context manager:

```python
>>> with lock:
...     # Critical section - print stuff here.
```

`Redlock`s time out (by default, after 10 seconds).  You should take care to
ensure that your critical section completes well within the timeout.  The
reasons that `Redlock`s time out are to preserve
[&ldquo;liveness&rdquo;](http://redis.io/topics/distlock#liveness-arguments)
and to avoid deadlocks (in the event that a process dies inside a critical
section before it releases its lock).

```python
>>> import time
>>> lock.acquire()
True
>>> bool(lock.locked())
True
>>> # Critical section - print stuff here.
>>> time.sleep(10)
>>> bool(lock.locked())
False
```

If 10 seconds isn&rsquo;t enough to complete executing your critical section,
then you can specify your own timeout:

```python
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
```



### redis_cache()

`redis_cache()` is a simple function return value cache, sometimes called
[&ldquo;memoize&rdquo;](https://en.wikipedia.org/wiki/Memoization).
`redis_cache()` implements Python&rsquo;s excellent
[`functools.lru_cache()`](https://docs.python.org/3/library/functools.html#functools.lru_cache)
API as closely as is feasible.  In other words, you can use `redis_cache()` the
same way that you use `functools.lru_cache()`.

*Limitations:*

1. Arguments to the function must be hashable
2. Return values from the function must be JSON serializable

In general, you should only use `redis_cache()` when you want to reuse
previously computed values.  Accordingly, it doesn&rsquo;t make sense to cache
functions with side-effects or impure functions such as `time()` or `random()`.

Decorate a function:

```python
>>> import time
>>> from pottery import redis_cache
>>> @redis_cache(redis=redis, key='expensive-function-cache')
... def expensive_function(n):
...     time.sleep(1)  # Simulate an expensive computation or database lookup.
...     return n
...
>>>
```

Notice the two keyword arguments to `redis_cache()`: The first is your Redis
client.  The second is the Redis key name for your function&rsquo;s return
value cache.

Call your function and observe the cache hit/miss rates:

```python
>>> expensive_function(5)
5
>>> expensive_function.cache_info()
CacheInfo(hits=0, misses=1, maxsize=None, currsize=1)
>>> expensive_function(5)
5
>>> expensive_function.cache_info()
CacheInfo(hits=1, misses=1, maxsize=None, currsize=1)
>>> expensive_function(6)
6
>>> expensive_function.cache_info()
CacheInfo(hits=1, misses=2, maxsize=None, currsize=1)
```

Notice that the first call to `expensive_function()` takes 1 second and results
in a cache miss; but the second call returns almost immediately and results in
a cache hit.  This is because after the first call, `redis_cache()` cached the
return value for the call when `n == 5`.

You can access your original undecorated underlying `expensive_function()` as
`expensive_function.__wrapped__`.  This is useful for introspection, for
bypassing the cache, or for rewrapping the original function with a different
cache.

You can force a cache reset for a particular combination of `args`/`kwargs`
with `expensive_function.__bypass__`.  A call to
`expensive_function.__bypass__(*args, **kwargs)` bypasses the cache lookup,
calls the original underlying function, then caches the results for future
calls to `expensive_function(*args, **kwargs)`.  Note that a call to
`expensive_function.__bypass__(*args, **kwargs)` results in neither a cache hit
nor a cache miss.

Finally, clear/invalidate your function&rsquo;s entire return value cache with
`expensive_function.cache_clear()`:

```python
>>> expensive_function.cache_info()
CacheInfo(hits=1, misses=2, maxsize=None, currsize=1)
>>> expensive_function.cache_clear()
>>> expensive_function.cache_info()
CacheInfo(hits=0, misses=0, maxsize=None, currsize=0)
```



### ContextTimer

`ContextTimer` helps you easily and accurately measure elapsed time.  Note that
`ContextTimer` measures wall (real-world) time, not CPU time; and that
`elapsed()` returns time in milliseconds.

You can use `ContextTimer` stand-alone&hellip;

```python
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
```

&hellip;or as a context manager:

```python
>>> tests = []
>>> with ContextTimer() as timer:
...     time.sleep(0.1)
...     tests.append(100 <= timer.elapsed() < 200)
>>> time.sleep(0.1)
>>> tests.append(100 <= timer.elapsed() < 200)
>>> tests
[True, True]
```



### HyperLogLogs

HyperLogLogs are an interesting data structure that allow you to answer the
question, *&ldquo;How many distinct elements have I seen?&rdquo;* but not the
questions, *&ldquo;Have I seen this element before?&rdquo;* or *&ldquo;What are
all of the elements that I&rsquo;ve seen before?&rdquo;*  So think of
HyperLogLogs as Python sets that you can add elements to and get the length of,
but that you can&rsquo;t use to test element membership, iterate through, or
get elements back out of.

HyperLogLogs are probabilistic, which means that they&rsquo;re accurate within
a margin of error up to 2%.  However, they can reasonably accurately estimate
the cardinality (size) of vast datasets (like the number of unique Google
searches issued in a day) with a tiny amount of storage (1.5 KB).

Create a `HyperLogLog`:

```python
>>> from pottery import HyperLogLog
>>> google_searches = HyperLogLog(redis=redis, key='google-searches')
```

Insert an element into the `HyperLogLog`:

```python
>>> google_searches.add('sonic the hedgehog video game')
```

See how many elements we&rsquo;ve inserted into the `HyperLogLog`:

```python
>>> len(google_searches)
1
```

Insert multiple elements into the `HyperLogLog`:

```python
>>> google_searches.update({
...     'google in 1998',
...     'minesweeper',
...     'joey tribbiani',
...     'wizard of oz',
...     'rgb to hex',
...     'pac-man',
...     'breathing exercise',
...     'do a barrel roll',
...     'snake',
... })
>>> len(google_searches)
10
```

Remove all of the elements from the `HyperLogLog`:

```python
>>> google_searches.clear()
>>> len(google_searches)
0
```



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

```python
>>> from pottery import BloomFilter
>>> dilberts = BloomFilter(
...     num_values=100,
...     false_positives=0.01,
...     redis=redis,
...     key='dilberts',
... )
```

Here, `num_values` represents the number of elements that you expect to insert
into your `BloomFilter`, and `false_positives` represents your acceptable false
positive probability.  Using these two parameters, `BloomFilter` automatically
computes its own storage size and number of times to run its hash functions on
element insertion/lookup such that it can guarantee a false positive rate at or
below what you can tolerate, given that you&rsquo;re going to insert your
specified number of elements.

Insert an element into the `BloomFilter`:

```python
>>> dilberts.add('rajiv')
```

Test for membership in the `BloomFilter`:

```python
>>> 'rajiv' in dilberts
True
>>> 'raj' in dilberts
False
>>> 'dan' in dilberts
False
```

See how many elements we&rsquo;ve inserted into the `BloomFilter`:

```python
>>> len(dilberts)
1
```

Note that `BloomFilter.__len__()` is an approximation, not an exact value,
though it&rsquo;s quite accurate.

Insert multiple elements into the `BloomFilter`:

```python
>>> dilberts.update({'raj', 'dan'})
```

Remove all of the elements from the `BloomFilter`:

```python
>>> dilberts.clear()
>>> len(dilberts)
0
```



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

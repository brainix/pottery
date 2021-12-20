# Pottery: Redis for Humans 🌎🌍🌏

[Redis](http://redis.io/) is awesome, but [Redis
commands](http://redis.io/commands) are not always fun.  Pottery is a Pythonic
way to access Redis.  If you know how to use Python dicts, then you already
know how to use Pottery.  Pottery is useful for accessing Redis more easily,
and also for implementing microservice resilience patterns; and it has been
battle tested in production at scale.

[![Build status](https://img.shields.io/github/workflow/status/brainix/pottery/Python%20package/master)](https://github.com/brainix/pottery/actions?query=branch%3Amaster)
[![Latest released version](https://badge.fury.io/py/pottery.svg)](https://badge.fury.io/py/pottery)

![Supported Python versions](https://img.shields.io/pypi/pyversions/pottery)
![Number of lines of code](https://img.shields.io/tokei/lines/github/brainix/pottery)

[![Total number of downloads](https://pepy.tech/badge/pottery)](https://pepy.tech/project/pottery)
[![Downloads per month](https://pepy.tech/badge/pottery/month)](https://pepy.tech/project/pottery)
[![Downloads per week](https://pepy.tech/badge/pottery/week)](https://pepy.tech/project/pottery)



## Table of Contents
- [Dicts 📖](#dicts)
- [Sets 🛍️](#sets)
- [Lists ⛓](#lists)
- [Counters 🧮](#counters)
- [Deques 🖇️](#deques)
- [Redlock 🔒](#redlock)
    - [synchronize() 👯‍♀️](#synchronize)
- [NextId 🔢](#nextid)
- [redis_cache()](#redis_cache)
- [CachedOrderedDict](#cachedordereddict)
- [Bloom filters](#bloom-filters)
- [HyperLogLogs](#hyperloglogs)
- [ContextTimer ⏱️](#contexttimer)



## Installation

```shell
$ pip3 install pottery
```

## Usage

First, set up your Redis client:

```python
>>> from redis import Redis
>>> redis = Redis.from_url('redis://localhost:6379/1')
>>>
```



## <a name="dicts"></a>Dicts 📖

`RedisDict` is a Redis-backed container compatible with Python&rsquo;s
[`dict`](https://docs.python.org/3/tutorial/datastructures.html#dictionaries).

Here is a small example using a `RedisDict`:

```python
>>> from pottery import RedisDict
>>> tel = RedisDict({'jack': 4098, 'sape': 4139}, redis=redis, key='tel')
>>> tel['guido'] = 4127
>>> tel
RedisDict{'jack': 4098, 'sape': 4139, 'guido': 4127}
>>> tel['jack']
4098
>>> del tel['sape']
>>> tel['irv'] = 4127
>>> tel
RedisDict{'jack': 4098, 'guido': 4127, 'irv': 4127}
>>> list(tel)
['jack', 'guido', 'irv']
>>> sorted(tel)
['guido', 'irv', 'jack']
>>> 'guido' in tel
True
>>> 'jack' not in tel
False
>>>
```

Notice the first two keyword arguments to `RedisDict()`:  The first is your
Redis client.  The second is the Redis key name for your dict.  Other than
that, you can use your `RedisDict` the same way that you use any other Python
`dict`.

*Limitations:*

1. Keys and values must be JSON serializable.



## <a name="sets"></a>Sets 🛍️

`RedisSet` is a Redis-backed container compatible with Python&rsquo;s
[`set`](https://docs.python.org/3/tutorial/datastructures.html#sets).

Here is a brief demonstration:

```python
>>> from pottery import RedisSet
>>> basket = RedisSet({'apple', 'orange', 'apple', 'pear', 'orange', 'banana'}, redis=redis, key='basket')
>>> sorted(basket)
['apple', 'banana', 'orange', 'pear']
>>> 'orange' in basket
True
>>> 'crabgrass' in basket
False

>>> a = RedisSet('abracadabra', redis=redis, key='magic')
>>> b = set('alacazam')
>>> sorted(a)
['a', 'b', 'c', 'd', 'r']
>>> sorted(a - b)
['b', 'd', 'r']
>>> sorted(a | b)
['a', 'b', 'c', 'd', 'l', 'm', 'r', 'z']
>>> sorted(a & b)
['a', 'c']
>>> sorted(a ^ b)
['b', 'd', 'l', 'm', 'r', 'z']
>>>
```

Notice the two keyword arguments to `RedisSet()`:  The first is your Redis
client.  The second is the Redis key name for your set.  Other than that, you
can use your `RedisSet` the same way that you use any other Python `set`.

Do more efficient membership testing for multiple elements using
`.contains_many()`:

```python
>>> nirvana = RedisSet({'kurt', 'krist', 'dave'}, redis=redis, key='nirvana')
>>> tuple(nirvana.contains_many('kurt', 'krist', 'chat', 'dave'))
(True, True, False, True)
>>>
```

*Limitations:*

1. Elements must be JSON serializable.



## <a name="lists"></a>Lists ⛓

`RedisList` is a Redis-backed container compatible with Python&rsquo;s
[`list`](https://docs.python.org/3/tutorial/introduction.html#lists).

```python
>>> from pottery import RedisList
>>> squares = RedisList([1, 4, 9, 16, 25], redis=redis, key='squares')
>>> squares
RedisList[1, 4, 9, 16, 25]
>>> squares[0]
1
>>> squares[-1]
25
>>> squares[-3:]
[9, 16, 25]
>>> squares[:]
[1, 4, 9, 16, 25]
>>> squares + [36, 49, 64, 81, 100]
RedisList[1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
>>>
```

Notice the two keyword arguments to `RedisList()`:  The first is your Redis
client.  The second is the Redis key name for your list.  Other than that, you
can use your `RedisList` the same way that you use any other Python `list`.

*Limitations:*

1. Elements must be JSON serializable.
2. Under the hood, Python implements `list` using an array.  Redis implements
   list using a
   [doubly linked list](https://redis.io/topics/data-types-intro#redis-lists).
   As such, inserting elements at the head or tail of a `RedisList` is fast,
   O(1).  However, accessing `RedisList` elements by index is slow, O(n).  So
   in terms of performance and ideal use cases, `RedisList` is more similar to
   Python&rsquo;s `deque` than Python&rsquo;s `list`.  Instead of `RedisList`,
   consider using [`RedisDeque`](#deques).



## <a name="counters"></a>Counters 🧮

`RedisCounter` is a Redis-backed container compatible with Python&rsquo;s
[`collections.Counter`](https://docs.python.org/3/library/collections.html#collections.Counter).

```python
>>> from pottery import RedisCounter
>>> c = RedisCounter(redis=redis, key='my-counter')
>>> c = RedisCounter('gallahad', redis=redis, key='my-counter')
>>> c.clear()
>>> c = RedisCounter({'red': 4, 'blue': 2}, redis=redis, key='my-counter')
>>> c.clear()
>>> c = RedisCounter(redis=redis, key='my-counter', cats=4, dogs=8)
>>> c.clear()

>>> c = RedisCounter(['eggs', 'ham'], redis=redis, key='my-counter')
>>> c['bacon']
0
>>> c['sausage'] = 0
>>> del c['sausage']
>>> c.clear()

>>> c = RedisCounter(redis=redis, key='my-counter', a=4, b=2, c=0, d=-2)
>>> sorted(c.elements())
['a', 'a', 'a', 'a', 'b', 'b']
>>> c.clear()

>>> RedisCounter('abracadabra', redis=redis, key='my-counter').most_common(3)
[('a', 5), ('b', 2), ('r', 2)]
>>> c.clear()

>>> c = RedisCounter(redis=redis, key='my-counter', a=4, b=2, c=0, d=-2)
>>> from collections import Counter
>>> d = Counter(a=1, b=2, c=3, d=4)
>>> c.subtract(d)
>>> c
RedisCounter{'a': 3, 'b': 0, 'c': -3, 'd': -6}
>>>
```

Notice the first two keyword arguments to `RedisCounter()`:  The first is your
Redis client.  The second is the Redis key name for your counter.  Other than
that, you can use your `RedisCounter` the same way that you use any other
Python `Counter`.

*Limitations:*

1. Keys must be JSON serializable.



## <a name="deques"></a>Deques 🖇️

`RedisDeque` is a Redis-backed container compatible with Python&rsquo;s
[`collections.deque`](https://docs.python.org/3/library/collections.html#collections.deque).

Example:

```python
>>> from pottery import RedisDeque
>>> d = RedisDeque('ghi', redis=redis, key='letters')
>>> for elem in d:
...     print(elem.upper())
G
H
I

>>> d.append('j')
>>> d.appendleft('f')
>>> d
RedisDeque(['f', 'g', 'h', 'i', 'j'])

>>> d.pop()
'j'
>>> d.popleft()
'f'
>>> list(d)
['g', 'h', 'i']
>>> d[0]
'g'
>>> d[-1]
'i'

>>> list(reversed(d))
['i', 'h', 'g']
>>> 'h' in d
True
>>> d.extend('jkl')
>>> d
RedisDeque(['g', 'h', 'i', 'j', 'k', 'l'])
>>> d.rotate(1)
>>> d
RedisDeque(['l', 'g', 'h', 'i', 'j', 'k'])
>>> d.rotate(-1)
>>> d
RedisDeque(['g', 'h', 'i', 'j', 'k', 'l'])

>>> RedisDeque(reversed(d), redis=redis)
RedisDeque(['l', 'k', 'j', 'i', 'h', 'g'])
>>> d.clear()

>>> d.extendleft('abc')
>>> d
RedisDeque(['c', 'b', 'a'])
>>>
```

Notice the two keyword arguments to `RedisDeque()`:  The first is your Redis
client.  The second is the Redis key name for your deque.  Other than that, you
can use your `RedisDeque` the same way that you use any other Python `deque`.

*Limitations:*

1. Elements must be JSON serializable.



## <a name="redlock"></a>Redlock 🔒

`Redlock` is a safe and reliable lock to coordinate access to a resource shared
across threads, processes, and even machines, without a single point of
failure.  [Rationale and algorithm
description.](http://redis.io/topics/distlock)

`Redlock` implements Python&rsquo;s excellent
[`threading.Lock`](https://docs.python.org/3/library/threading.html#lock-objects)
API as closely as is feasible.  In other words, you can use `Redlock` the same
way that you use `threading.Lock`.  The main reason to use `Redlock` over
`threading.Lock` is that `Redlock` can coordinate access to a resource shared
across different machines; `threading.Lock` can&rsquo;t.

Instantiate a `Redlock`:

```python
>>> from pottery import Redlock
>>> printer_lock = Redlock(key='printer', masters={redis})
>>>
```

The `key` argument represents the resource, and the `masters` argument
specifies your Redis masters across which to distribute the lock.  In
production, you should have 5 Redis masters.  This is to eliminate a single
point of failure &mdash; you can lose up to 2 out of the 5 Redis masters and
your `Redlock` will remain available and performant.  Now you can protect
access to your resource:

```python
>>> printer_lock.acquire()
True
>>> # Critical section - print stuff here.
>>> printer_lock.release()
>>>
```

Or you can protect access to your resource inside a context manager:

```python
>>> with printer_lock:
...     # Critical section - print stuff here.
...     pass
>>>
```

It&rsquo;s safest to instantiate a new `Redlock` object every time you need to
protect your resource and to not share `Redlock` instances across different
parts of code.  In other words, think of the `key` as identifying the resource;
don&rsquo;t think of any particular `Redlock` as identifying the resource.
Instantiating a new `Redlock` every time you need a lock sidesteps bugs by
decoupling how you use `Redlock` from the forking/threading model of your
application/service.

`Redlock`s are automatically released (by default, after 10 seconds).  You
should take care to ensure that your critical section completes well within
that timeout.  The reasons that `Redlock`s are automatically released are to
preserve
[&ldquo;liveness&rdquo;](http://redis.io/topics/distlock#liveness-arguments)
and to avoid deadlocks (in the event that a process dies inside a critical
section before it releases its lock).

```python
>>> import time
>>> printer_lock.acquire()
True
>>> bool(printer_lock.locked())
True
>>> # Critical section - print stuff here.
>>> time.sleep(10)
>>> bool(printer_lock.locked())
False
>>>
```

If 10 seconds isn&rsquo;t enough to complete executing your critical section,
then you can specify your own auto release time (in milliseconds):

```python
>>> printer_lock = Redlock(key='printer', masters={redis}, auto_release_time=15*1000)
>>> printer_lock.acquire()
True
>>> bool(printer_lock.locked())
True
>>> # Critical section - print stuff here.
>>> time.sleep(10)
>>> bool(printer_lock.locked())
True
>>> time.sleep(5)
>>> bool(printer_lock.locked())
False
>>>
```

By default, `.acquire()` blocks indefinitely until the lock is acquired.  You
can make `.acquire()` return immediately with the `blocking` argument.
`.acquire()` returns `True` if the lock was acquired; `False` if not.

```python
>>> printer_lock_1 = Redlock(key='printer', masters={redis})
>>> printer_lock_2 = Redlock(key='printer', masters={redis})
>>> printer_lock_1.acquire(blocking=False)
True
>>> printer_lock_2.acquire(blocking=False)  # Returns immediately.
False
>>> printer_lock_1.release()
>>>
```

You can make `.acquire()` block but not indefinitely by specifying the
`timeout` argument (in seconds):

```python
>>> printer_lock_1.acquire(timeout=1)
True
>>> printer_lock_2.acquire(timeout=1)  # Waits 1 second.
False
>>> printer_lock_1.release()
>>>
```

You can similarly configure the Redlock context manager&rsquo;s
blocking/timeout behavior during Redlock initialization.  If the context
manager fails to acquire the lock, it raises the `QuorumNotAchieved` exception.

```python
>>> import contextlib
>>> from pottery import QuorumNotAchieved
>>> printer_lock_1 = Redlock(key='printer', masters={redis}, context_manager_blocking=True, context_manager_timeout=0.2)
>>> printer_lock_2 = Redlock(key='printer', masters={redis}, context_manager_blocking=True, context_manager_timeout=0.2)
>>> with printer_lock_1:
...     with contextlib.suppress(QuorumNotAchieved):
...         with printer_lock_2:  # Waits 0.2 seconds; raises QuorumNotAchieved.
...             pass
...     assert printer_lock_1.locked()
...     assert not printer_lock_2.locked()
>>>
```



### <a name="synchronize"></a>synchronize() 👯‍♀️

`synchronize()` is a decorator that allows only one thread to execute a
function at a time.  Under the hood, `synchronize()` uses a Redlock, so refer
to the [Redlock documentation](#redlock) for more details.

Here&rsquo;s how to use `synchronize()`:

```python
>>> from pottery import synchronize
>>> @synchronize(key='synchronized-func', masters={redis}, auto_release_time=500, blocking=True, timeout=-1)
... def func():
...   # Only one thread can execute this function at a time.
...   return True
...
>>>
```


## <a name="nextid"></a>NextId 🔢

`NextId` safely and reliably produces increasing IDs across threads, processes,
and even machines, without a single point of failure.  [Rationale and algorithm
description.](http://antirez.com/news/102)

Instantiate an ID generator:

```python
>>> from pottery import NextId
>>> tweet_ids = NextId(key='tweet-ids', masters={redis})
>>>
```

The `key` argument represents the sequence (so that you can have different
sequences for user IDs, comment IDs, etc.), and the `masters` argument
specifies your Redis masters across which to distribute ID generation (in
production, you should have 5 Redis masters).  Now, whenever you need a user
ID, call `next()` on the ID generator:

```python
>>> next(tweet_ids)
1
>>> next(tweet_ids)
2
>>> next(tweet_ids)
3
>>>
```

Two caveats:

1. If many clients are generating IDs concurrently, then there may be
   &ldquo;holes&rdquo; in the sequence of IDs (e.g.: 1, 2, 6, 10, 11, 21,
   &hellip;).
2. This algorithm scales to about 5,000 IDs per second (with 5 Redis masters).
   If you need IDs faster than that, then you may want to consider other
   techniques.



## redis_cache()

`redis_cache()` is a simple lightweight unbounded function return value cache,
sometimes called
[&ldquo;memoize&rdquo;](https://en.wikipedia.org/wiki/Memoization).
`redis_cache()` implements Python&rsquo;s excellent
[`functools.cache()`](https://docs.python.org/3/library/functools.html#functools.cache)
API as closely as is feasible.  In other words, you can use `redis_cache()` the
same way that you use `functools.cache()`.

*Limitations:*

1. Arguments to the function must be hashable.
2. Return values from the function must be JSON serializable.
3. Just like `functools.cache()`, `redis_cache()` does not allow for a maximum
   size, and does not evict old values, and grows unbounded.  Only use
   `redis_cache()` in one of these cases:
    1. Your function&rsquo;s argument space has a known small cardinality.
    2. You specify a `timeout` when calling `redis_cache()` to decorate your
       function, to dump your _entire_ return value cache `timeout` seconds
       after the last cache access (hit or miss).
    3. You periodically call `.cache_clear()` to dump your _entire_ return
       value cache.
    4. You&rsquo;re ok with your return value cache growing unbounded, and you
       [understand the implications](https://docs.redislabs.com/latest/rs/administering/database-operations/eviction-policy/)
       of this for your underlying Redis instance.

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
CacheInfo(hits=1, misses=2, maxsize=None, currsize=2)
>>>
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
CacheInfo(hits=1, misses=2, maxsize=None, currsize=2)
>>> expensive_function.cache_clear()
>>> expensive_function.cache_info()
CacheInfo(hits=0, misses=0, maxsize=None, currsize=0)
>>>
```



## CachedOrderedDict

The best way that I can explain `CachedOrderedDict` is through an example
use-case.  Imagine that your search engine returns document IDs, which then you
have to hydrate into full documents via the database to return to the client.
The data structure used to represent such search results must have the
following properties:

1. It must preserve the order of the document IDs returned by the search engine.
2. It must map document IDs to hydrated documents.
3. It must cache previously hydrated documents.

Properties 1 and 2 are satisfied by Python&rsquo;s
[`collections.OrderedDict`](https://docs.python.org/3/library/collections.html#collections.OrderedDict).
However, `CachedOrderedDict` extends Python&rsquo;s `OrderedDict` to also
satisfy property 3.

The most common usage pattern for `CachedOrderedDict` is as follows:

1. Instantiate `CachedOrderedDict` with the IDs that you must look up or
   compute passed in as the `dict_keys` argument to the initializer.
2. Compute and store the cache misses for future lookups.
3. Return some representation of your `CachedOrderedDict` to the client.

Instantiate a `CachedOrderedDict`:

```python
>>> from pottery import CachedOrderedDict
>>> search_results_1 = CachedOrderedDict(
...     redis_client=redis,
...     redis_key='search-results',
...     dict_keys=(1, 2, 3, 4, 5),
... )
>>>
```

The `redis_client` argument to the initializer is your Redis client, and the
`redis_key` argument is the Redis key for the Redis Hash backing your cache.
The `dict_keys` argument represents an ordered iterable of keys to be looked up
and automatically populated in your `CachedOrderedDict` (on cache hits), or
that you&rsquo;ll have to compute and populate for future lookups (on cache
misses).  Regardless of whether keys are cache hits or misses,
`CachedOrderedDict` preserves the order of `dict_keys` (like a list), maps
those keys to values (like a dict), and maintains an underlying cache for
future key lookups.

In the beginning, the cache is empty, so let&rsquo;s populate it:

```python
>>> sorted(search_results_1.misses())
[1, 2, 3, 4, 5]
>>> search_results_1[1] = 'one'
>>> search_results_1[2] = 'two'
>>> search_results_1[3] = 'three'
>>> search_results_1[4] = 'four'
>>> search_results_1[5] = 'five'
>>> sorted(search_results_1.misses())
[]
>>>
```

Note that `CachedOrderedDict` preserves the order of `dict_keys`:

```python
>>> for key, value in search_results_1.items():
...     print(f'{key}: {value}')
1: one
2: two
3: three
4: four
5: five
>>>
```

Now, let&rsquo;s look at a combination of cache hits and misses:

```python
>>> search_results_2 = CachedOrderedDict(
...     redis_client=redis,
...     redis_key='search-results',
...     dict_keys=(2, 4, 6, 8, 10),
... )
>>> sorted(search_results_2.misses())
[6, 8, 10]
>>> search_results_2[2]
'two'
>>> search_results_2[6] = 'six'
>>> search_results_2[8] = 'eight'
>>> search_results_2[10] = 'ten'
>>> sorted(search_results_2.misses())
[]
>>> for key, value in search_results_2.items():
...     print(f'{key}: {value}')
2: two
4: four
6: six
8: eight
10: ten
>>>
```

*Limitations:*

1. Keys and values must be JSON serializable.



## Bloom filters

Bloom filters are a powerful data structure that help you to answer the
questions, _&ldquo;Have I seen this element before?&rdquo;_ and _&ldquo;How
many distinct elements have I seen?&rdquo;_; but not the question, _&ldquo;What
are all of the elements that I&rsquo;ve seen before?&rdquo;_  So think of Bloom
filters as Python sets that you can add elements to, use to test element
membership, and get the length of; but that you can&rsquo;t iterate through or
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
...     num_elements=100,
...     false_positives=0.01,
...     redis=redis,
...     key='dilberts',
... )
>>>
```

Here, `num_elements` represents the number of elements that you expect to
insert into your `BloomFilter`, and `false_positives` represents your
acceptable false positive probability.  Using these two parameters,
`BloomFilter` automatically computes its own storage size and number of times
to run its hash functions on element insertion/lookup such that it can
guarantee a false positive rate at or below what you can tolerate, given that
you&rsquo;re going to insert your specified number of elements.

Insert an element into the `BloomFilter`:

```python
>>> dilberts.add('rajiv')
>>>
```

Test for membership in the `BloomFilter`:

```python
>>> 'rajiv' in dilberts
True
>>> 'raj' in dilberts
False
>>> 'dan' in dilberts
False
>>>
```

See how many elements we&rsquo;ve inserted into the `BloomFilter`:

```python
>>> len(dilberts)
1
>>>
```

Note that `BloomFilter.__len__()` is an approximation, not an exact value,
though it&rsquo;s quite accurate.

Insert multiple elements into the `BloomFilter`:

```python
>>> dilberts.update({'raj', 'dan'})
>>>
```

Do more efficient membership testing for multiple elements using
`.contains_many()`:

```python
>>> tuple(dilberts.contains_many('rajiv', 'raj', 'dan', 'luis'))
(True, True, True, False)
>>>
```

Remove all of the elements from the `BloomFilter`:

```python
>>> dilberts.clear()
>>> len(dilberts)
0
>>>
```

*Limitations:*

1. Elements must be JSON serializable.
2. `len(bf)` is probabilistic in that it&rsquo;s an accurate approximation.  You
   can tune how accurate you want it to be with the `num_elements` and
   `false_positives` arguments to `.__init__()`, at the expense of storage space
   and insertion/lookup time.
3. Membership testing against a Bloom filter is probabilistic in that it *may*
   return false positives, but *never* returns false negatives.  This means that
   if `element in bf` evaluates to `True`, then you *may* have inserted the
   element into the Bloom filter.  But if `element in bf` evaluates to `False`,
   then you *must not* have inserted it.  Again, you can tune accuracy with the
   `num_elements` and `false_positives` arguments to `.__init__()`, at the
   expense of storage space and insertion/lookup time.



## HyperLogLogs

HyperLogLogs are an interesting data structure designed to answer the question,
_&ldquo;How many distinct elements have I seen?&rdquo;_; but not the questions,
_&ldquo;Have I seen this element before?&rdquo;_ or _&ldquo;What are all of the
elements that I&rsquo;ve seen before?&rdquo;_  So think of HyperLogLogs as
Python sets that you can add elements to and get the length of; but that you
can&rsquo;t use to test element membership, iterate through, or get elements
out of.

HyperLogLogs are probabilistic, which means that they&rsquo;re accurate within
a margin of error up to 2%.  However, they can reasonably accurately estimate
the cardinality (size) of vast datasets (like the number of unique Google
searches issued in a day) with a tiny amount of storage (1.5 KB).

Create a `HyperLogLog`:

```python
>>> from pottery import HyperLogLog
>>> google_searches = HyperLogLog(redis=redis, key='google-searches')
>>>
```

Insert an element into the `HyperLogLog`:

```python
>>> google_searches.add('sonic the hedgehog video game')
>>>
```

See how many elements we&rsquo;ve inserted into the `HyperLogLog`:

```python
>>> len(google_searches)
1
>>>
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
>>>
```

Through a clever hack, we can do membership testing against a `HyperLogLog`,
even though it was never designed for this purpose.  The way that the hack works
is that it creates a temporary copy of the `HyperLogLog`, then inserts the
element that you&rsquo;re running the membership test for into the temporary
copy.  If the insertion changes the temporary `HyperLogLog`&rsquo;s cardinality,
then the element must not have been inserted into the original `HyperLogLog`.

```python
>>> 'joey tribbiani' in google_searches
True
>>> 'jennifer aniston' in google_searches
False
>>>
```

Do more efficient membership testing for multiple elements using
`.contains_many()`:

```python
>>> tuple(google_searches.contains_many('joey tribbiani', 'jennifer aniston'))
(True, False)
>>>
```

Remove all of the elements from the `HyperLogLog`:

```python
>>> google_searches.clear()
>>> len(google_searches)
0
>>>
```

*Limitations:*

1. Elements must be JSON serializable.
2. `len(hll)` is probabilistic in that it&rsquo;s an accurate approximation.
3. Membership testing against a HyperLogLog is probabilistic in that it *may*
   return false positives, but *never* returns false negatives.  This means that
   if `element in hll` evaluates to `True`, then you *may* have inserted the
   element into the HyperLogLog.  But if `element in hll` evaluates to `False`,
   then you *must not* have inserted it.



## <a name="contexttimer"></a>ContextTimer ⏱️

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
>>>
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
>>>
```



## Contributing

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
    3. `$ make test-readme`

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

`make test-readme` doctests the Python code examples in this README to ensure
that they&rsquo;re correct.

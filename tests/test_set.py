# --------------------------------------------------------------------------- #
#   test_set.py                                                               #
#                                                                             #
#   Copyright © 2015-2021, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


from redis import Redis

from pottery import KeyExistsError
from pottery import RedisSet
from tests.base import TestCase  # type: ignore


class SetTests(TestCase):
    '''These tests come from these examples:
        https://docs.python.org/3/tutorial/datastructures.html#sets
    '''

    def test_init(self):
        set_ = RedisSet(redis=self.redis)
        assert set_ == set()

    def test_keyexistserror(self):
        fruits = {'apple', 'orange', 'apple', 'pear', 'orange', 'banana'}
        basket = RedisSet(fruits, redis=self.redis, key='pottery:basket')
        basket  # Workaround for Pyflakes.  :-(
        with self.assertRaises(KeyExistsError):
            RedisSet(fruits, redis=self.redis, key='pottery:basket')

    def test_basic_usage(self):
        fruits = {'apple', 'orange', 'apple', 'pear', 'orange', 'banana'}
        basket = RedisSet(fruits, redis=self.redis)
        assert basket == {'orange', 'banana', 'pear', 'apple'}
        assert 'orange' in basket
        assert not 'crabgrass' in basket

    def test_add(self):
        fruits = {'apple', 'orange', 'apple', 'pear', 'orange', 'banana'}
        basket = RedisSet(fruits, redis=self.redis)
        basket.add('tomato')
        assert basket == {
            'apple', 'orange', 'apple', 'pear', 'orange', 'banana', 'tomato',
        }

    def test_discard(self):
        fruits = {'apple', 'orange', 'apple', 'pear', 'orange', 'banana', 'tomato'}
        basket = RedisSet(fruits, redis=self.redis)
        basket.discard('tomato')
        assert basket == {
            'apple', 'orange', 'apple', 'pear', 'orange', 'banana',
        }

    def test_repr(self):
        basket = RedisSet({'apple'}, redis=self.redis)
        assert repr(basket) == "RedisSet{'apple'}"

        basket = RedisSet({'apple', 'orange'}, redis=self.redis)
        assert repr(basket) in {
            "RedisSet{'apple', 'orange'}",
            "RedisSet{'orange', 'apple'}",
        }

    def test_pop(self):
        fruits = {'apple', 'orange', 'apple', 'pear', 'orange', 'banana'}
        basket = RedisSet(fruits, redis=self.redis)
        for _ in range(len(fruits)):
            fruit = basket.pop()
            assert fruit in fruits
            fruits.discard(fruit)

        assert not fruits
        assert not basket
        with self.assertRaises(KeyError):
            basket.pop()

    def test_remove(self):
        basket = RedisSet({'apple', 'orange'}, redis=self.redis)
        basket.remove('orange')
        assert basket == {'apple'}

        basket.remove('apple')
        assert basket == set()

        with self.assertRaises(KeyError):
            basket.remove('apple')

    def test_set_operations(self):
        a = RedisSet('abracadabra', redis=self.redis)
        b = RedisSet('alacazam', redis=self.redis)
        assert a == {'a', 'r', 'b', 'c', 'd'}
        assert a - b == {'r', 'd', 'b'}
        assert isinstance(a - b, RedisSet)
        assert a | b == {'a', 'c', 'r', 'd', 'b', 'm', 'z', 'l'}
        assert isinstance(a | b, RedisSet)
        assert a & b == {'a', 'c'}
        assert isinstance(a & b, RedisSet)
        assert a ^ b == {'r', 'd', 'b', 'm', 'z', 'l'}
        assert isinstance(a ^ b, RedisSet)

    def test_isdisjoint(self):
        a = RedisSet('abc', redis=self.redis)
        b = RedisSet('cde', redis=self.redis)
        assert not a.isdisjoint(b)
        c = RedisSet('def', redis=self.redis)
        assert a.isdisjoint(c)

        other_url = 'redis://127.0.0.1:6379/'
        other_redis = Redis.from_url(other_url, socket_timeout=1)
        b = RedisSet('cde', redis=other_redis)
        assert not a.isdisjoint(b)
        c = RedisSet('def', redis=other_redis)
        assert a.isdisjoint(c)

        d = {'c', 'd', 'e'}
        assert not a.isdisjoint(d)
        e = {'d', 'e', 'f'}
        assert a.isdisjoint(e)

    def test_issubset(self):
        a = RedisSet('abc', redis=self.redis)
        b = RedisSet('abc', redis=self.redis)
        # assert a.issubset(b)
        # assert b.issubset(a)
        assert a <= b
        assert b <= a
        assert not a < b
        assert not b < a
        c = RedisSet('abcd', redis=self.redis)
        # assert a.issubset(c)
        # assert not c.issubset(a)
        assert a <= c
        assert not c <= a
        assert a < c
        assert not c < a
        d = RedisSet('def', redis=self.redis)
        # assert not a.issubset(d)
        # assert not d.issubset(a)
        assert not a <= d
        assert not d <= a
        assert not a < d
        assert not d < a

    def test_issuperset(self):
        a = RedisSet('abc', redis=self.redis)
        b = RedisSet('abc', redis=self.redis)
        # assert a.issuperset(b)
        # assert b.issuperset(a)
        assert a >= b
        assert b >= a
        assert not a > b
        assert not b > a
        c = RedisSet('abcd', redis=self.redis)
        # assert not a.issuperset(c)
        # assert c.issuperset(a)
        assert not a >= c
        assert c >= a
        assert not a > c
        assert c > a
        d = RedisSet('def', redis=self.redis)
        # assert not a.issuperset(d)
        # assert not d.issuperset(a)
        assert not a >= d
        assert not d >= a
        assert not a > d
        assert not d > a

    def test_update_with_no_arguments(self):
        s = RedisSet(redis=self.redis)
        s.update()
        assert s == set()

        s = RedisSet('abcd', redis=self.redis)
        s.update()
        assert s == {'a', 'b', 'c', 'd'}

    def test_update_with_empty_set(self):
        metasyntactic_variables = RedisSet(redis=self.redis)
        metasyntactic_variables.update(set())
        assert metasyntactic_variables == set()

        metasyntactic_variables = RedisSet(
            {'foo', 'bar', 'baz'},
            redis=self.redis,
        )
        metasyntactic_variables.update(set())
        assert metasyntactic_variables == {'foo', 'bar', 'baz'}

    def test_update_with_set(self):
        metasyntactic_variables = RedisSet(redis=self.redis)
        metasyntactic_variables.update({'foo', 'bar', 'baz'})
        assert metasyntactic_variables == {'foo', 'bar', 'baz'}
        metasyntactic_variables.update({'qux'})
        assert metasyntactic_variables == {'foo', 'bar', 'baz', 'qux'}

    def test_update_with_range(self):
        ramanujans_friends = RedisSet(redis=self.redis)
        ramanujans_friends.update(range(10))
        assert ramanujans_friends == {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

    def test_update_with_set_and_range(self):
        silliness = RedisSet(redis=self.redis)
        silliness.update({'foo', 'bar', 'baz'}, range(5))
        assert silliness == {'foo', 'bar', 'baz', 0, 1, 2, 3, 4}
        silliness.update({'qux'}, range(6))
        assert silliness == {'foo', 'bar', 'baz', 'qux', 0, 1, 2, 3, 4, 5}

    def test_difference_update_with_empty_set(self):
        ramanujans_friends = RedisSet(redis=self.redis)
        ramanujans_friends.difference_update(set())
        assert ramanujans_friends == set()

        ramanujans_friends = RedisSet(range(10), redis=self.redis)
        ramanujans_friends.difference_update(set())
        assert ramanujans_friends == {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

    def test_difference_update_with_set(self):
        ramanujans_friends = RedisSet(redis=self.redis)
        ramanujans_friends.difference_update({5, 6, 7, 8, 9})
        assert ramanujans_friends == set()

        ramanujans_friends = RedisSet(range(10), redis=self.redis)
        ramanujans_friends.difference_update({5, 6, 7, 8, 9})
        assert ramanujans_friends == {0, 1, 2, 3, 4}

    def test_difference_update_with_range(self):
        ramanujans_friends = RedisSet(redis=self.redis)
        ramanujans_friends.difference_update(range(5))
        assert ramanujans_friends == set()

        ramanujans_friends = RedisSet(range(10), redis=self.redis)
        ramanujans_friends.difference_update(range(5))
        assert ramanujans_friends == {5, 6, 7, 8, 9}

    def test_difference_update_with_range_and_set(self):
        ramanujans_friends = RedisSet(redis=self.redis)
        ramanujans_friends.difference_update(range(4), {6, 7, 8, 9})
        assert ramanujans_friends == set()

        ramanujans_friends = RedisSet(range(10), redis=self.redis)
        ramanujans_friends.difference_update(range(4), {6, 7, 8, 9})
        assert ramanujans_friends == {4, 5}

    def test_membership_for_non_jsonifyable_element(self):
        redis_set = RedisSet(redis=self.redis)
        assert not BaseException in redis_set

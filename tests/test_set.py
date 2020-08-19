# --------------------------------------------------------------------------- #
#   test_set.py                                                               #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
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
        set_ = RedisSet()
        assert set_ == set()

    def test_keyexistserror(self):
        fruits = {'apple', 'orange', 'apple', 'pear', 'orange', 'banana'}
        basket = RedisSet(fruits, key='pottery:basket')
        basket  # Workaround for Pyflakes.  :-(
        with self.assertRaises(KeyExistsError):
            RedisSet(fruits, key='pottery:basket')

    def test_basic_usage(self):
        fruits = {'apple', 'orange', 'apple', 'pear', 'orange', 'banana'}
        basket = RedisSet(fruits)
        assert basket == {'orange', 'banana', 'pear', 'apple'}
        assert 'orange' in basket
        assert not 'crabgrass' in basket

    def test_add(self):
        basket = RedisSet({
            'apple', 'orange', 'apple', 'pear', 'orange', 'banana',
        })
        basket.add('tomato')
        assert basket == {
            'apple', 'orange', 'apple', 'pear', 'orange', 'banana', 'tomato',
        }

    def test_discard(self):
        basket = RedisSet({
            'apple', 'orange', 'apple', 'pear', 'orange', 'banana', 'tomato',
        })
        basket.discard('tomato')
        assert basket == {
            'apple', 'orange', 'apple', 'pear', 'orange', 'banana',
        }

    def test_repr(self):
        basket = RedisSet({'apple'})
        assert repr(basket) == "RedisSet{'apple'}"

        basket = RedisSet({'apple', 'orange'})
        assert repr(basket) in {
            "RedisSet{'apple', 'orange'}",
            "RedisSet{'orange', 'apple'}",
        }

    def test_pop(self):
        fruits = {'apple', 'orange', 'apple', 'pear', 'orange', 'banana'}
        basket = RedisSet(fruits)
        for _ in range(len(fruits)):
            fruit = basket.pop()
            assert fruit in fruits
            fruits.discard(fruit)

        assert not fruits
        assert not basket
        with self.assertRaises(KeyError):
            basket.pop()

    def test_remove(self):
        basket = RedisSet({'apple', 'orange'})
        basket.remove('orange')
        assert basket == {'apple'}

        basket.remove('apple')
        assert basket == set()

        with self.assertRaises(KeyError):
            basket.remove('apple')

    def test_set_operations(self):
        a = RedisSet('abracadabra')
        b = RedisSet('alacazam')
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
        a = RedisSet('abc')
        b = RedisSet('cde')
        assert not a.isdisjoint(b)
        c = RedisSet('def')
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
        a = RedisSet('abc')
        b = RedisSet('abc')
        # assert a.issubset(b)
        # assert b.issubset(a)
        assert a <= b
        assert b <= a
        assert not a < b
        assert not b < a
        c = RedisSet('abcd')
        # assert a.issubset(c)
        # assert not c.issubset(a)
        assert a <= c
        assert not c <= a
        assert a < c
        assert not c < a
        d = RedisSet('def')
        # assert not a.issubset(d)
        # assert not d.issubset(a)
        assert not a <= d
        assert not d <= a
        assert not a < d
        assert not d < a

    def test_issuperset(self):
        a = RedisSet('abc')
        b = RedisSet('abc')
        # assert a.issuperset(b)
        # assert b.issuperset(a)
        assert a >= b
        assert b >= a
        assert not a > b
        assert not b > a
        c = RedisSet('abcd')
        # assert not a.issuperset(c)
        # assert c.issuperset(a)
        assert not a >= c
        assert c >= a
        assert not a > c
        assert c > a
        d = RedisSet('def')
        # assert not a.issuperset(d)
        # assert not d.issuperset(a)
        assert not a >= d
        assert not d >= a
        assert not a > d
        assert not d > a

    def test_update_with_no_arguments(self):
        s = RedisSet()
        s.update()
        assert s == set()

        s = RedisSet('abcd')
        s.update()
        assert s == {'a', 'b', 'c', 'd'}

    def test_update_with_empty_set(self):
        metasyntactic_variables = RedisSet()
        metasyntactic_variables.update(set())
        assert metasyntactic_variables == set()

        metasyntactic_variables = RedisSet({'foo', 'bar', 'baz'})
        metasyntactic_variables.update(set())
        assert metasyntactic_variables == {'foo', 'bar', 'baz'}

    def test_update_with_set(self):
        metasyntactic_variables = RedisSet()
        metasyntactic_variables.update({'foo', 'bar', 'baz'})
        assert metasyntactic_variables == {'foo', 'bar', 'baz'}
        metasyntactic_variables.update({'qux'})
        assert metasyntactic_variables == {'foo', 'bar', 'baz', 'qux'}

    def test_update_with_range(self):
        ramanujans_friends = RedisSet()
        ramanujans_friends.update(range(10))
        assert ramanujans_friends == {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

    def test_update_with_set_and_range(self):
        silliness = RedisSet()
        silliness.update({'foo', 'bar', 'baz'}, range(5))
        assert silliness == {'foo', 'bar', 'baz', 0, 1, 2, 3, 4}
        silliness.update({'qux'}, range(6))
        assert silliness == {'foo', 'bar', 'baz', 'qux', 0, 1, 2, 3, 4, 5}

    def test_difference_update_with_empty_set(self):
        ramanujans_friends = RedisSet()
        ramanujans_friends.difference_update(set())
        assert ramanujans_friends == set()

        ramanujans_friends = RedisSet(range(10))
        ramanujans_friends.difference_update(set())
        assert ramanujans_friends == {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

    def test_difference_update_with_set(self):
        ramanujans_friends = RedisSet()
        ramanujans_friends.difference_update({5, 6, 7, 8, 9})
        assert ramanujans_friends == set()

        ramanujans_friends = RedisSet(range(10))
        ramanujans_friends.difference_update({5, 6, 7, 8, 9})
        assert ramanujans_friends == {0, 1, 2, 3, 4}

    def test_difference_update_with_range(self):
        ramanujans_friends = RedisSet()
        ramanujans_friends.difference_update(range(5))
        assert ramanujans_friends == set()

        ramanujans_friends = RedisSet(range(10))
        ramanujans_friends.difference_update(range(5))
        assert ramanujans_friends == {5, 6, 7, 8, 9}

    def test_difference_update_with_range_and_set(self):
        ramanujans_friends = RedisSet()
        ramanujans_friends.difference_update(range(4), {6, 7, 8, 9})
        assert ramanujans_friends == set()

        ramanujans_friends = RedisSet(range(10))
        ramanujans_friends.difference_update(range(4), {6, 7, 8, 9})
        assert ramanujans_friends == {4, 5}

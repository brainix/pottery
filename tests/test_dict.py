#-----------------------------------------------------------------------------#
#   test_dict.py                                                              #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



from pottery import RedisDict
from tests.base import TestCase



class DictTests(TestCase):
    '''These tests come from these examples:
        https://docs.python.org/3/tutorial/datastructures.html#dictionaries
    '''

    def test_basic_usage(self):
        tel = RedisDict(jack=4098, sape=4139)
        tel['guido'] = 4127
        assert tel == {'sape': 4139, 'guido': 4127, 'jack': 4098}
        assert tel['jack'] == 4098
        del tel['sape']
        tel['irv'] = 4127
        assert tel == {'guido': 4127, 'irv': 4127, 'jack': 4098}
        assert sorted(tel.keys()) == ['guido', 'irv', 'jack']
        assert 'guido' in tel
        assert not 'jack' not in tel

    def test_constructor_with_key_value_pairs(self):
        d = RedisDict([('sape', 4139), ('guido', 4127), ('jack', 4098)])
        assert d == {'sape': 4139, 'jack': 4098, 'guido': 4127}

    def test_constructor_with_kwargs(self):
        d = RedisDict(sape=4139, guido=4127, jack=4098)
        assert d == {'sape': 4139, 'jack': 4098, 'guido': 4127}

    # The following tests come from these examples:
    #   https://docs.python.org/3.4/library/stdtypes.html#mapping-types-dict

    def test_more_construction_options(self):
        a = RedisDict(one=1, two=2, three=3)
        b = {'one': 1, 'two': 2, 'three': 3}
        c = RedisDict(zip(['one', 'two', 'three'], [1, 2, 3]))
        d = RedisDict([('two', 2), ('one', 1), ('three', 3)])
        e = RedisDict({'three': 3, 'one': 1, 'two': 2})
        assert a == b == c == d == e

    def test_len(self):
        a = RedisDict()
        assert len(a) == 0
        a = RedisDict(one=1, two=2, three=3)
        assert len(a) == 3
        a['four'] = 4
        assert len(a) == 4
        del a['four']
        assert len(a) == 3

    def test_keyerror(self):
        a = RedisDict(one=1, two=2, three=3)
        assert a['one'] == 1
        assert a['two'] == 2
        assert a['three'] == 3
        with self.assertRaises(KeyError):
            a['four']

    def test_key_assignment(self):
        a = RedisDict(one=1, two=2, three=2)
        assert a['three'] == 2
        a['three'] = 3
        assert a['three'] == 3
        a['four'] = 4
        assert a['four'] == 4

    def test_key_deletion(self):
        a = RedisDict(one=1, two=2, three=3)
        assert sorted(a.keys()) == ['one', 'three', 'two']
        a['four'] = 4
        assert sorted(a.keys()) == ['four', 'one', 'three', 'two']
        with self.assertRaises(KeyError):
            del a['five']
        del a['four']
        assert sorted(a.keys()) == ['one', 'three', 'two']
        del a['three']
        assert sorted(a.keys()) == ['one', 'two']
        del a['two']
        assert sorted(a.keys()) == ['one']
        del a['one']
        assert sorted(a.keys()) == []
        with self.assertRaises(KeyError):
            del a['one']

    def test_key_membership(self):
        a = RedisDict(one=1, two=2, three=3)
        assert 'one' in a
        assert 'four' not in a
        assert not 'four' in a
        a['four'] = 4
        assert 'four' in a
        del a['four']
        assert 'four' not in a
        assert not 'four' in a

    def test_clear(self):
        a = RedisDict(one=1, two=2, three=3)
        assert sorted(a.keys()) == ['one', 'three', 'two']
        a.clear()
        assert sorted(a.keys()) == []
        a.clear()
        assert sorted(a.keys()) == []

    def test_get(self):
        a = RedisDict(one=1, two=2, three=3)
        assert a.get('one') == 1
        assert a.get('one', 42) == 1
        assert a.get('two') == 2
        assert a.get('two', 42) == 2
        assert a.get('three') == 3
        assert a.get('three', 42) == 3
        assert a.get('four') is None
        assert a.get('four', 42) == 42
        a['four'] = 4
        assert a.get('four') == 4
        assert a.get('four', 42) == 4
        del a['four']
        assert a.get('four') is None
        assert a.get('four', 42) == 42

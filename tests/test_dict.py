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

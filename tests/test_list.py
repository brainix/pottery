#-----------------------------------------------------------------------------#
#   test_list.py                                                              #
#                                                                             #
#   Copyright (c) 2015, Rajiv Bakulesh Shah.                                  #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



from pottery import RedisList
from tests.base import TestCase



class ListTests(TestCase):
    '''These tests come from these examples:
        https://docs.python.org/3/tutorial/introduction.html#lists
        https://docs.python.org/3/tutorial/datastructures.html#more-on-lists
    '''

    def test_basic_usage(self):
        squares = RedisList((1, 4, 9, 16, 25))
        assert tuple(squares) == (1, 4, 9, 16, 25)
        assert squares[0] == 1
        assert squares[-1] == 25
        assert tuple(squares[-3:]) == (9, 16, 25)
        assert tuple(squares[:]) == (1, 4, 9, 16, 25)
        assert tuple(squares + [36, 49, 64, 81, 100]) == (1, 4, 9, 16, 25, 36, 49, 64, 81, 100)
        cubes = RedisList((1, 8, 27, 65, 125))
        cubes[3] = 64
        assert tuple(cubes) == (1, 8, 27, 64, 125)
        cubes.append(216)
        cubes.append(7 ** 3)
        assert tuple(cubes) == (1, 8, 27, 64, 125, 216, 343)
        letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g']
        assert tuple(letters) == ('a', 'b', 'c', 'd', 'e', 'f', 'g')
        letters[2:5] = ['C', 'D', 'E']
        assert tuple(letters) == ('a', 'b', 'C', 'D', 'E', 'f', 'g')
        letters[2:5] = []
        assert tuple(letters) == ('a', 'b', 'f', 'g')
        letters[:] = []
        assert tuple(letters) == tuple()
        letters = RedisList(('a', 'b', 'c', 'd'))
        assert len(letters) == 4
        a = ['a', 'b', 'c']
        n = [1, 2, 3]
        x = RedisList((a, n))
        assert tuple(x) == (['a', 'b', 'c'], [1, 2, 3])
        assert tuple(x[0]) == ('a', 'b', 'c')
        assert x[0][1] == 'b'

    def test_more_on_lists(self):
        a = RedisList((66.25, 333, 333, 1, 1234.5))
        assert (a.count(333), a.count(66.25), a.count('x')) == (2, 1, 0)
        a.insert(2, -1)
        a.append(333)
        assert tuple(a) == (66.25, 333, -1, 333, 1, 1234.5, 333)
        assert a.index(333) == 1
        a.remove(333)
        assert tuple(a) == (66.25, -1, 333, 1, 1234.5, 333)
        a.reverse()
        assert tuple(a) == (333, 1234.5, 1, 333, -1, 66.25)
        a.sort()
        assert tuple(a) == (-1, 1, 66.25, 333, 333, 1234.5)
        assert a.pop() == 1234.5
        assert tuple(a) == (-1, 1, 66.25, 333, 333)

    def test_using_list_as_stack(self):
        stack = RedisList((3, 4, 5))
        stack.append(6)
        stack.append(7)
        assert tuple(stack) == (3, 4, 5, 6, 7)
        assert stack.pop() == 7
        assert tuple(stack) == (3, 4, 5, 6)
        assert stack.pop() == 6
        assert stack.pop() == 5
        assert tuple(stack) == (3, 4)

    def test_del(self):
        a = RedisList((-1, 1, 66.25, 333, 333, 1234.5))
        del a[0]
        assert tuple(a) == (1, 66.25, 333, 333, 1234.5)
        del a[2:4]
        assert tuple(a) == (1, 66.25, 1234.5)
        del a[:]
        assert tuple(a) == tuple()

#-----------------------------------------------------------------------------#
#   test_list.py                                                              #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



from pottery import KeyExistsError
from pottery import RedisList
from tests.base import TestCase



class ListTests(TestCase):
    '''These tests come from these examples:
        https://docs.python.org/3/tutorial/introduction.html#lists
        https://docs.python.org/3/tutorial/datastructures.html#more-on-lists
    '''

    def test_indexerror(self):
        list_ = RedisList()
        with self.assertRaises(IndexError):
            list_[0] = 'raj'

    def test_keyexistserror(self):
        squares = RedisList((1, 4, 9, 16, 25), key='pottery:squares')
        with self.assertRaises(KeyExistsError):
            squares = RedisList((1, 4, 9, 16, 25), key='pottery:squares')

    def test_basic_usage(self):
        squares = RedisList((1, 4, 9, 16, 25))
        assert squares == [1, 4, 9, 16, 25]
        assert squares[0] == 1
        assert squares[-1] == 25
        assert squares[-3:] == [9, 16, 25]
        assert squares[:] == [1, 4, 9, 16, 25]
        assert squares + [36, 49, 64, 81, 100] == [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]

    def test_mutability_and_append(self):
        cubes = RedisList((1, 8, 27, 65, 125))
        cubes[3] = 64
        assert cubes == [1, 8, 27, 64, 125]
        cubes.append(216)
        cubes.append(7**3)
        assert cubes == [1, 8, 27, 64, 125, 216, 343]

    def test_slices(self):
        letters = RedisList(('a', 'b', 'c', 'd', 'e', 'f', 'g'))
        assert letters == ['a', 'b', 'c', 'd', 'e', 'f', 'g']
        assert letters[2:5] == ['c', 'd', 'e']
        assert letters[2:5:2] == ['c', 'e']
        assert letters[2:5:3] == ['c']
        assert letters[2:5:4] == ['c']
        letters[2:5] = ['C', 'D', 'E']
        assert letters == ['a', 'b', 'C', 'D', 'E', 'f', 'g']
        letters[2:5:2] = [None, None]
        assert letters == ['a', 'b', None, 'D', None, 'f', 'g']
        letters[2:5] = []
        assert letters == ['a', 'b', 'f', 'g']
        letters[:] = []
        assert letters == []

    def test_len(self):
        letters = RedisList(('a', 'b', 'c', 'd'))
        assert len(letters) == 4

    def test_nesting(self):
        a = ['a', 'b', 'c']
        n = [1, 2, 3]
        x = RedisList((a, n))
        assert x == [['a', 'b', 'c'], [1, 2, 3]]
        assert x[0] == ['a', 'b', 'c']
        assert x[0][1] == 'b'

    def test_more_on_lists(self):
        a = RedisList((66.25, 333, 333, 1, 1234.5))
        assert (a.count(333), a.count(66.25), a.count('x')) == (2, 1, 0)
        a.insert(2, -1)
        a.append(333)
        assert a == [66.25, 333, -1, 333, 1, 1234.5, 333]
        assert a.index(333) == 1
        a.remove(333)
        assert a == [66.25, -1, 333, 1, 1234.5, 333]
        a.reverse()
        assert a == [333, 1234.5, 1, 333, -1, 66.25]
        a.sort()
        assert a == [-1, 1, 66.25, 333, 333, 1234.5]
        assert a.pop() == 1234.5
        assert a == [-1, 1, 66.25, 333, 333]

    def test_using_list_as_stack(self):
        stack = RedisList((3, 4, 5))
        stack.append(6)
        stack.append(7)
        assert stack == [3, 4, 5, 6, 7]
        assert stack.pop() == 7
        assert stack == [3, 4, 5, 6]
        assert stack.pop() == 6
        assert stack.pop() == 5
        assert stack == [3, 4]

    def test_del(self):
        a = RedisList((-1, 1, 66.25, 333, 333, 1234.5))
        del a[0]
        assert a == [1, 66.25, 333, 333, 1234.5]
        del a[2:4]
        assert a == [1, 66.25, 1234.5]
        del a[:]
        assert a == []

    def test_insert_left(self):
        squares = RedisList((9, 16, 25))
        squares.insert(-1, 4)
        assert squares == [4, 9, 16, 25]
        squares.insert(0, 1)
        assert squares == [1, 4, 9, 16, 25]

    def test_sort(self):
        squares = RedisList({1, 4, 9, 16, 25})
        squares.sort()
        assert squares == [1, 4, 9, 16, 25]

        squares.sort(reverse=True)
        assert squares == [25, 16, 9, 4, 1]

        with self.assertRaises(NotImplementedError):
            squares.sort(key=str)

    def test_eq_same_redis_instance_and_key(self):
        squares1 = RedisList((1, 4, 9, 16, 25), key='pottery:squares')
        squares2 = RedisList(key='pottery:squares')
        assert squares1 == squares2
        assert not squares1 != squares2

    def test_eq_different_lengths(self):
        squares1 = RedisList((1, 4, 9, 16, 25), key='pottery:squares')
        squares2 = (1, 4, 9, 16, 25, 36)
        assert not squares1 == squares2
        assert squares1 != squares2

    def test_eq_different_types_different_items(self):
        squares1 = RedisList((1, 4, 9, 16, 25), key='pottery:squares')
        squares2 = (4, 9, 16, 25, 36)
        assert not squares1 == squares2
        assert squares1 != squares2

    def test_eq_typeerror(self):
        squares = RedisList((1, 4, 9, 16, 25), key='pottery:squares')
        assert not squares == None
        assert squares != None

    def test_repr(self):
        squares = RedisList((1, 4, 9, 16, 25))
        assert repr(squares) == 'RedisList[1, 4, 9, 16, 25]'

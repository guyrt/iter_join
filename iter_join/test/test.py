from unittest2 import TestCase
from ..iter_join import left_join, right_join, inner_join


class TestInnerJoin(TestCase):
    def test_inner_sample(self):
        # All lefts have a right. Test this is so.
        left = ((i, ) for i in xrange(5))
        right = ((i, ) for i in xrange(6))
        gen = inner_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual(list_gen, [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)])
        
    def test_proper_left_join(self):
        # Not all lefts have a right. Only return pairs
        left = ((i, ) for i in xrange(5))
        right = ((2 * i, ) for i in xrange(4))
        gen = inner_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual(list_gen, [(0, 0), (2, 2), (4, 4)])
        
    def test_duplicated_left_value(self):
        # Some lefts repeat.
        left = ((i / 2, ) for i in xrange(5))
        right = ((i, ) for i in xrange(10))
        gen = inner_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual(list_gen, [(0, 0), (1, 1), (2, 2)])
        
    def test_interlaced(self):
        # return nothing
        left = ((2 * i, ) for i in xrange(10))
        right = ((2 * i + 1, ) for i in xrange(10))
        gen = inner_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual([], list_gen)
        
    def test_repeat_left_alone(self):
        left = ((i, ) for i in xrange(3))
        right = ((0, ) for i in xrange(3))
        gen = inner_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual([(0, 0), (0, 0), (0, 0)], list_gen)
        
    def test_duplicated_right_value(self):
        right = ((i, ) for i in xrange(3))
        left = ((0, ) for i in xrange(3))
        gen = inner_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual([(0, 0)], list_gen)
        
    def test_empty_left(self):
        left = ((i, ) for i in xrange(0))
        right = ((0, ) for i in xrange(3))
        gen = inner_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual([], list_gen)
        
    def test_empty_right(self):
        right = ((i, ) for i in xrange(0))
        left = ((0, ) for i in xrange(3))
        gen = inner_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual([], list_gen)


class TestLeftJoin(TestCase):
    
    def test_inner_sample(self):
        # All lefts have a right. Test this is so.
        left = ((i, ) for i in xrange(5))
        right = ((i, ) for i in xrange(6))
        gen = left_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual(list_gen, [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)])
        
    def test_proper_left_join(self):
        # Not all lefts have a right. Test this is so.
        left = ((i, ) for i in xrange(5))
        right = ((2 * i, ) for i in xrange(4))
        gen = left_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual(list_gen, [(0, 0), (1, None), (2, 2), (3, None), (4, 4)])
        
    def test_duplicated_left_value(self):
        # Some lefts repeat.
        left = ((i / 2, ) for i in xrange(5))
        right = ((i, ) for i in xrange(10))
        gen = left_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual(list_gen, [(0, 0), (0, None), (1, 1), (1, None), (2, 2)])
        
    def test_interlaced(self):
        left = ((2 * i, ) for i in xrange(10))
        right = ((2 * i + 1, ) for i in xrange(10))
        gen = left_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual(zip(range(0, 20, 2), (None for i in range(0, 20, 2))), list_gen)
        
    def test_repeat_left_alone(self):
        left = ((i, ) for i in xrange(3))
        right = ((0, ) for i in xrange(3))
        gen = left_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual([(0, 0), (0, 0), (0, 0), (1, None), (2, None)], list_gen)
        
    def test_duplicated_right_value(self):
        right = ((i, ) for i in xrange(3))
        left = ((0, ) for i in xrange(3))
        gen = left_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual([(0, 0), (0, None), (0, None)], list_gen)
        
    def test_empty_left(self):
        left = ((i, ) for i in xrange(0))
        right = ((0, ) for i in xrange(3))
        gen = left_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual([], list_gen)
        
    def test_empty_right(self):
        right = ((i, ) for i in xrange(0))
        left = ((0, ) for i in xrange(3))
        gen = left_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual([(0, None), (0, None), (0, None)], list_gen)


class TestRightJoin(TestCase):
    
    def test_inner_sample(self):
        # All lefts have a right. Test this is so.
        left = ((i, ) for i in xrange(5))
        right = ((i, ) for i in xrange(6))
        gen = right_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual(list_gen, [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (None, 5)])
        
    def test_proper_left_join(self):
        # Not all lefts have a right. Test this is so.
        left = ((i, ) for i in xrange(5))
        right = ((2 * i, ) for i in xrange(4))
        gen = right_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual(list_gen, [(0, 0), (2, 2), (4, 4), (None, 6)])
        
    def test_duplicated_left_value(self):
        # Some lefts repeat.
        left = ((i / 2, ) for i in xrange(5))
        right = ((i, ) for i in xrange(10))
        gen = right_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual(list_gen,  [(0, 0),
            (0, 0),
            (1, 1),
            (1, 1),
            (2, 2),
            (None, 3),
            (None, 4),
            (None, 5),
            (None, 6),
            (None, 7),
            (None, 8),
            (None, 9)])
        
    def test_interlaced(self):
        left = ((2 * i, ) for i in xrange(10))
        right = ((2 * i + 1, ) for i in xrange(10))
        gen = right_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual(zip((None for i in range(0, 20, 2)), range(1, 20, 2), ), list_gen)
        
    def test_repeat_left_alone(self):
        left = ((i, ) for i in xrange(3))
        right = ((0, ) for i in xrange(3))
        gen = right_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual([(0, 0), (None, 0), (None, 0)], list_gen)
        
    def test_duplicated_right_value(self):
        right = ((i, ) for i in xrange(3))
        left = ((0, ) for i in xrange(3))
        gen = right_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual([(0, 0), (0, 0), (0, 0), (None, 1), (None, 2)], list_gen)
        
    def test_empty_left(self):
        left = ((i, ) for i in xrange(0))
        right = ((0, ) for i in xrange(3))
        gen = right_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual([(None, 0), (None, 0), (None, 0)], list_gen)
        
    def test_empty_right(self):
        right = ((i, ) for i in xrange(0))
        left = ((0, ) for i in xrange(3))
        gen = right_join(left, right, lambda x: x[0], lambda x: x[0])
        list_gen = list(gen)
        self.assertEqual([], list_gen)

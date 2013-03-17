

def left_join(left_iter, right_iter, left_access, right_access, right_empty=None):
    """
    Given two iterators, return their LEFT JOIN combination using typical SQL rules.
    
    Params:
        left_iter: Iterator for left side of join
        right_iter: Iterator for right side of join
        left_access: func(left_value) returns key value for values in left.
        right_access: func(right_value) returns key value for values in right.
        right_empty: values to append for missing right sides. Defaults to a tuple of "Nones"
    """
    left_yielded = False
    try:
        left_value = left_iter.next()
        left_key = left_access(left_value)
        # Main loop: yield either zipped left pairs or zipped right pairs.
        try:
            right_value = right_iter.next()
            right_key = right_access(right_value)
            if not right_empty:
                right_empty = (None, ) * len(right_value)
            while 1:
                while left_key > right_key:
                    right_value = right_iter.next()
                    right_key = right_access(right_value)
                while left_key < right_key:
                    if not left_yielded:
                        yield left_value + right_empty
                        left_yielded = True
                    left_value = left_iter.next()
                    left_key = left_access(left_value)
                    left_yielded = False
                
                if left_key == right_key:
                    yield left_value + right_value
                    left_yielded = True
                    right_value = right_iter.next()
                    right_key = right_access(right_value)
                
        except StopIteration:
            # right iterable was empty.
            if not right_empty:
                right_empty = (None, )
        if not left_yielded:
            yield left_value + right_empty
        while 1:
            yield left_iter.next() + right_empty
        
    except StopIteration:
        # left iteration ran out.
        pass
        

def right_join(left_iter, right_iter, left_access, right_access, left_empty=None):
    left_join(right_iter, left_iter, right_access, left_access, left_empty)

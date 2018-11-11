from itertools import groupby
from operator import itemgetter


class Operations(object):
    """
    doc for operations
    """

    def __init__(self, input, **kwargs):
        self.input = input
        self.kwargs = kwargs
        self._maximum_uses_as_input = 0
        self._current_uses_as_input = 0
        if isinstance(input, Operations):
            input._maximum_uses_as_input += 1
        if type(self) == Joiner and isinstance(self.second_input, Operations):
            self.second_input._maximum_uses_as_input += 1

        self.result = []

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        for k in self.__dict__:
            if k != '_input_counter' and getattr(self, k) != getattr(other, k):
                return False
        return True

    def run(self):
        if self.result == []:
            if type(self) in (Mapper, Reducer, Joiner) and self._maximum_uses_as_input <= 1:
                self.result = self._process_run(self.input.result, **self.kwargs)
            elif type(self) in (Mapper, Reducer, Joiner) and self._maximum_uses_as_input > 1:
                self.result = list(self._process_run(self.input.result, **self.kwargs))
            else:
                self._process_run(self.input.result, **self.kwargs)

    def _clear_memory(self):
        self.input._current_uses_as_input += 1
        if type(self) == Joiner:
            self.second_input._current_uses_as_input += 1

        if type(self.input.result) == list and self.input._maximum_uses_as_input == self.input._current_uses_as_input:
            # print('Clearing: {}'.format(self.input))
            self.input.result = []
        if type(self) == Joiner and type(self.second_input.result) == list:
            if self.second_input._maximum_uses_as_input == self.second_input._current_uses_as_input:
                # print('Clearing: {}'.format(self.second_input))
                self.second_input.result = []


class Input(Operations):
    """nice doc"""

    def __init__(self):
        super().__init__(input=None)

    def run(self):
        self.result = []
        for row in self.input:
            self.result.append(row)


class Mapper(Operations):
    """base class for mapping"""

    def __init__(self, mapper, input, **kwargs):
        self.mapper = mapper
        super().__init__(input, **kwargs)

    def _process_run(self, input, **kwargs):
        for row in input:
            for x in self.mapper(row, **kwargs):
                yield x
        super()._clear_memory()


class Sorter(Operations):
    """
    TODO doc
    """

    def __init__(self, key, input):
        self.key = key
        super().__init__(input)

    # def _slice_by_key(self):
    #     if type(self.input) == tuple:
    #         return lambda x: tuple(x[k] for k in self.key)
    #     return lambda x: x[self.key]

    def _process_run(self, input):
        if type(self.key) == str:
            self.key = [self.key]
        if type(input) != list:
            self.result = sorted(list(input), key=itemgetter(*self.key))
        else:
            self.result = sorted(input, key=itemgetter(*self.key))
        super()._clear_memory()


class Folder(Operations):
    """docstring for Folder"""

    def __init__(self, folder, state, input):
        self.folder = folder
        self.state = state
        super().__init__(input)

    def _process_run(self, input):
        for row in input:
            self.state = self.folder(self.state, row)
        self.result = [self.state]
        super()._clear_memory()


class Reducer(Operations):
    """
    docstring for Reducer
    use groupby from itertools
    """

    def __init__(self, reducer, key, input, **kwargs):
        self.reducer = reducer
        self.key = key
        super().__init__(input, **kwargs)

    def _slice_by_key(self):
        if type(self.key) == tuple:
            return lambda x: tuple(x[k] for k in self.key)
        return lambda x: x[self.key]

    def _process_run(self, input, **kwargs):
        if type(self.key) == str:
            self.key = [self.key]
        for group in groupby(input, itemgetter(*self.key)):
            yield from self.reducer(group[1], **kwargs)
        super()._clear_memory()


class Joiner(Operations):
    """
    TODO
    """

    def __init__(self, key, method, input, second_input):
        self.second_input = second_input
        self.key = key
        self.method = method
        super().__init__(input)

    def _reverse(self):
        self.input, self.second_input = self.second_input, self.input
        self.key = (self.key[1], self.key[0])

    def _process_left_join(self, reversed=False):
        if reversed:
            self._reverse()
        for row_1 in self.input.result:
            flag = True
            for row_2 in self.second_input.result:
                # print(row_1[self.key[0]], row_2[self.key[1]])
                if row_1[self.key[0]] == row_2[self.key[1]]:
                    flag = False
                    if reversed:
                        new_row = dict(row_2)
                        new_row.update(row_1)
                    else:
                        new_row = dict(row_1)
                        new_row.update(row_2)
                    yield new_row
            if flag:
                if reversed:
                    new_row = {k: None for k in row_2}
                    new_row.update(row_1)
                else:
                    new_row = dict(row_1)
                    new_row.update({k: None for k in row_2})
                yield new_row
        if reversed:
            self._reverse()

    def _process_right_join(self):
        yield from self._process_left_join(reversed=True)

    def _process_only_outer_join(self, reversed=False):
        if reversed:
            self._reverse()
        for row_1 in self.input.result:
            for row_2 in self.second_input.result:
                if row_1[self.key[0]] == row_2[self.key[1]]:
                    break
            else:
                if reversed:
                    new_row = {k: None for k in row_2}
                    new_row.update(row_1)
                else:
                    new_row = dict(row_1)
                    new_row.update({k: None for k in row_2})
                yield new_row
        if reversed:
            self._reverse()

    def _process_outer_join(self):
        yield from self._process_inner_join()
        yield from self._process_only_outer_join()
        yield from self._process_only_outer_join(reversed=True)

    def _process_inner_join(self):
        for row_1 in self.input.result:
            for row_2 in self.second_input.result:
                if row_1[self.key[0]] == row_2[self.key[1]]:
                    new_row = dict(row_1)
                    new_row.update(row_2)
                    yield new_row

    def _process_cross_join(self):
        for row_1 in self.input.result:
            for row_2 in self.second_input.result:
                new_row = dict(row_1)
                new_row.update(row_2)
                yield new_row

    def _process_run(self, input):
        self.second_input.result = list(self.second_input.result)
        if self.method == 'inner':
            yield from self._process_inner_join()
        elif self.method == 'left':
            yield from self._process_left_join()
        elif self.method == 'right':
            yield from self._process_right_join()
        elif self.method == 'outer':
            yield from self._process_outer_join()
        elif self.method == 'cross':
            yield from self._process_cross_join()
        super()._clear_memory()

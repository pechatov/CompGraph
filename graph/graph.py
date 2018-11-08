from collections import Counter
from copy import deepcopy
from graph.operations import Mapper, Reducer, Folder, Sorter, Joiner, Input


class Graph(object):
    """docstring for Graph"""

    def __init__(self):
        self._queue = []
        self._queue.append(Input())

    def input(self, input: 'Graph') -> None:
        if isinstance(input, Graph):
            self._queue = deepcopy(input._queue)
        else:
            self._queue[0].input = input
        return self

    def _delete_same_nodes(self):
        indeces_of_unique_nodes = set(range(len(self._queue)))
        for i in range(len(self._queue)):
            for j in range(i + 1, len(self._queue)):
                if self._queue[i] == self._queue[j]:
                    indeces_of_unique_nodes.discard(j)
                    for k in range(j + 1, len(self._queue)):
                        if self._queue[k].input == self._queue[j]:
                            self._queue[k].input = self._queue[i]
                            self._queue[i]._input_counter += 1
                            print('deleted')
        self._queue = [self._queue[i] for i in sorted(indeces_of_unique_nodes)]

        # for i in range(1, len(self._queue)):
        #     for j in range(i + 1, len(self._queue)):
        #         if self._queue[i] == self._queue[j]:
        #             for k in range(j + 1, len(self._queue)):
        #                 if self._queue[k].input is self._queue[j]:
        #                     self._queue[k].input = self._queue[i]
        #                     self._queue[i]._input_counter += 1
        #             self._queue.pop(j)
        #             j -= 1

    def _insert_inputs(self, **kwargs):
        for node in self._queue:
            if isinstance(node, Input):
                for k in kwargs:
                    if node.input == k:
                        node.input = kwargs[k]

    def run(self, **kwargs):
        self._insert_inputs(kwargs)

        self._delete_same_nodes()
        # return self._queue[-1].result

    def map(self, mapper: Mapper):
        new_node = Mapper(mapper, self._queue[-1])
        self._queue.append(new_node)
        return self

    def sort(self, key):
        new_node = Sorter(key, self._queue[-1])
        self._queue.append(new_node)
        return self

    def reduce(self, reducer: Reducer, key):
        new_node = Reducer(reducer, key, self._queue[-1])
        self._queue.append(new_node)
        return self

    def fold(self, folder: Folder, state=None):
        new_node = Folder(folder, state, self._queue[-1])
        self._queue.append(new_node)
        return self

    def join(self, other_graph: 'Graph', key: str, method: str):
        print('*' * 100)
        first_input = self._queue[-1]
        for node in other_graph._queue:
            print('node = {}, node._input_counter = {}'.format(node, node._input_counter))
            self._queue.append(node)
        print('*' * 100)
        new_node = Joiner(key, method, first_input, self._queue[-1])
        self._queue.append(new_node)
        return self


"""
def _filter_punctuation(txt):
p = set(string.punctuation)
return "".join([c for c in txt if c not in p])
"""

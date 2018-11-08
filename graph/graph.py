from collections import Counter
from graph.operations import Mapper, Reducer, Folder, Sorter, Joiner, Input


class Graph(object):
    """docstring for Graph"""

    def __init__(self):
        self._queue = []
        self._queue.append(Input())

    def input(self, input: 'Graph') -> None:

        self._queue = input._queue
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

    def run(self, input_table):
        self._queue[0].input = input_table
        # self._delete_same_nodes()
        for node in self._queue:
            node.run()
            # print(node.__dict__)
            # print('*' * 50)

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
        first_input = self._queue[-1]
        for node in other_graph._queue:
            print(node)
            self._queue.append(node)
        new_node = Joiner(key, method, first_input, self._queue[-1])
        self._queue.append(new_node)
        return self


"""
def _filter_punctuation(txt):
p = set(string.punctuation)
return "".join([c for c in txt if c not in p])
"""

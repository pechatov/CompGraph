from copy import deepcopy
from graph.operations import Mapper, Reducer, Folder, Sorter, Joiner, Input
import json


class Graph(object):
    """docstring for Graph"""

    def __init__(self):
        self._queue = []
        self._queue.append(Input())

    def input(self, input: 'Graph') -> None:
        if isinstance(input, Graph):
            self._queue = deepcopy(input._queue)
        else:
            self._queue[0].input_id = input
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
                            self._queue[i]._maximum_uses_as_input += 1
        self._queue = [self._queue[i] for i in sorted(indeces_of_unique_nodes)]

    def _insert_inputs(self, **kwargs):
        for node in self._queue:
            if isinstance(node, Input):
                for k in kwargs:
                    if node.input_id == k:
                        if self._parser == None:
                            node.input = kwargs[k]
                        else:
                            node.input = self._parser(kwargs[k])

    def print_result(self):
        for row in self.result:
            print(row)

    def _clear_input_counter(self):
        for node in self._queue:
            node._current_uses_as_input = 0
            node.result = []

    def run(self, parser=None, output=None, **kwargs):
        self.result = []
        self._parser = parser
        self._clear_input_counter()
        self._delete_same_nodes()
        self._insert_inputs(**kwargs)
        for node in self._queue:
            print('Running: {}'.format(node))
            node.run()
            # print('*' * 100)
            # self.print_nodes()

        if output is None:
            self.result = self._queue[-1].result
            return list(self.result)
        else:
            with open(output, 'w') as file:
                for row in self._queue[-1].result:
                    file.write(json.dumps(row))

    def print_nodes(self, **kwargs):
        # self.result = []
        # self._clear_input_counter()
        # self._delete_same_nodes()
        # self._insert_inputs(**kwargs)
        for node in self._queue:
            print('node: {}, max: {}, current: {}'.format(
                node, node._maximum_uses_as_input, node._current_uses_as_input), end=' ')
            if not type(node.result) == list:
                print('node.result: generator')
            else:
                print('node.result: {}'.format(len(node.result)))

    def map(self, mapper: Mapper, **kwargs):
        new_node = Mapper(mapper, self._queue[-1], **kwargs)
        self._queue.append(new_node)
        return self

    def sort(self, key):
        new_node = Sorter(key, self._queue[-1])
        self._queue.append(new_node)
        return self

    def reduce(self, reducer: Reducer, key, **kwargs):
        new_node = Reducer(reducer, key, self._queue[-1], **kwargs)
        self._queue.append(new_node)
        return self

    def fold(self, folder: Folder, state=None, **kwargs):
        new_node = Folder(folder, state, self._queue[-1])
        self._queue.append(new_node)
        return self

    def join(self, other_graph: 'Graph', method: str, key=None):
        first_input = self._queue[-1]
        for node in other_graph._queue:
            self._queue.append(node)
        print('*' * 100)
        new_node = Joiner(key, method, first_input, self._queue[-1])
        self._queue.append(new_node)
        return self


"""

"""

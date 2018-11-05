from abc import abstractclassmethod


class Mapper(object):
    """base class for mapping"""

    @abstractclassmethod
    def __call__(self, r):
        pass


class Sort(object):

    def __call__(self, records, col_name):
        pass


class Folder(object):
    """docstring for Folder"""
    @abstractclassmethod
    def __call__(self, records, folder):
        pass


class Reducer(object):
    """docstring for Reducer"""
    @abstractclassmethod
    def __call(self, records):
        pass


class Aggregator(object):
    """docstring for Aggregator"""
    @abstractclassmethod
    def __call__(self, )

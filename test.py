import runner

from itertools import cycle, islice
from pytest import approx


def sorted_eq(tb1, tb2, key):
    return sorted(tb1, key=lambda x: tuple(x[k] for k in key)) == \
        sorted(tb2, key=lambda x: tuple(x[k] for k in key))


def test_word_count():
    docs = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
        {'doc_id': 2, 'text': 'Hello, my little little hell'}
    ]

    etalon = [
        {'count': 1, 'text': 'hell'},
        {'count': 1, 'text': 'world'},
        {'count': 2, 'text': 'hello'},
        {'count': 2, 'text': 'my'},
        {'count': 3, 'text': 'little'}
    ]

    g = runner.build_word_count_graph('docs')

    result = g.run(docs=docs)

    assert result == etalon


def test_word_count_multiple_call():
    g = runner.build_word_count_graph('text')

    rows1 = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
    ]

    etalon1 = [
        {'count': 1, 'text': 'world'},
        {'count': 1, 'text': 'hello'},
        {'count': 1, 'text': 'my'},
        {'count': 1, 'text': 'little'}
    ]

    result1 = g.run(
        text=rows1
    )

    rows2 = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
        {'doc_id': 2, 'text': 'Hello, my little little hell'}
    ]

    etalon2 = [
        {'count': 1, 'text': 'hell'},
        {'count': 1, 'text': 'world'},
        {'count': 2, 'text': 'hello'},
        {'count': 2, 'text': 'my'},
        {'count': 3, 'text': 'little'}
    ]

    result2 = g.run(
        text=rows2,
        verbose=True
    )

    assert sorted_eq(etalon2, result2, ['text'])
